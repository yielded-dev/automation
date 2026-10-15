import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { Miniflare, Log, LogLevel, convertV4MiniflareOptions } from "miniflare";
import { build } from "vite-plus/pack";
import { v5 as uuidV5 } from "uuid";
import { gitArtifacts } from "./fixtures/git-artifacts.mjs";
import { localPostgres } from "./fixtures/postgres.mjs";

const [image, selectedCase] = process.argv.slice(process.argv[2] === "--" ? 3 : 2);
assert.ok(image, "Usage: vp run test:e2e:launch -- <local image> [case]");
assert.equal(process.env.MINIFLARE_WORKERD_PATH, undefined, "Use the pinned workerd binary");
const root = fileURLToPath(new URL("../../", import.meta.url));
const execute = (file, args) =>
  execFileSync(file, args, { cwd: root, encoding: "utf8", timeout: 30_000 }).trim();
const dockerHost =
  process.env.DOCKER_HOST ??
  execute("docker", ["context", "inspect", "--format", "{{.Endpoints.docker.Host}}"]);
assert.ok(dockerHost.startsWith("unix://"), "Only local Docker is supported");
const docker = (...args) => execute("docker", ["--host", dockerHost, ...args]);
const imageId = docker("image", "inspect", image, "--format", "{{.Id}}");
assert.equal(
  docker("image", "inspect", imageId, "--format", "{{.Os}}/{{.Architecture}}"),
  "linux/amd64",
);
const proxyId = docker(
  "image",
  "inspect",
  "cloudflare/proxy-everything:3cb1195@sha256:0ef6716c52430096900b150d84a3302057d6cd2319dae7987128c85d0733e3c8",
  "--format",
  "{{.Id}}",
);
const directory = await mkdtemp(join(tmpdir(), "repairs-launch-"));
const bundle = join(directory, "bundle");
const runName = `repairs-launch-${randomUUID()}`;
const mfPackage = createRequire(import.meta.url)("miniflare/package.json");
const evidence = {
  passed: false,
  scope:
    "Real local Postgres, workerd, SQLite DOs, native alarms, Docker and Git. Synthetic host authority, Artifacts control plane and model harness; no provider calls or hosted speedup claim. Fresh container per accepted run; shared Docker wall times are diagnostic. SQL is from Postgres logs; nativeRpcs records actual native method entries. Spans use the existing Effect tracer delegate for local operation timing only, not Cloudflare collector/export parity.",
  head: execute("git", ["rev-parse", "HEAD"]),
  dirty: execute("git", ["status", "--porcelain"]) !== "",
  selectedCase: selectedCase ?? null,
  sourceHash: createHash("sha256")
    .update(await readFile(join(root, "packages/automation/src/server/RepairExecution.ts")))
    .digest("hex"),
  repositoryHash: createHash("sha256")
    .update(
      await readFile(
        join(root, "packages/automation/src/server/database/RepairExecutionRepository.ts"),
      ),
    )
    .digest("hex"),
  preparationHash: createHash("sha256")
    .update(
      await readFile(join(root, "packages/sandbox/src/cloudflare/WorkspaceBaselinePreparation.ts")),
    )
    .digest("hex"),
  setupHash: createHash("sha256")
    .update(await readFile(join(root, "packages/sandbox/src/cloudflare/RepositorySetup.ts")))
    .digest("hex"),
  checkoutHash: createHash("sha256")
    .update(await readFile(join(root, "packages/sandbox/src/cloudflare/ArtifactsCheckout.ts")))
    .digest("hex"),
  node: process.version,
  miniflare: mfPackage.version,
  workerd: mfPackage.dependencies.workerd,
  imageId,
  proxyId,
  cases: [],
};
console.log(`Native launch evidence: ${directory}`);
const failures = [];
const launches = new Map();
const snapshots = new Set();
const spans = [];
const nativeRpcs = [];
const preparationJoins = [];
const harnessHistory = [];
const historyCleanupFailures = [];
const preparationCaptures = [];
const recipeReads = [];
const capturedCheckpoints = [];
const done = new Map();
let mf;
let pg;
let releaseCheckpoint;
const save = () => writeFile(join(directory, "results.json"), JSON.stringify(evidence, null, 2));
const identity = (...parts) =>
  uuidV5(JSON.stringify(["kommunikasie-repair", ...parts]), uuidV5.URL);
const quote = (value) => "'" + value.replaceAll("'", "''") + "'";
let requestNumber = 0;
const rpc = async (launch, mode = "start", fault) => {
  const application = `launch_${++requestNumber}`;
  const drain = Promise.withResolvers();
  if (mode !== "evidence") done.set(application, drain);
  const spanOffset = spans.length;
  const rpcOffset = nativeRpcs.length;
  const start = performance.now();
  const response = await mf.dispatchFetch("http://localhost/", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ launch, mode, application, ...(fault ? { fault } : {}) }),
    signal: AbortSignal.timeout(120_000),
  });
  const body = await response.text();
  assert.equal(response.status, 200, body);
  if (mode !== "evidence") await drain.promise;
  const result = JSON.parse(body);
  const statements = await pg.statements(application);
  const operations = spans.slice(spanOffset);
  const launchStart = operations.find((span) => span.name === "RepairExecution.start");
  const nativeCreate = operations.find((span) => span.name === "RepairSandboxTask.create");
  return {
    application,
    ...result,
    clientWallMillis: Math.round(performance.now() - start),
    statements,
    sqlCount: statements.length,
    launchMillis:
      launchStart && nativeCreate
        ? nativeCreate.startedAtMillis - launchStart.startedAtMillis
        : undefined,
    spans: operations,
    nativeRpcs: nativeRpcs.slice(rpcOffset).filter((entry) => entry.id === launch.runId),
  };
};
const poll = async (launch, status = "succeeded") => {
  const deadline = performance.now() + 120_000;
  let result;
  while (performance.now() < deadline) {
    result = await rpc(launch, "evidence");
    if (result.state?.status === status) {
      if (result.checkpoint) snapshots.add(result.checkpoint.snapshotId);
      return result;
    }
    assert.notEqual(result.state?.status, "failed", JSON.stringify(result));
    await setTimeout(100);
  }
  assert.fail(`Native alarm did not finish: ${JSON.stringify(result)}`);
};
const record = async (name, run) => {
  if (selectedCase !== undefined && name !== selectedCase) return;
  const item = { name, passed: false };
  evidence.cases.push(item);
  try {
    Object.assign(item, await run(item));
    item.passed = true;
  } catch (error) {
    item.failure = String(error.stack ?? error);
    throw error;
  } finally {
    await save();
  }
  console.log(
    JSON.stringify({
      name,
      passed: true,
      sqlCount: item.sqlCount,
      clientWallMillis: item.clientWallMillis,
    }),
  );
  return item;
};
try {
  pg = await localPostgres(directory);
  evidence.postgres = pg.version;
  pg.sql(
    await readFile(
      new URL(import.meta.resolve("@yielded/automation/migrations/001-repairs.sql")),
      "utf8",
    ),
  );
  pg.sql("CREATE TABLE fixture_authority (run_id uuid PRIMARY KEY, allowed boolean NOT NULL)");
  pg.sql(`CREATE TABLE sandbox_workspace_baselines (
    class_identity text NOT NULL, repository text NOT NULL, setup_fingerprint text NOT NULL,
    image_digest text NOT NULL, head text NOT NULL, snapshot_id text NOT NULL,
    payload jsonb NOT NULL, expires_at timestamptz NOT NULL, created_at timestamptz NOT NULL,
    PRIMARY KEY (class_identity, repository, setup_fingerprint, image_digest)
  )`);
  const fixture = await gitArtifacts(directory);
  const remoteFetches = () =>
    fixture.transfers.filter((transfer) => transfer.operation === "git-upload-pack").length;
  fixture.pin(fixture.sources.base.repositoryName, fixture.base);
  const launch = (extra = {}) => {
    const attemptId = randomUUID();
    const runId = identity("run", attemptId);
    const result = {
      runId,
      origin: {
        incidentId: randomUUID(),
        attemptId,
        evidenceManifestId: randomUUID(),
        repository: "fixture/project",
        evidenceHash: "b".repeat(64),
        offendingSha: fixture.base,
        targetSha: fixture.base,
        deadlineAtMs: Date.now() + 15 * 60_000,
      },
      launch: {
        seedRepository: fixture.sources.base.repositoryName,
        githubRepositoryId: 1,
        targetSha: fixture.base,
        targetRef: "main",
        title: "Synthetic repair",
        prompt: "Repair the local fixture",
        modelSettings: { model: "fixture", reasoningEffort: "high", fast: false },
        evidenceHash: "b".repeat(64),
        allowedScopes: ["repository_read", "repository_write"],
        promptPolicy: "fixture",
      },
      ...extra,
    };
    launches.set(runId, result);
    pg.sql(`INSERT INTO fixture_authority VALUES ('${runId}', true)`);
    return result;
  };
  await build({
    cwd: root,
    config: false,
    entry: { "launch-worker": "test/e2e/launch.worker.ts" },
    outDir: bundle,
    outExtensions: () => ({ js: ".mjs" }),
    platform: "neutral",
    target: "es2022",
    dts: false,
    deps: {
      alwaysBundle: [/^(?!cloudflare:|node:)/],
      neverBundle: [/^(cloudflare:|node:)/],
      onlyBundle: false,
    },
  });
  evidence.workerSha256 = createHash("sha256")
    .update(await readFile(join(bundle, "launch-worker.mjs")))
    .digest("hex");
  let revokeOnSource;
  let invalidateOnSource;
  let hiddenRepository;
  let concurrentSeed;
  let replaceAfterLog;
  let metadataOverride;
  let replaceAfterPush = false;
  let recreatedPush;
  let unavailableHeldInfo = false;
  let checkpointHook;
  let coordinatorSource = {
    ...fixture.sources.base,
    artifactsRef: `refs/heads/repair-source/${fixture.base}`,
  };
  const coordinatorExecutions = [];
  let losePreparationReply = false;
  const configuration = {
    name: runName,
    scriptPath: join(bundle, "launch-worker.mjs"),
    modulesRoot: bundle,
    modules: true,
    compatibilityDate: "2026-09-30",
    compatibilityFlags: ["nodejs_compat"],
    cf: false,
    telemetry: { enabled: false },
    log: new Log(LogLevel.WARN),
    handleStructuredLogs({ message, level }) {
      if (message.startsWith("LAUNCH_SPAN "))
        spans.push(JSON.parse(message.slice("LAUNCH_SPAN ".length)));
      else if (message.startsWith("LAUNCH_RPC "))
        nativeRpcs.push(JSON.parse(message.slice("LAUNCH_RPC ".length)));
      else if (message.startsWith("LAUNCH_PREPARATION "))
        preparationJoins.push(message.slice("LAUNCH_PREPARATION ".length));
      else if (message.startsWith("LAUNCH_HISTORY "))
        harnessHistory.push({
          ...JSON.parse(message.slice("LAUNCH_HISTORY ".length)),
          remoteFetches: remoteFetches(),
        });
      else if (message.startsWith("LAUNCH_HISTORY_CLEANUP "))
        historyCleanupFailures.push(JSON.parse(message.slice("LAUNCH_HISTORY_CLEANUP ".length)));
      else if (message.startsWith("LAUNCH_PREPARATION_CAPTURE "))
        preparationCaptures.push(JSON.parse(message.slice("LAUNCH_PREPARATION_CAPTURE ".length)));
      else if (message.startsWith("LAUNCH_RECIPE_READ "))
        recipeReads.push(JSON.parse(message.slice("LAUNCH_RECIPE_READ ".length)));
      else if (message.startsWith("LAUNCH_DONE ")) {
        const id = message.slice("LAUNCH_DONE ".length);
        done.get(id)?.resolve();
        done.delete(id);
      } else if (level === "warn" || level === "error") console.error(message);
    },
    containerEngine: {
      localDocker: { socketPath: dockerHost, containerEgressInterceptorImage: proxyId },
    },
    durableObjects: {
      SANDBOX: {
        className: "Sandbox",
        useSQLite: true,
        container: { images: [{ name: "harness", image: imageId }] },
      },
      RUNS: { className: "RepairRun", useSQLite: true },
      PREPARATIONS: { className: "PreparationOwner", useSQLite: true },
      COMPAT_PREPARATIONS: { className: "CompatiblePreparationOwner", useSQLite: true },
    },
    bindings: { SANDBOX_MAX_INSTANCES: "8" },
    hyperdrives: { DATABASE: pg.url },
    serviceBindings: {
      FIXTURE_PREPARATION: async (request) => {
        if (new URL(request.url).pathname === "/source") return Response.json(coordinatorSource);
        const execution = await request.json();
        coordinatorExecutions.push(execution);
        if (execution.result.baseline)
          snapshots.add(execution.result.baseline.checkpoint.snapshotId);
        if (losePreparationReply) {
          losePreparationReply = false;
          return new Response(null, { status: 503 });
        }
        return Response.json(null);
      },
      FIXTURE_CHECKPOINT: async (request) => {
        const receipt = await request.json();
        snapshots.add(receipt.checkpoint.snapshotId);
        capturedCheckpoints.push(receipt);
        await checkpointHook?.(receipt);
        return Response.json(null);
      },
      FIXTURE_ARTIFACTS: async (request) => {
        const { operation, name } = await request.clone().json();
        if (revokeOnSource && operation === "info") {
          pg.sql(`UPDATE fixture_authority SET allowed = false WHERE run_id = '${revokeOnSource}'`);
          revokeOnSource = undefined;
        }
        if (invalidateOnSource && operation === "info") {
          const change = invalidateOnSource;
          invalidateOnSource = undefined;
          change.original = pg.sql(
            `SELECT state FROM repair_executions WHERE run_id = '${change.runId}'`,
          );
          pg.sql(
            `UPDATE repair_executions SET state = ${quote(change.malformed ? "invalid-json" : JSON.stringify({ ...JSON.parse(change.original), retirement: "draining" }))}
              WHERE run_id = '${change.runId}'`,
          );
        }
        if (name === hiddenRepository) return new Response(null, { status: 404 });
        if (name === concurrentSeed && operation === "create") {
          concurrentSeed = undefined;
          await fixture.control(request);
          return new Response(null, { status: 409 });
        }
        const response = await fixture.control(request);
        if (unavailableHeldInfo && name === recreatedPush?.name && operation === "info") {
          unavailableHeldInfo = false;
          return new Response(null, { status: 503 });
        }
        if (name === replaceAfterLog && operation === "log") {
          fixture.generation(name, randomUUID());
          replaceAfterLog = undefined;
        }
        return operation === "info" && metadataOverride
          ? Response.json({ ...(await response.json()), ...metadataOverride })
          : response;
      },
    },
    outboundService: async (request) => {
      const response = await fixture.transport(request);
      if (replaceAfterPush && new URL(request.url).pathname.endsWith("/git-receive-pack")) {
        const name = fixture.transfers.at(-1).name;
        fixture.generation(name, randomUUID());
        recreatedPush = { name, offset: fixture.operations.length };
        unavailableHeldInfo = true;
        replaceAfterPush = false;
      }
      return response;
    },
    durableObjectsPersist: join(directory, "state"),
  };
  let options = convertV4MiniflareOptions(configuration);
  mf = new Miniflare(options);
  const first = launch();
  let saved;
  await record("startup-read-authority", async (item) => {
    const source = {
      ...fixture.sources.base,
      artifactsRef: `refs/heads/repair-source/${fixture.base}`,
    };
    const metadata = async (kind) => {
      const offset = fixture.operations.length;
      const response = await mf.dispatchFetch("http://localhost/metadata", {
        method: "POST",
        body: JSON.stringify({ kind, source }),
        signal: AbortSignal.timeout(30_000),
      });
      assert.equal(response.status, 200, await response.clone().text());
      return {
        ...(await response.json()),
        operations: fixture.operations.slice(offset).map((entry) => entry.operation),
      };
    };
    const rejectedGeneration = (result) => {
      assert.equal(result.exit, "Failure");
      assert.match(result.failure, /ArtifactsRepositoryGenerationMismatchError/);
    };
    item.pin = await metadata("pin");
    item.head = await metadata("head");
    for (const result of [item.pin, item.head]) {
      assert.equal(result.exit, "Success", result.failure);
      assert.equal(result.value, fixture.base);
    }
    item.rejectedMetadata = [];
    try {
      fixture.generation(source.repositoryName, randomUUID());
      const result = await metadata("pin");
      rejectedGeneration(result);
      assert.equal(result.operations.includes("log"), false);
      item.rejectedMetadata.push(result);
    } finally {
      fixture.generation(source.repositoryName, source.repositoryId);
    }
    for (const replacement of [
      { name: "different-source" },
      { remote: "https://fixture.invalid/git/fixture/different-source.git" },
    ]) {
      try {
        metadataOverride = replacement;
        const result = await metadata("pin");
        rejectedGeneration(result);
        assert.equal(result.operations.includes("log"), false);
        item.rejectedMetadata.push(result);
      } finally {
        metadataOverride = undefined;
      }
    }
    item.replacedDuringRead = [];
    for (const kind of ["pin", "head"]) {
      try {
        replaceAfterLog = source.repositoryName;
        const result = await metadata(kind);
        rejectedGeneration(result);
        assert.deepEqual(result.operations.slice(-2), ["log", "info"]);
        item.replacedDuringRead.push(result);
      } finally {
        replaceAfterLog = undefined;
        fixture.generation(source.repositoryName, source.repositoryId);
      }
    }
    const sourceInput = launch();
    const sourceOffset = fixture.operations.length;
    item.source = await rpc(sourceInput, "source");
    assert.equal(item.source.exit, "Success", item.source.failure);
    item.sourceOperations = fixture.operations.slice(sourceOffset);
    try {
      replaceAfterLog = source.repositoryName;
      item.replacedSource = await rpc(sourceInput, "source");
      assert.equal(item.replacedSource.exit, "Failure");
      assert.match(item.replacedSource.failure, /The repair source generation changed/);
    } finally {
      replaceAfterLog = undefined;
      fixture.generation(source.repositoryName, source.repositoryId);
    }

    const nativeOffset = fixture.operations.length;
    const spanOffset = spans.length;
    const freshInput = launch();
    item.fresh = await rpc(freshInput);
    assert.equal(item.fresh.exit, "Success", item.fresh.failure);
    const native = await poll(freshInput);
    assert.equal(native.turns, 1);
    assert.ok(native.state.savedCheckpoint);
    item.nativeOperations = fixture.operations.slice(nativeOffset);
    item.nativeSpans = spans.slice(spanOffset);
    const retained = pg.sql(
      `SELECT fingerprint || ':' || intent FROM repair_execution_receipts WHERE run_id = '${freshInput.runId}'`,
    );
    item.retry = await rpc(freshInput, "retain-launch");
    assert.equal(item.retry.exit, "Success", item.retry.failure);
    item.conflict = await rpc(freshInput, "retain-launch", "changed-retained-intent");
    assert.equal(item.conflict.exit, "Failure");
    assert.match(
      item.conflict.failure,
      /The repair operation already has different immutable inputs/,
    );
    const state = pg.sql(
      `SELECT state FROM repair_executions WHERE run_id = '${freshInput.runId}'`,
    );
    try {
      pg.sql(
        `UPDATE repair_executions SET state = jsonb_set(state::jsonb, '{origin,deadlineAtMs}', '0')::text WHERE run_id = '${freshInput.runId}'`,
      );
      item.expired = await rpc(freshInput, "retain-launch");
      assert.equal(item.expired.exit, "Failure");
      assert.match(item.expired.failure, /Repair admission authority expired/);
    } finally {
      pg.sql(
        `UPDATE repair_executions SET state = ${quote(state)} WHERE run_id = '${freshInput.runId}'`,
      );
    }
    assert.equal((await rpc(freshInput, "retire")).exit, "Success");
    item.retired = await rpc(freshInput, "retain-launch");
    assert.equal(item.retired.exit, "Failure");
    assert.match(item.retired.failure, /Repair admission authority expired/);
    for (const result of [item.expired, item.retired])
      assert.ok(result.statements.includes("ROLLBACK"));
    assert.equal(
      pg.sql(
        `SELECT fingerprint || ':' || intent FROM repair_execution_receipts WHERE run_id = '${freshInput.runId}'`,
      ),
      retained,
    );

    const held = launch();
    const heldSpanOffset = spans.length;
    replaceAfterPush = true;
    assert.equal((await rpc(held)).exit, "Success");
    const heldDeadline = performance.now() + 120_000;
    while (performance.now() < heldDeadline) {
      item.heldVerification = spans
        .slice(heldSpanOffset)
        .find(
          (span) =>
            span.name === "ArtifactsCheckout.verifyExpectedBoundGenerationHead" &&
            span.outcome === "Failure",
        );
      if (item.heldVerification) break;
      await setTimeout(100);
    }
    assert.ok(item.heldVerification, "Observe native held-client publication verification");
    assert.match(item.heldVerification.failure, /ArtifactsRepositoryGenerationMismatchError/);
    item.heldClient = await rpc(held, "evidence");
    assert.equal(item.heldClient.state.status, "running");
    assert.equal(item.heldClient.state.savedCheckpoint, null);
    assert.ok(recreatedPush, "Replace the provider generation after an actual native Git push");
    item.heldClientOperations = fixture.operations.slice(recreatedPush.offset);
    assert.equal(
      item.heldClientOperations.some((entry) => entry.operation === "log"),
      false,
      "A held client must refresh generation before observing the replacement head",
    );
    assert.equal(
      item.heldClientOperations.filter((entry) => entry.operation === "info").length,
      2,
      "A transient held-client refresh failure must retry before rejecting the new generation",
    );
    assert.equal(fixture.tokens.size, 0, "Replaced-generation failure must still revoke tokens");
    assert.equal((await rpc(held, "retire")).exit, "Success");
    const receiptReads = (result) =>
      result.statements.filter((sql) =>
        sql.startsWith("SELECT fingerprint, intent FROM repair_execution_receipts"),
      ).length;
    item.readCounts = {
      pin: item.pin.operations.filter((operation) => operation === "info").length,
      head: item.head.operations.filter((operation) => operation === "info").length,
      source: item.sourceOperations.filter((entry) => entry.operation === "info").length,
      freshReceipt: receiptReads(item.fresh),
      retryReceipt: receiptReads(item.retry),
      conflictReceipt: receiptReads(item.conflict),
    };
    assert.deepEqual(item.readCounts, {
      pin: 2,
      head: 2,
      source: 2,
      freshReceipt: 0,
      retryReceipt: 1,
      conflictReceipt: 1,
    });
  });
  await record("prepare-native-baseline", async (item) => {
    // The preceding launch publishes a compatible baseline; this fault needs a cold registry.
    pg.sql("TRUNCATE sandbox_workspace_baselines");
    const source = {
      ...fixture.sources.base,
      artifactsRef: `refs/heads/repair-source/${fixture.base}`,
    };
    const prepare = async (input) => {
      const response = await mf.dispatchFetch("http://localhost/prepare", {
        method: "POST",
        body: JSON.stringify(input),
        signal: AbortSignal.timeout(120_000),
      });
      const body = await response.text();
      assert.equal(response.status, 200, body);
      const result = JSON.parse(body);
      if (result.baseline) snapshots.add(result.baseline.checkpoint.snapshotId);
      return result;
    };
    const sessionId = `fixture-preparation:${source.commitSha}`;
    const input = { sessionId, turnId: sessionId, source };
    const preparationFetches = remoteFetches();
    await mf.setOptions(
      convertV4MiniflareOptions({
        ...configuration,
        bindings: { ...configuration.bindings, FIXTURE_FAIL_HISTORY_CLEANUP: true },
      }),
    );
    item.interrupted = await prepare(input);
    assert.equal(item.interrupted.status, "failed");
    assert.match(item.interrupted.failure, /remove/);
    assert.equal(historyCleanupFailures.length, 1);
    item.beforeCleanup = historyCleanupFailures[0];
    assert.equal(item.beforeCleanup.shallow, "false");
    assert.equal(item.beforeCleanup.parentSource, "history-4");
    assert.equal(item.beforeCleanup.commits, 6);
    assert.equal(item.beforeCleanup.bare, "true");
    assert.equal(item.beforeCleanup.transportCommits, 6);
    assert.equal(item.beforeCleanup.installs, 0);
    item.interruptedFetches = remoteFetches() - preparationFetches;
    assert.equal(pg.sql("SELECT count(*) FROM sandbox_workspace_baselines"), "0");
    options = convertV4MiniflareOptions({
      ...configuration,
      bindings: {
        ...configuration.bindings,
        FIXTURE_HISTORY_TRANSPORT_PATH: item.beforeCleanup.path,
      },
    });
    await mf.setOptions(options);
    const retryFetches = remoteFetches();
    const prepared = await prepare(input);
    item.retryFetches = remoteFetches() - retryFetches;
    assert.equal(item.retryFetches, 0, "Imported history must survive preparation retry");
    item.preparationFetches = remoteFetches() - preparationFetches;
    assert.equal(prepared.status, "prepared");
    assert.equal(prepared.baseline.checkpoint.instanceId, item.beforeCleanup.instanceId);
    item.capture = preparationCaptures.filter(
      (capture) => capture.instanceId === prepared.baseline.checkpoint.instanceId,
    );
    assert.equal(item.capture.length, 1);
    assert.equal(item.capture[0].installs, 1);
    assert.equal(
      item.capture[0].transportExists,
      false,
      "Native capture must not retain the imported transport object store",
    );
    assert.equal(
      prepared.baseline.repository,
      `fixture.invalid/fixture/${source.repositoryName}@${source.repositoryId}`,
    );
    await mf.setOptions(options);
    const replayFetches = remoteFetches();
    assert.deepEqual(await prepare(input), prepared);
    assert.equal(remoteFetches() - replayFetches, 0);
    const launchInput = launch();
    launchInput.origin.repository = "fixture/prepared";
    const turnFetches = remoteFetches();
    const historyOffset = harnessHistory.length;
    assert.equal((await rpc(launchInput)).exit, "Success");
    const native = await poll(launchInput);
    assert.equal(harnessHistory.length - historyOffset, 1);
    item.preAgent = {
      ...harnessHistory[historyOffset],
      remoteFetches: harnessHistory[historyOffset].remoteFetches - turnFetches,
    };
    item.prepared = prepared;
    item.setup = native.setupReport;
    assert.equal(native.setupReport.installation, "checkpoint");
    assert.equal(native.restoredSnapshotId, prepared.baseline.checkpoint.snapshotId);
    assert.equal(item.preAgent.shallow, "false");
    assert.equal(item.preAgent.parentSource, "history-4");
    assert.equal(item.preAgent.installs, 1, "Restoring preparation must not reinstall");
    assert.deepEqual(
      item.preAgent.commits,
      execute("git", ["-C", join(directory, "source"), "rev-list", fixture.base]).split("\n"),
    );
    assert.equal(
      item.preAgent.remoteFetches,
      0,
      "Prepared history must be available before coding",
    );
    await mf.setOptions(options);
    assert.equal((await rpc(launchInput)).exit, "Success");
    assert.equal(preparationJoins.filter((id) => id === launchInput.runId).length, 1);
    assert.equal((await rpc(launchInput, "retire")).exit, "Success");
    const explicit = await prepare({
      sessionId: randomUUID(),
      turnId: randomUUID(),
      source,
      preparationKey: "fixture-shared",
    });
    assert.equal(explicit.baseline.repository, "fixture.invalid/fixture/prepared:fixture-shared");
    return { prepared, explicit, setup: native.setupReport };
  });
  await record("prepare-setup-failure", async (item) => {
    const call = async (path, input) => {
      const response = await mf.dispatchFetch(`http://localhost/${path}`, {
        method: "POST",
        body: JSON.stringify(input),
      });
      assert.equal(response.status, 200, await response.clone().text());
      return response.json();
    };
    const attempt = async (input) => {
      const offset = spans.length;
      const readOffset = recipeReads.length;
      const fetches = remoteFetches();
      const started = performance.now();
      const result = await call("prepare", input);
      const operations = spans.slice(offset);
      if (result.baseline) snapshots.add(result.baseline.checkpoint.snapshotId);
      return {
        result,
        wallMillis: Math.round(performance.now() - started),
        remoteFetches: remoteFetches() - fetches,
        starts: operations.filter((span) => span.name === "SandboxContainer.nativeStart").length,
        installs: operations.filter((span) => span.name === "repository-install").length,
        captureMillis: operations
          .filter((span) => span.name === "SandboxContainer.nativeSnapshot")
          .map((span) => span.durationMillis),
        reads: recipeReads.slice(readOffset),
        spans: operations,
      };
    };
    item.failures = [];
    // Collect the real failures before checking the terminal preparation contract.
    for (const kind of ["oversized", "invalid", "install"]) {
      const source = await fixture.failedSetup(`${kind}-preparation-source`, kind);
      fixture.pin(source.repositoryName, source.commitSha);
      const sessionId = `fixture-prepare-${source.repositoryName}`;
      const input = {
        sessionId,
        turnId: sessionId,
        source: { ...source, artifactsRef: `refs/heads/repair-source/${source.commitSha}` },
      };
      const sample = { kind, input };
      item.failures.push(sample);
      sample.failed = await attempt(input);
      sample.owner = await call("preparation-state", { sessionId, destroy: false });
      fixture.moveMain(source.repositoryName, fixture.candidate);
      fixture.pin(source.repositoryName, fixture.candidate);
      sample.duplicate = await attempt(input);
    }
    for (const sample of item.failures) {
      const { failed, duplicate, owner } = sample;
      assert.equal(failed.result.status, "failed");
      assert.equal(failed.result.setupFailures.length, 1, failed.result.failure);
      assert.equal(failed.result.setupFailures[0].recovery, "terminal");
      assert.equal(failed.installs, sample.kind === "install" ? 1 : 0);
      assert.equal(failed.captureMillis.length, 0);
      if (sample.kind === "oversized") {
        assert.match(failed.result.failure, /exceeds 16 KiB/);
        assert.deepEqual(failed.reads, [
          {
            sourceHead: sample.input.source.commitSha,
            maxBytes: 16 * 1024 + 1,
            stdoutBytes: 16 * 1024 + 1,
            stderrBytes: 0,
            exitCode: 0,
            timedOut: false,
            truncated: true,
          },
        ]);
      } else if (sample.kind === "invalid")
        assert.match(failed.result.failure, /Invalid repository setup recipe/);
      else assert.equal(failed.result.setupFailures[0].report.steps[0].exitCode, 23);
      assert.equal(failed.result.reason, "setup-failed");
      assert.deepEqual(failed.result.failureKinds, ["Fail"]);
      assert.equal(owner.description.current, null);
      assert.equal(owner.checkpoint, null);
      assert.equal(duplicate.result.reason, "workspace-lost");
      assert.equal(duplicate.remoteFetches, 0);
      assert.equal(duplicate.starts, 0);
      assert.equal(duplicate.installs, 0);
      assert.equal(duplicate.captureMillis.length, 0);
      assert.deepEqual(duplicate.reads, []);

      const sessionId = `${sample.input.sessionId}:next`;
      const commitSha = fixture.head(sample.input.source.repositoryName);
      const nextInput = {
        ...sample.input,
        sessionId,
        turnId: sessionId,
        source: {
          ...sample.input.source,
          commitSha,
          artifactsRef: `refs/heads/repair-source/${commitSha}`,
        },
      };
      if (sample.kind === "oversized") {
        await mf.setOptions(
          convertV4MiniflareOptions({
            ...configuration,
            bindings: { ...configuration.bindings, FIXTURE_RECIPE_STDERR: true },
          }),
        );
        sample.uncertain = await attempt(nextInput);
        sample.uncertainOwner = await call("preparation-state", { sessionId, destroy: false });
        await mf.setOptions(options);
        assert.equal(sample.uncertain.result.status, "failed");
        assert.equal(sample.uncertain.result.reason, null);
        assert.equal(sample.uncertain.result.setupFailures[0].recovery, "retryable");
        assert.equal(sample.uncertain.reads.length, 1);
        const read = sample.uncertain.reads[0];
        assert.equal(read.exitCode, 0);
        assert.equal(read.timedOut, false);
        assert.equal(read.truncated, true);
        assert.ok(read.stdoutBytes < 16 * 1024);
        assert.equal(read.stderrBytes, 16 * 1024 + 1);
        assert.notEqual(sample.uncertainOwner.description.current, null);
        assert.equal(sample.uncertain.installs, 0);
        assert.equal(sample.uncertain.captureMillis.length, 0);
      }
      sample.next = await attempt(nextInput);
      if (sample.kind === "oversized") assert.equal(sample.next.starts, 0);
      assert.equal(sample.next.result.status, "prepared", sample.next.result.failure);
      assert.equal(sample.next.result.baseline.checkpoint.head, fixture.candidate);
      assert.equal(sample.next.installs, 1);
      assert.equal(sample.next.captureMillis.length, 1);
    }
  });
  await record("prepare-terminal-baseline", async (item) => {
    const source = {
      ...fixture.sources.base,
      artifactsRef: `refs/heads/repair-source/${fixture.base}`,
    };
    const request = (sessionId) => ({
      sessionId,
      turnId: sessionId,
      preparationKey: "fixture-terminal",
      source,
    });
    const call = async (path, input) => {
      const response = await mf.dispatchFetch(`http://localhost/${path}`, {
        method: "POST",
        body: JSON.stringify(input),
      });
      assert.equal(response.status, 200, await response.clone().text());
      return response.json();
    };
    const attempt = async (input) => {
      const offset = spans.length;
      const fetches = remoteFetches();
      const result = await call("prepare", input);
      const operations = spans.slice(offset);
      if (result.baseline) snapshots.add(result.baseline.checkpoint.snapshotId);
      return {
        result,
        remoteFetches: remoteFetches() - fetches,
        starts: operations.filter((span) => span.name === "SandboxContainer.nativeStart").length,
        installs: operations.filter((span) => span.name === "repository-install").length,
        captures: operations.filter((span) => span.name === "SandboxContainer.nativeSnapshot")
          .length,
        spans: operations,
      };
    };
    const noReplay = (sample) => {
      assert.equal(sample.remoteFetches, 0);
      assert.equal(sample.starts, 0);
      assert.equal(sample.installs, 0);
      assert.equal(sample.captures, 0);
    };
    const interruptedOptions = convertV4MiniflareOptions({
      ...configuration,
      bindings: { ...configuration.bindings, FIXTURE_FAIL_HISTORY_CLEANUP: true },
    });
    const normalOptions = convertV4MiniflareOptions({
      ...configuration,
      bindings: { ...configuration.bindings, FIXTURE_CHECKPOINT_EVIDENCE: true },
    });
    const lost = request("fixture-pre-capture-loss");
    await mf.setOptions(interruptedOptions);
    item.beforeLoss = await attempt(lost);
    assert.match(item.beforeLoss.result.failure, /remove/);
    assert.equal(item.beforeLoss.installs, 0);
    item.lostOwner = await call("preparation-state", {
      sessionId: lost.sessionId,
      destroy: true,
    });
    assert.equal(item.lostOwner.checkpoint, null);
    assert.equal(item.lostOwner.description.current, null);
    await mf.setOptions(normalOptions);
    item.loss = await attempt(lost);
    item.lossDuplicate = await attempt(lost);
    for (const sample of [item.loss, item.lossDuplicate]) {
      assert.equal(sample.result.reason, "workspace-lost");
      noReplay(sample);
    }

    const revoked = request("fixture-source-revoked");
    await mf.setOptions(interruptedOptions);
    item.beforeRevocation = await attempt(revoked);
    assert.match(item.beforeRevocation.result.failure, /remove/);
    assert.equal(item.beforeRevocation.installs, 0);
    await mf.setOptions(normalOptions);
    const replacement = randomUUID();
    fixture.generation(source.repositoryName, replacement);
    try {
      item.revoked = await attempt(revoked);
      assert.equal(item.revoked.result.reason, "source-changed");
      assert.match(item.revoked.result.failure, /ArtifactsRepositoryGenerationMismatchError/);
      noReplay(item.revoked);
      item.revokedOwner = await call("preparation-state", {
        sessionId: revoked.sessionId,
        destroy: false,
      });
      assert.equal(item.revokedOwner.description.current, null);
      assert.equal(item.revokedOwner.checkpoint, null);
      item.revokedDuplicate = await attempt(revoked);
      assert.equal(item.revokedDuplicate.result.reason, "workspace-lost");
      noReplay(item.revokedDuplicate);
      fixture.pin(source.repositoryName, fixture.candidate);
      const next = {
        ...request("fixture-next-preparation-generation"),
        source: {
          ...source,
          repositoryId: replacement,
          commitSha: fixture.candidate,
          artifactsRef: `refs/heads/repair-source/${fixture.candidate}`,
        },
      };
      item.nextCapture = await attempt({ ...next, fault: "drop-publication-reply" });
      assert.equal(item.nextCapture.result.status, "failed");
      assert.equal(item.nextCapture.result.reason, null);
      item.capturedOwner = await call("preparation-state", {
        sessionId: next.sessionId,
        destroy: true,
      });
      assert.equal(item.capturedOwner.checkpoint.head, fixture.candidate);
      item.nextGeneration = await attempt(next);
      assert.equal(item.nextGeneration.result.status, "prepared");
      assert.equal(item.nextGeneration.result.baseline.checkpoint.head, fixture.candidate);
      assert.equal(
        item.nextGeneration.result.baseline.checkpoint.snapshotId,
        item.capturedOwner.checkpoint.snapshotId,
      );
      noReplay(item.nextGeneration);
    } finally {
      fixture.generation(source.repositoryName, source.repositoryId);
      await mf.setOptions(options);
    }
  });
  await record("prepare-events", async (item) => {
    pg.sql("TRUNCATE sandbox_workspace_baselines");
    const repository = "fixture/project";
    const call = async (path, compatible = false) => {
      const response = await mf.dispatchFetch(`http://localhost/coordinator-${path}`, {
        method: "POST",
        body: JSON.stringify({ repository, compatible }),
      });
      assert.equal(response.status, 200, await response.clone().text());
      return response.json();
    };
    const until = async (predicate, compatible = false) => {
      const deadline = performance.now() + 120_000;
      while (performance.now() < deadline) {
        const value = await call("state", compatible);
        if (predicate(value)) return value;
        await setTimeout(100);
      }
      assert.fail("The native preparation alarm did not reach the expected state");
    };
    const select = (commitSha, repositoryId = fixture.sources.base.repositoryId) => {
      fixture.pin(fixture.sources.base.repositoryName, commitSha);
      coordinatorSource = {
        ...fixture.sources.base,
        repositoryId,
        commitSha,
        artifactsRef: `refs/heads/repair-source/${commitSha}`,
      };
    };
    const settled = (source) =>
      until(
        ({ state, alarm }) =>
          alarm === undefined &&
          state?.completed?.checkpoint?.head === source.commitSha &&
          state.request.source.repositoryId === source.repositoryId,
      );

    select(fixture.base);
    losePreparationReply = true;
    item.accepted = await call("request");
    item.pending = await until(
      ({ state, alarm }) => state?.completed === null && alarm?.attempts > 0,
    );
    assert.equal(coordinatorExecutions.length, 1);
    assert.equal(coordinatorExecutions[0].result.status, "prepared");
    const original = coordinatorExecutions[0];
    assert.deepEqual(item.pending.state.request, original.input.request);
    assert.equal(item.pending.state.generation, original.input.generation);
    assert.ok(item.pending.alarm.retryAt);

    // Restart workerd with durable SQLite intact, then replay/coalesce source events.
    await mf.setOptions(
      convertV4MiniflareOptions({
        ...configuration,
        bindings: { ...configuration.bindings, FIXTURE_RELOAD: "preparation" },
      }),
    );
    item.reloaded = await call("state");
    assert.deepEqual(item.reloaded.state, item.pending.state);
    assert.deepEqual(item.reloaded.alarm.retryAt, item.pending.alarm.retryAt);
    select(fixture.candidate);
    await Promise.all(Array.from({ length: 5 }, () => call("request")));
    item.advanced = await settled(coordinatorSource);
    assert.deepEqual(
      coordinatorExecutions[1],
      original,
      "Lost replies replay the retained generation and capture",
    );
    assert.equal(new Set(coordinatorExecutions.map(({ input }) => input.generation)).size, 2);
    assert.equal(coordinatorExecutions.at(-1).input.request.source.commitSha, fixture.candidate);
    assert.notEqual(item.advanced.state.generation, original.input.generation);
    const count = coordinatorExecutions.length;
    await Promise.all(Array.from({ length: 5 }, () => call("request")));
    await settled(coordinatorSource);
    assert.equal(
      coordinatorExecutions.length,
      count,
      "Completed matching preparations do not start another owner",
    );

    // A replacement provider generation at the same commit is still a new source.
    select(fixture.changed);
    losePreparationReply = true;
    await call("request");
    item.generationPending = await until(
      ({ state, alarm }) => state?.completed === null && alarm?.attempts > 0,
    );
    const retained = coordinatorExecutions.at(-1);
    const replacement = randomUUID();
    fixture.generation(fixture.sources.base.repositoryName, replacement);
    select(fixture.changed, replacement);
    await call("request");
    item.replaced = await settled(coordinatorSource);
    assert.deepEqual(coordinatorExecutions.at(-2), retained);
    assert.equal(item.replaced.state.request.source.repositoryId, replacement);
    assert.notEqual(item.replaced.state.generation, item.generationPending.state.generation);
    assert.equal(item.replaced.state.completed.checkpoint.head, fixture.changed);
    await call("request", true);
    item.compatible = await until(
      ({ state }) => state?.completed?.checkpoint?.head === fixture.changed,
      true,
    );
    assert.equal(
      item.compatible.tables.some((name) => name.startsWith("repair_sandbox_")),
      false,
    );
    assert.equal(
      item.compatible.execution.tables.some((name) => name.startsWith("repair_sandbox_")),
      false,
    );
    await mf.setOptions(
      convertV4MiniflareOptions({
        ...configuration,
        bindings: {
          ...configuration.bindings,
          FIXTURE_REPOSITORY_SETUP_PATH: ".fixture-disabled-setup.json",
        },
      }),
    );
    await call("request", true);
    item.disabled = await until(({ state }) => state?.completed?.status === "disabled", true);
    assert.notEqual(item.disabled.state.generation, item.compatible.state.generation);
    assert.notEqual(
      item.disabled.state.completed.configurationFingerprint,
      item.compatible.state.completed.configurationFingerprint,
    );
    await mf.setOptions(options);
    await call("request", true);
    item.reenabled = await until(
      ({ state }) => state?.completed?.checkpoint?.head === fixture.changed,
      true,
    );
    assert.notEqual(item.reenabled.state.generation, item.disabled.state.generation);
    assert.equal(
      item.reenabled.state.completed.configurationFingerprint,
      item.compatible.state.completed.configurationFingerprint,
    );
    item.executions = coordinatorExecutions;
    item.scope =
      "Real workerd entrypoints, SQLite, alarms, Postgres, Docker captures and reload; setup configuration invalidation and task-provider isolation on the compatible owner. Scripted source authority and Artifacts metadata; no model provider or hosted execution.";
    fixture.generation(fixture.sources.base.repositoryName, fixture.sources.base.repositoryId);
    await mf.setOptions(options);
  });
  await record("prepare-current-baseline", async (item) => {
    pg.sql("TRUNCATE sandbox_workspace_baselines");
    await mf.setOptions(
      convertV4MiniflareOptions({
        ...configuration,
        bindings: { ...configuration.bindings, FIXTURE_CHECKPOINT_EVIDENCE: true },
      }),
    );
    const call = async (path, input) => {
      const response = await mf.dispatchFetch(`http://localhost/${path}`, {
        method: "POST",
        body: JSON.stringify(input),
      });
      assert.equal(response.status, 200, await response.clone().text());
      return response.json();
    };
    const source = (commitSha) => ({ ...fixture.sources.base, commitSha });
    const prepare = async (accepted, preparationKey, generation = "current", fault) => {
      const sessionId = `${generation}:${preparationKey ?? "source"}:${accepted.commitSha}`;
      fixture.pin(accepted.repositoryName, accepted.commitSha);
      const input = {
        sessionId,
        turnId: sessionId,
        preparationKey,
        fault,
        source: { ...accepted, artifactsRef: `refs/heads/repair-source/${accepted.commitSha}` },
      };
      const fetches = remoteFetches();
      const spanOffset = spans.length;
      const captureOffset = preparationCaptures.length;
      const checkpointOffset = capturedCheckpoints.length;
      const started = performance.now();
      const result = await call("prepare", input);
      const operations = spans.slice(spanOffset);
      console.log(
        "BASELINE_SAMPLE " +
          JSON.stringify({
            sessionId,
            status: result.status,
            reason: result.reason,
            head: result.baseline?.checkpoint.head,
            wallMillis: Math.round(performance.now() - started),
          }),
      );
      return {
        input,
        result,
        wallMillis: Math.round(performance.now() - started),
        remoteFetches: remoteFetches() - fetches,
        clones: operations.filter(
          (span) => span.name === "ArtifactsCheckout.cloneSourceIntoSandbox",
        ).length,
        installs: operations.filter((span) => span.name === "repository-install").length,
        captureMillis: operations
          .filter((span) => span.name === "SandboxContainer.nativeSnapshot")
          .map((span) => span.durationMillis),
        captures: preparationCaptures.slice(captureOffset),
        checkpoints: capturedCheckpoints.slice(checkpointOffset),
        spans: operations,
      };
    };
    const exact = (sample, head) => {
      assert.ok(["prepared", "reused"].includes(sample.result.status), sample.result.failure);
      assert.equal(sample.result.baseline.checkpoint.head, head);
    };
    const noReplay = (sample) => {
      assert.equal(sample.remoteFetches, 0);
      assert.equal(sample.clones, 0);
      assert.equal(sample.installs, 0);
      assert.equal(sample.captureMillis.length, 0);
      assert.equal(sample.checkpoints.length, 0);
    };
    const row = (key) =>
      JSON.parse(
        pg.sql(`SELECT payload FROM sandbox_workspace_baselines
      WHERE repository = ${quote(key)} ORDER BY created_at DESC LIMIT 1`),
      );
    const due = (snapshotId) =>
      pg.sql(`UPDATE sandbox_workspace_baselines
      SET expires_at = now() + interval '1 day' WHERE snapshot_id = ${quote(snapshotId)}`);
    // Fail real PG publication only after the native owner acknowledged its capture.
    const interruptPublication = async (accepted, key, generation, destroy = true) => {
      let hidden = false;
      checkpointHook = () => {
        checkpointHook = undefined;
        pg.sql("ALTER TABLE sandbox_workspace_baselines RENAME TO fixture_hidden_baselines");
        hidden = true;
      };
      try {
        const interrupted = await prepare(accepted, key, generation);
        assert.equal(interrupted.result.status, "failed");
        assert.match(interrupted.result.failure, /WorkspaceBaselineError/);
        let retained = await call("preparation-state", {
          sessionId: interrupted.input.sessionId,
          destroy: false,
        });
        assert.equal(retained.preparation.result, null);
        assert.equal(retained.description.current.instanceId, retained.checkpoint.instanceId);
        assert.equal(
          retained.checkpoint.snapshotId,
          interrupted.checkpoints[0].checkpoint.snapshotId,
        );
        if (destroy) {
          retained = await call("preparation-state", {
            sessionId: interrupted.input.sessionId,
            destroy: true,
          });
          assert.equal(retained.description.current, null);
        }
        return { ...interrupted, retained };
      } finally {
        checkpointHook = undefined;
        if (hidden)
          pg.sql("ALTER TABLE fixture_hidden_baselines RENAME TO sandbox_workspace_baselines");
      }
    };

    item.base = await prepare(source(fixture.base));
    exact(item.base, fixture.base);
    assert.equal(item.base.installs, 1);
    fixture.moveMain(fixture.sources.base.repositoryName, fixture.candidate);
    item.advance = await prepare(source(fixture.candidate));
    exact(item.advance, fixture.candidate);
    assert.equal(item.advance.result.baseline.report.installation, "checkpoint");
    assert.equal(item.advance.clones, 0);
    assert.equal(item.advance.installs, 0);
    assert.equal(item.advance.remoteFetches, 1);
    assert.equal(item.advance.captureMillis.length, 1);
    assert.equal(
      item.advance.captures[0].restoredSnapshotId,
      item.base.result.baseline.checkpoint.snapshotId,
    );
    assert.equal(item.advance.captures[0].installs, 1);
    assert.equal(item.advance.captures[0].head, fixture.candidate);
    assert.equal(item.advance.captures[0].shallow, "false");
    assert.equal(item.advance.captures[0].commits, 7);
    assert.equal(item.advance.captures[0].pristine, true);
    assert.equal(
      row(item.advance.result.baseline.repository).checkpoint.snapshotId,
      item.advance.result.baseline.checkpoint.snapshotId,
    );
    item.duplicate = await prepare(source(fixture.candidate));
    assert.deepEqual(item.duplicate.result, item.advance.result);
    noReplay(item.duplicate);
    item.exactReuse = await prepare(source(fixture.candidate), undefined, "fresh-duplicate");
    assert.equal(item.exactReuse.result.status, "reused");
    assert.deepEqual(item.exactReuse.result.baseline, item.advance.result.baseline);
    noReplay(item.exactReuse);

    const launchInput = launch();
    launchInput.origin.targetSha = fixture.candidate;
    launchInput.launch.targetSha = fixture.candidate;
    const turnFetches = remoteFetches();
    const historyOffset = harnessHistory.length;
    assert.equal((await rpc(launchInput)).exit, "Success");
    const native = await poll(launchInput);
    item.restored = {
      ...harnessHistory[historyOffset],
      remoteFetches: harnessHistory[historyOffset].remoteFetches - turnFetches,
      setup: native.setupReport,
    };
    assert.equal(native.restoredSnapshotId, item.advance.result.baseline.checkpoint.snapshotId);
    assert.equal(item.restored.remoteFetches, 0);
    assert.equal(item.restored.installs, 1);
    assert.equal(item.restored.setup.installation, "checkpoint");
    assert.equal(item.restored.commits[0], fixture.candidate);
    assert.equal(item.restored.shallow, "false");
    assert.deepEqual(
      item.restored.commits,
      execute("git", ["-C", join(directory, "source"), "rev-list", fixture.candidate]).split("\n"),
    );
    assert.equal((await rpc(launchInput, "retire")).exit, "Success");

    item.changed = await prepare(source(fixture.changed));
    exact(item.changed, fixture.changed);
    assert.equal(item.changed.installs, 1);
    assert.equal(item.changed.result.baseline.report.installation, "executed");
    assert.equal(item.changed.captures[0].restoredSnapshotId, null);
    assert.equal(
      item.changed.captures[0].installs,
      1,
      "Changed inputs install in a clean container",
    );
    assert.notEqual(
      item.changed.result.baseline.checkpoint.setupFingerprint,
      item.advance.result.baseline.checkpoint.setupFingerprint,
    );

    item.retryBase = await prepare(source(fixture.base), "retry");
    exact(item.retryBase, fixture.base);
    item.interrupted = await interruptPublication(source(fixture.candidate), "retry", "current");
    assert.equal(item.interrupted.installs, 0);
    assert.equal(
      item.interrupted.retained.preparation.previousSnapshotId,
      item.retryBase.result.baseline.checkpoint.snapshotId,
    );
    item.resumed = await prepare(source(fixture.candidate), "retry");
    exact(item.resumed, fixture.candidate);
    assert.equal(
      item.resumed.result.baseline.checkpoint.snapshotId,
      item.interrupted.retained.checkpoint.snapshotId,
    );
    noReplay(item.resumed);

    item.lostPublicationReply = await prepare(
      source(fixture.candidate),
      "lost-reply",
      "current",
      "drop-publication-reply",
    );
    assert.equal(item.lostPublicationReply.result.status, "failed");
    const published = await call("preparation-state", {
      sessionId: item.lostPublicationReply.input.sessionId,
      destroy: true,
    });
    assert.equal(published.preparation.result, null);
    item.publicationRecovered = await prepare(source(fixture.candidate), "lost-reply");
    exact(item.publicationRecovered, fixture.candidate);
    assert.equal(
      item.publicationRecovered.result.baseline.checkpoint.snapshotId,
      published.checkpoint.snapshotId,
    );
    noReplay(item.publicationRecovered);

    item.raceBase = await prepare(source(fixture.base), "races");
    exact(item.raceBase, fixture.base);
    due(item.raceBase.result.baseline.checkpoint.snapshotId);
    const captured = Promise.withResolvers();
    const held = Promise.withResolvers();
    releaseCheckpoint = held.resolve;
    checkpointHook = async (receipt) => {
      if (!receipt.input.captureId.startsWith("refresh:")) return;
      checkpointHook = undefined;
      item.staleRefresh = receipt;
      captured.resolve();
      await held.promise;
    };
    const refresh = call("refresh-baselines", {});
    try {
      await Promise.race([
        captured.promise,
        refresh.then(() => assert.fail("Refresh ended before capture")),
      ]);
      item.raceWinner = await prepare(source(fixture.candidate), "races");
      exact(item.raceWinner, fixture.candidate);
    } finally {
      held.resolve();
      releaseCheckpoint = undefined;
      checkpointHook = undefined;
      await refresh;
    }
    const winner = item.raceWinner.result.baseline;
    assert.equal(
      row(winner.repository).checkpoint.snapshotId,
      winner.checkpoint.snapshotId,
      "A stale expiry refresh must not overwrite the newer head",
    );
    item.stalePreparation = await interruptPublication(
      source(fixture.base),
      "races",
      "older-owner",
      false,
    );
    assert.equal(
      item.stalePreparation.retained.preparation.previousSnapshotId,
      winner.checkpoint.snapshotId,
    );
    due(winner.checkpoint.snapshotId);
    await call("refresh-baselines", {});
    item.refreshedWinner = row(winner.repository);
    assert.equal(item.refreshedWinner.checkpoint.head, fixture.candidate);
    assert.notEqual(item.refreshedWinner.checkpoint.snapshotId, winner.checkpoint.snapshotId);
    item.staleRetry = await prepare(source(fixture.base), "races", "older-owner");
    assert.equal(item.staleRetry.result.reason, "publication-conflict");
    noReplay(item.staleRetry);
    item.conflictCleanup = await call("preparation-state", {
      sessionId: item.staleRetry.input.sessionId,
      destroy: false,
    });
    assert.equal(item.conflictCleanup.description.current, null);
    assert.deepEqual(row(winner.repository), item.refreshedWinner);

    item.absentCapture = await interruptPublication(source(fixture.base), "absent", "older-owner");
    assert.equal(item.absentCapture.retained.preparation.previousSnapshotId, null);
    item.absentWinner = await prepare(source(fixture.candidate), "absent");
    exact(item.absentWinner, fixture.candidate);
    item.absentRetry = await prepare(source(fixture.base), "absent", "older-owner");
    assert.equal(item.absentRetry.result.reason, "publication-conflict");
    noReplay(item.absentRetry);
    assert.equal(
      row(item.absentWinner.result.baseline.repository).checkpoint.head,
      fixture.candidate,
    );
    fixture.moveMain(fixture.sources.base.repositoryName, fixture.base);
    await mf.setOptions(options);
  });
  for (let sample = 0; sample < 3; sample++) {
    const input = sample === 0 ? first : launch();
    await record(`fresh-${sample + 1}`, async () => {
      const started = performance.now();
      const result = await rpc(input);
      assert.equal(result.exit, "Success", result.failure);
      assert.equal(result.value.accepted, true);
      assert.deepEqual(result.nativeRpcs, [{ operation: "create", id: input.runId }]);
      const native = await poll(input);
      assert.equal(native.turns, 1);
      assert.ok(native.state.savedCheckpoint);
      const admissionFingerprint = pg.sql(
        `SELECT admission_fingerprint FROM repair_executions WHERE run_id = '${input.runId}'`,
      );
      assert.notEqual(admissionFingerprint, native.state.fingerprint);
      assert.equal(
        admissionFingerprint,
        createHash("sha256")
          .update(JSON.stringify(["fixture-host", native.state.fingerprint]))
          .digest("hex"),
      );
      assert.equal(
        native.state.request.source.artifactsRepositoryId,
        fixture.sources.base.repositoryId,
      );
      if (sample === 0) saved = native;
      else assert.equal((await rpc(input, "retire")).exit, "Success");
      return {
        ...result,
        nativeElapsedMillis: Math.round(performance.now() - started),
        nativeReceipt: native.state.launchReceipt,
        savedCheckpoint: native.state.savedCheckpoint,
      };
    });
  }
  await record("duplicate-after-reload", async () => {
    await mf.setOptions(options);
    const result = await rpc(first);
    assert.equal(result.exit, "Success", result.failure);
    const native = await poll(first);
    assert.deepEqual(native.state.launchReceipt, saved.state.launchReceipt);
    assert.deepEqual(native.state.request, saved.state.request);
    assert.equal(native.turns, 1);
    const conflict = await rpc(first, "start", "changed-create-fingerprint");
    assert.equal(conflict.exit, "Failure");
    assert.match(conflict.failure, /The repair launch receipt conflicts/);
    return result;
  });
  await record("ambiguous-create-reply", async () => {
    const input = launch();
    const failed = await rpc(input, "start", "drop-create-reply");
    assert.equal(failed.exit, "Failure");
    const completed = await poll(input);
    const projection = JSON.parse(
      pg.sql(
        `SELECT json_build_object('fingerprint', admission_fingerprint, 'outcome', state::json->>'outcome') FROM repair_executions WHERE run_id = '${input.runId}'`,
      ),
    );
    assert.deepEqual(
      projection,
      { fingerprint: null, outcome: "pending" },
      "Native coding must finish without Postgres launch confirmation",
    );
    assert.equal(
      pg.sql(
        `SELECT result IS NULL FROM repair_execution_receipts WHERE run_id = '${input.runId}'`,
      ),
      "t",
    );
    await mf.setOptions(options);
    const result = await rpc(input);
    assert.equal(result.exit, "Success", result.failure);
    const native = await poll(input);
    assert.deepEqual(native.state.launchReceipt, completed.state.launchReceipt);
    assert.deepEqual(native.state.request, completed.state.request);
    assert.equal(native.turns, 1);
    assert.equal((await rpc(input, "retire")).exit, "Success");
    return {
      ...result,
      interrupted: failed,
      nativeReceipt: native.state.launchReceipt,
      projectionBeforeRetry: projection,
    };
  });
  await record("interrupted-before-native", async () => {
    const input = launch();
    input.origin.proofPolicy = { second: 2, first: 1 };
    input.launch.proofPolicy = { second: 2, first: 1 };
    const failed = await rpc(input, "start", "interrupt-before-create");
    assert.equal(failed.exit, "Failure");
    const intent = pg.sql(
      `SELECT intent FROM repair_execution_receipts WHERE run_id = '${input.runId}'`,
    );
    assert.ok(intent);
    assert.equal((await rpc(input, "evidence")).state, null);
    assert.deepEqual((await rpc(input, "events")).value, []);
    const pendingDetail = await rpc(input, "detail");
    assert.equal(pendingDetail._tag, "Success");
    assert.equal(pendingDetail.value, null);
    const pendingInspection = await rpc(input, "inspect");
    assert.equal(pendingInspection.exit, "Success", pendingInspection.failure);
    assert.equal(pendingInspection.value.outcome, "pending");
    assert.equal(pendingInspection.value.candidateSha, null);
    const pendingReport = await rpc(input, "report");
    assert.equal(pendingReport.exit, "Success", pendingReport.failure);
    assert.equal(pendingReport.value.outcome, "pending");
    assert.equal(pendingReport.value.message, null);
    await mf.setOptions(options);
    input.origin.proofPolicy = { first: 1, second: 2 };
    input.launch.proofPolicy = { first: 1, second: 2 };
    const result = await rpc(input);
    assert.equal(result.exit, "Success", result.failure);
    const native = await poll(input);
    assert.deepEqual(native.state.request, JSON.parse(intent));
    assert.equal(native.turns, 1);
    assert.equal((await rpc(input, "retire")).exit, "Success");
    return { ...result, interrupted: failed, pendingDetail, pendingInspection, pendingReport };
  });
  await record("revoked-authority", async () => {
    const input = launch();
    pg.sql(`UPDATE fixture_authority SET allowed = false WHERE run_id = '${input.runId}'`);
    const operations = fixture.operations.length;
    const result = await rpc(input);
    assert.equal(result.exit, "Failure");
    assert.match(result.failure, /RepairConflict: Repair execution authority is unavailable/);
    assert.equal((await rpc(input, "evidence")).state, null);
    assert.deepEqual((await rpc(input, "events")).value, []);
    try {
      await rpc(input, "break-events");
      assert.equal(
        (await rpc(input, "events"))._tag,
        "RepairUnavailable",
        "A real SQLite read failure must not become an empty event stream",
      );
      assert.equal(
        (await rpc(input, "snapshot"))._tag,
        "RepairUnavailable",
        "A real SQLite read failure must not become an empty snapshot",
      );
      assert.equal(
        (await rpc(input, "detail"))._tag,
        "RepairUnavailable",
        "A real SQLite read failure must not become an absent native detail",
      );
    } finally {
      await rpc(input, "restore-events");
    }
    assert.deepEqual((await rpc(input, "events")).value, []);
    assert.equal(
      fixture.operations.length,
      operations,
      "Revoked authority must not reach source operations",
    );
    return result;
  });
  await record("expired-deadline", async () => {
    const input = launch();
    input.origin.deadlineAtMs = Date.now() - 1;
    const operations = fixture.operations.length;
    const result = await rpc(input);
    assert.equal(result.exit, "Failure");
    assert.match(result.failure, /RepairConflict: Repair execution authority is unavailable/);
    assert.equal((await rpc(input, "evidence")).state, null);
    assert.equal(fixture.operations.length, operations);
    return result;
  });
  await record("revoked-during-source", async () => {
    const input = launch();
    revokeOnSource = input.runId;
    const result = await rpc(input);
    assert.equal(result.exit, "Failure");
    assert.match(result.failure, /RepairConflict: Repair execution authority is unavailable/);
    assert.equal((await rpc(input, "evidence")).state, null);
    assert.ok(
      pg.sql(`SELECT intent FROM repair_execution_receipts WHERE run_id = '${input.runId}'`),
    );
    assert.equal((await rpc(input, "retire")).exit, "Success");
    const cancelled = await rpc(input, "evidence");
    assert.equal(cancelled.state.cancelled, true);
    assert.equal(cancelled.state.launchReceipt, null);
    const response = await mf.dispatchFetch("http://localhost/native", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(cancelled.state.request),
    });
    assert.equal((await response.json()).value.result.outcome, "cancelled");
    assert.equal((await rpc(input, "evidence")).pipeline, null);
    return result;
  });
  await record("launch-read-overlap", async (item) => {
    const input = launch();
    const result = await rpc(input, "start", "slow-authorization");
    Object.assign(item, result);
    assert.equal(result.exit, "Success", result.failure);
    const authority = result.spans.find((span) => span.query?.includes("SELECT allowed"));
    const previous = result.spans.find((span) =>
      span.query?.startsWith("SELECT intent FROM repair_execution_receipts"),
    );
    assert.ok(authority && previous);
    assert.ok(
      authority.startedAtMillis < previous.endedAtMillis &&
        previous.endedAtMillis < authority.endedAtMillis,
      "The saved launch read must finish while the real Postgres permission query is still pending",
    );
    const native = await poll(input);
    assert.equal(native.turns, 1);
    assert.equal((await rpc(input, "retire")).exit, "Success");
    return result;
  });
  await record("launch-retention-rollback", async (item) => {
    const results = (item.results = []);
    for (const malformed of [false, true]) {
      const input = launch();
      const change = { runId: input.runId, malformed, original: undefined };
      invalidateOnSource = change;
      try {
        const result = await rpc(input);
        results.push(result);
        assert.equal(result.exit, "Failure");
        assert.match(
          result.failure,
          malformed ? /SchemaError/ : /Repair admission authority expired/,
        );
        assert.deepEqual(result.nativeRpcs, []);
        assert.ok(result.statements.includes("ROLLBACK"));
        assert.equal(
          pg.sql(`SELECT count(*) FROM repair_execution_receipts WHERE run_id = '${input.runId}'`),
          "0",
          "Invalid execution state must roll back the uncommitted launch",
        );
      } finally {
        invalidateOnSource = undefined;
        if (change.original !== undefined)
          pg.sql(
            `UPDATE repair_executions SET state = ${quote(change.original)} WHERE run_id = '${input.runId}'`,
          );
      }
    }
    return { results };
  });
  await record("concurrent-duplicate-delivery", async () => {
    const input = launch();
    const results = await Promise.all([rpc(input), rpc(input)]);
    for (const result of results) assert.equal(result.exit, "Success", result.failure);
    const native = await poll(input);
    assert.equal(native.turns, 1);
    assert.equal(
      pg.sql(`SELECT count(*) FROM repair_execution_receipts WHERE run_id = '${input.runId}'`),
      "1",
    );
    assert.equal((await rpc(input, "retire")).exit, "Success");
    return { deliveries: results, nativeReceipt: native.state.launchReceipt };
  });
  await record("immutable-launch-and-admission", async () => {
    const result = await rpc({
      ...first,
      launch: { ...first.launch, prompt: "Changed immutable prompt" },
    });
    assert.equal(result.exit, "Failure");
    for (const changed of [
      { ...saved.state.request, commandId: randomUUID() },
      {
        ...saved.state.request,
        source: { ...saved.state.request.source, artifactsRepositoryId: "changed-generation" },
      },
    ]) {
      const response = await mf.dispatchFetch("http://localhost/native", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(changed),
      });
      assert.equal((await response.json())._tag, "RepairConflict");
    }
    assert.equal((await poll(first)).turns, 1);
    return result;
  });
  await record("saved-checkpoint-continuation", async () => {
    assert.equal((await rpc(first, "reconcile")).value.outcome, "completed");
    const candidate = saved.state.savedCheckpoint.commitSha;
    const publication = JSON.stringify({
      id: randomUUID(),
      kind: "repair",
      sourceHead: candidate,
      contentHash: "c".repeat(64),
      branch: "fixture-repair",
      number: 1,
      url: "https://github.com/fixture/project/pull/1",
    });
    pg.sql(
      `INSERT INTO repair_publication(run_id, source_head, kind, receipt) VALUES ('${first.runId}', '${candidate}', 'repair', ${quote(publication)})`,
    );
    const input = launch();
    input.origin.incidentId = first.origin.incidentId;
    input.launch.continuation = { runId: first.runId, candidateSha: candidate, publication: null };
    const prior = saved.state.savedCheckpoint;
    const rejectSource = async () => {
      const operations = fixture.operations.length;
      const rejected = await rpc(input);
      assert.equal(rejected.exit, "Failure");
      assert.equal((await rpc(input, "evidence")).state, null);
      assert.equal(
        fixture.operations.slice(operations).filter((entry) => entry.operation === "create").length,
        0,
      );
      return rejected;
    };
    const rejectedSources = [];
    try {
      fixture.generation(prior.repositoryName, "replacement-generation");
      rejectedSources.push(await rejectSource());
    } finally {
      fixture.generation(prior.repositoryName, prior.artifactsRepositoryId);
    }
    try {
      execute("git", [
        "-C",
        join(directory, "repositories", `${prior.repositoryName}.git`),
        "update-ref",
        "refs/heads/main",
        fixture.base,
      ]);
      rejectedSources.push(await rejectSource());
    } finally {
      execute("git", [
        "-C",
        join(directory, "repositories", `${prior.repositoryName}.git`),
        "update-ref",
        "refs/heads/main",
        candidate,
      ]);
    }
    try {
      hiddenRepository = prior.repositoryName;
      rejectedSources.push(await rejectSource());
    } finally {
      hiddenRepository = undefined;
    }
    const result = await rpc(input);
    assert.equal(result.exit, "Success", result.failure);
    const native = await poll(input);
    assert.equal(
      native.state.request.source.repositoryName,
      saved.state.savedCheckpoint.repositoryName,
    );
    assert.equal(
      native.state.request.source.artifactsRepositoryId,
      saved.state.savedCheckpoint.artifactsRepositoryId,
    );
    assert.equal(native.state.request.source.commitSha, candidate);
    assert.equal(native.state.savedCheckpoint.baseHead, candidate);
    assert.notEqual(
      native.state.savedCheckpoint.repositoryName,
      saved.state.savedCheckpoint.repositoryName,
    );
    assert.equal(
      pg.sql(`SELECT receipt FROM repair_publication WHERE run_id = '${first.runId}'`),
      publication,
    );
    assert.equal((await rpc(input, "retire")).exit, "Success");
    return { ...result, savedCheckpoint: native.state.savedCheckpoint, rejectedSources };
  });
  await record("fresh-seed-and-concurrent-creator", async () => {
    const results = [];
    for (const race of [false, true]) {
      const input = launch();
      input.launch.seedRepository = race ? "concurrent-seed" : "new-seed";
      if (race) concurrentSeed = input.launch.seedRepository;
      const result = await rpc(input, "source");
      assert.equal(result.exit, "Success", result.failure);
      assert.equal(result.value.source.repositoryName, input.launch.seedRepository);
      assert.equal(result.value.source.commitSha, fixture.base);
      assert.deepEqual(result.value.missing, [fixture.base]);
      assert.equal((await rpc(input, "evidence")).state, null);
      results.push(result);
    }
    return { sources: results };
  });
  await record("unconfirmed-native-failure", async (item) => {
    const source = await fixture.failedSetup();
    fixture.pin(source.repositoryName, source.commitSha);
    const input = launch();
    input.origin.offendingSha = source.commitSha;
    input.origin.targetSha = source.commitSha;
    input.origin.deadlineAtMs = Date.now() + 90_000;
    input.launch.targetSha = source.commitSha;
    input.launch.seedRepository = source.repositoryName;
    item.interrupted = await rpc(input, "start", "drop-create-reply");
    assert.equal(item.interrupted.exit, "Failure");
    item.native = await poll(input, "failed");
    assert.equal(item.native.codexTurnId, null);
    assert.equal(item.native.setupReport.state, "failed");
    assert.ok(item.native.setupReport.steps.some((step) => step.exitCode === 23));
    assert.equal(
      pg.sql(
        `SELECT admission_fingerprint IS NULL FROM repair_executions WHERE run_id = '${input.runId}'`,
      ),
      "t",
    );
    pg.sql(`UPDATE fixture_authority SET allowed = false WHERE run_id = '${input.runId}'`);
    item.revoked = {
      inspect: await rpc(input, "inspect"),
      report: await rpc(input, "report"),
    };
    item.revokedRetry = await rpc(input);
    pg.sql(`UPDATE fixture_authority SET allowed = true WHERE run_id = '${input.runId}'`);
    await setTimeout(Math.max(0, input.origin.deadlineAtMs - Date.now() + 20));
    item.expired = {
      inspect: await rpc(input, "inspect"),
      report: await rpc(input, "report"),
    };
    item.expiredRetry = await rpc(input);
    item.nativeAfterExpiry = await rpc(input, "evidence");
    for (const observations of [item.revoked, item.expired]) {
      for (const result of [observations.inspect, observations.report]) {
        assert.equal(result.exit, "Success", result.failure);
        assert.equal(result.value.outcome, "failed");
      }
      assert.equal(observations.report.value.message, null);
    }
    for (const retry of [item.revokedRetry, item.expiredRetry]) {
      assert.equal(retry.exit, "Failure");
      assert.match(retry.failure, /Repair execution authority is unavailable/);
      assert.deepEqual(retry.nativeRpcs, []);
    }
    assert.equal(item.nativeAfterExpiry.state.status, "failed");
    assert.deepEqual(item.nativeAfterExpiry.state.launchReceipt, item.native.state.launchReceipt);
    assert.equal(
      nativeRpcs.filter((entry) => entry.id === input.runId).length,
      1,
      "Observing a terminal failure must not replay native create",
    );
  });
  await record("unconfirmed-retired-continuation", async (item) => {
    const input = launch();
    item.interrupted = await rpc(input, "start", "drop-create-reply");
    assert.equal(item.interrupted.exit, "Failure");
    item.completed = await poll(input);
    assert.equal(item.completed.turns, 1);
    item.inspection = await rpc(input, "inspect");
    item.report = await rpc(input, "report");
    assert.equal(item.inspection.value.outcome, "completed");
    assert.equal(item.report.value.outcome, "completed");
    item.retirement = await rpc(input, "retire-completed");
    assert.equal(item.retirement.exit, "Success", item.retirement.failure);
    assert.equal(item.retirement.value.retirement, "retired");
    assert.equal(item.retirement.value.outcome, "completed");
    assert.equal(item.retirement.value.stopReason, null);
    assert.equal(
      pg.sql(
        `SELECT admission_fingerprint IS NULL FROM repair_executions WHERE run_id = '${input.runId}'`,
      ),
      "t",
    );
    item.retired = await rpc(input, "evidence");
    assert.deepEqual(item.retired.state.savedCheckpoint, item.completed.state.savedCheckpoint);
    assert.deepEqual(item.retired.state.launchReceipt, item.completed.state.launchReceipt);
    const prior = item.completed.state.savedCheckpoint;
    const continuation = launch();
    continuation.origin.incidentId = input.origin.incidentId;
    continuation.launch.continuation = {
      runId: input.runId,
      candidateSha: prior.commitSha,
      publication: null,
    };
    const operations = fixture.operations.length;
    item.missingFingerprint = await rpc(continuation, "start", "omit-inspect-fingerprint");
    assert.equal(item.missingFingerprint.exit, "Failure");
    assert.match(
      item.missingFingerprint.failure,
      /The continuation private checkpoint does not match/,
    );
    assert.deepEqual(item.missingFingerprint.nativeRpcs, []);
    assert.equal(fixture.operations.length, operations);
    assert.equal(
      pg.sql(
        `SELECT admission_fingerprint IS NULL FROM repair_executions WHERE run_id = '${input.runId}'`,
      ),
      "t",
    );
    try {
      pg.sql(
        `UPDATE repair_executions SET admission_fingerprint = '${"d".repeat(64)}' WHERE run_id = '${input.runId}'`,
      );
      item.changedFingerprint = await rpc(continuation);
    } finally {
      pg.sql(
        `UPDATE repair_executions SET admission_fingerprint = NULL WHERE run_id = '${input.runId}'`,
      );
    }
    item.sourceOperationsWhileChanged = fixture.operations.length - operations;
    item.continuation = await rpc(continuation);
    assert.equal(item.continuation.exit, "Success", item.continuation.failure);
    assert.equal(item.changedFingerprint.exit, "Failure");
    assert.match(
      item.changedFingerprint.failure,
      /The continuation private checkpoint does not match/,
    );
    assert.deepEqual(item.changedFingerprint.nativeRpcs, []);
    assert.equal(item.sourceOperationsWhileChanged, 0);
    const continued = await poll(continuation);
    assert.equal(continued.turns, 1);
    assert.equal(continued.state.request.source.repositoryName, prior.repositoryName);
    assert.equal(continued.state.request.source.artifactsRepositoryId, prior.artifactsRepositoryId);
    assert.equal(continued.state.request.source.commitSha, prior.commitSha);
    assert.equal(continued.state.savedCheckpoint.baseHead, prior.commitSha);
    item.savedCheckpoint = continued.state.savedCheckpoint;
    item.retiredDetail = await rpc(input, "detail");
    assert.equal(item.retiredDetail._tag, "Success");
    assert.equal(item.retiredDetail.value.requestFingerprint, item.retired.state.fingerprint);
    assert.equal(
      pg.sql(
        `SELECT admission_fingerprint IS NULL FROM repair_executions WHERE run_id = '${input.runId}'`,
      ),
      "f",
    );
    item.confirmedLegacySource = await rpc(continuation, "source", "omit-inspect-fingerprint");
    assert.equal(item.confirmedLegacySource.exit, "Success", item.confirmedLegacySource.failure);
    assert.equal(item.confirmedLegacySource.value.source.repositoryName, prior.repositoryName);
    assert.equal(
      item.confirmedLegacySource.value.source.artifactsRepositoryId,
      prior.artifactsRepositoryId,
    );
    assert.equal(item.confirmedLegacySource.value.source.commitSha, prior.commitSha);
    item.retiredRetry = await rpc(input);
    assert.equal(item.retiredRetry.exit, "Failure");
    assert.match(item.retiredRetry.failure, /Repair execution authority is unavailable/);
    assert.deepEqual(item.retiredRetry.nativeRpcs, []);
    assert.equal(nativeRpcs.filter((entry) => entry.id === input.runId).length, 1);
    assert.equal((await rpc(continuation, "retire")).exit, "Success");
  });
  assert.ok(evidence.cases.length > 0, `Unknown native case: ${selectedCase}`);
} catch (error) {
  failures.push(error);
} finally {
  releaseCheckpoint?.();
  if (mf) {
    for (const input of launches.values()) {
      try {
        const native = await rpc(input, "evidence");
        if (native.checkpoint) snapshots.add(native.checkpoint.snapshotId);
        const result = await rpc(input, "retire");
        assert.equal(result.exit, "Success", result.failure);
      } catch (error) {
        failures.push(error);
      }
    }
    try {
      await mf.dispose();
    } catch (error) {
      failures.push(error);
    }
  }
  if (pg)
    try {
      await pg.close();
    } catch (error) {
      failures.push(error);
    }
  try {
    const containers = docker(
      "ps",
      "--all",
      "--filter",
      `name=^/workerd-${runName}-`,
      "--format",
      "{{.ID}}",
    )
      .split("\n")
      .filter(Boolean);
    if (containers.length) docker("rm", "--force", ...containers);
    for (const snapshot of snapshots) {
      assert.match(snapshot, /^[0-9a-f-]{36}$/);
      docker("image", "rm", `workerd-container-snap-${snapshot}:latest`);
    }
    await rm(join(directory, "state"), { recursive: true, force: true });
    await rm(bundle, { recursive: true, force: true });
  } catch (error) {
    failures.push(error);
  }
  evidence.failures = failures.map((error) => String(error.stack ?? error));
  evidence.passed = failures.length === 0;
  await save();
}
if (failures.length)
  throw new AggregateError(failures, `Native launch failed; evidence: ${directory}`);
console.log(
  "NATIVE_LAUNCH_PASS: native alarm recovery and local Postgres launch semantics; owned resources removed",
);
