import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { existsSync } from "node:fs";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Miniflare, Log, LogLevel, convertV4MiniflareOptions } from "miniflare";
import { build } from "vite-plus/pack";
import { gitArtifacts } from "../../../../test/fixtures/git-artifacts.mjs";

const [image, expected = "1", suite = "all"] = process.argv.slice(process.argv[2] === "--" ? 3 : 2);
assert.ok(
  image,
  "Usage: vp run -F @yielded/sandbox test:e2e:git -- <local image> [expected restore fetches: 1 or 2] [all|warm|inspect|fork]",
);
assert.ok(["1", "2"].includes(expected));
assert.ok(["all", "warm", "inspect", "fork"].includes(suite));
assert.equal(process.env.MINIFLARE_WORKERD_PATH, undefined, "Use the pinned workerd binary");
const root = fileURLToPath(new URL("../../", import.meta.url));
const execute = (file, args) =>
  execFileSync(file, args, { cwd: root, encoding: "utf8", timeout: 30_000 }).trim();
const dockerHost =
  process.env.DOCKER_HOST ??
  execute("docker", ["context", "inspect", "--format", "{{.Endpoints.docker.Host}}"]);
assert.ok(dockerHost.startsWith("unix://"), "Use a local Docker Unix socket");
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
const directory = await mkdtemp(join(tmpdir(), "repairs-git-history-"));
const bundle = join(directory, "bundle");
const runName = `repairs-git-${randomUUID()}`;
const require = createRequire(import.meta.url);
const mfPackage = require("miniflare/package.json");
const sourceRepository = existsSync(join(root, "../../.git"));
const pipelineEntry = import.meta.resolve("@yielded/sandbox/cloudflare/MessagePipeline");
const checkoutEntry = import.meta.resolve("@yielded/sandbox/cloudflare/ArtifactsCheckout");
const evidence = {
  passed: false,
  scope:
    "Native workerd/Docker pipeline and real Git HTTP; synthetic Artifacts metadata and no model or hosted provider. Client wall times include fixture setup and assertions; shared Docker timings do not establish isolated or hosted speedup.",
  head: sourceRepository ? execute("git", ["rev-parse", "HEAD"]) : null,
  dirty: sourceRepository ? execute("git", ["status", "--porcelain"]) !== "" : null,
  pipelineEntry,
  checkoutEntry,
  pipelineSha256: createHash("sha256")
    .update(await readFile(new URL(pipelineEntry)))
    .digest("hex"),
  checkoutSha256: createHash("sha256")
    .update(await readFile(new URL(checkoutEntry)))
    .digest("hex"),
  node: process.version,
  miniflare: mfPackage.version,
  workerd: mfPackage.dependencies.workerd,
  tracing: "CloudflareTracer.layer with native context and sampled span delegation",
  sandbox: require("@yielded/sandbox/package.json").version,
  effect: require("effect/package.json").version,
  effectCf: require("effect-cf/package.json").version,
  imageId,
  proxyId,
  expectedRestoreFetches: Number(expected),
  suite,
  cases: [],
};
console.log(`Native Git evidence: ${directory}`);
const fixture = await gitArtifacts(directory);
await build({
  cwd: root,
  config: false,
  entry: { "git-history-worker": "test/e2e/git-history.worker.ts" },
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
const events = [];
let logDone;
let forkFault;
let modelResult = Promise.withResolvers();
let forkFinished = Promise.withResolvers();
const configuration = {
  name: runName,
  scriptPath: join(bundle, "git-history-worker.mjs"),
  modulesRoot: bundle,
  modules: true,
  compatibilityDate: "2026-09-30",
  compatibilityFlags: ["nodejs_compat"],
  unsafeObservability: true,
  cf: false,
  telemetry: { enabled: false },
  log: new Log(LogLevel.WARN),
  handleStructuredLogs({ message, level }) {
    if (message.startsWith("GIT_HISTORY_EVENT ")) {
      const event = JSON.parse(message.slice("GIT_HISTORY_EVENT ".length));
      events.push(event);
      if (event.kind === "harness-result") modelResult.resolve();
      if (event.kind === "span" && event.name === "fork") forkFinished.resolve();
    } else if (message === "GIT_HISTORY_DONE") logDone?.resolve();
    else if (level === "error" || level === "warn") console.error(message);
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
    GIT_HISTORY: { className: "GitHistory", useSQLite: true },
  },
  bindings: { SANDBOX_MAX_INSTANCES: "4" },
  serviceBindings: {
    FIXTURE_ARTIFACTS: async (request) => {
      if (new URL(request.url).pathname === "/before-model") {
        await forkFinished.promise;
        return new Response(null, { status: 204 });
      }
      const { operation, name, args } = await request.clone().json();
      if (forkFault === "failed" && operation === "fork")
        return new Response(null, { status: 503 });
      const response = await fixture.control(request);
      if (
        operation === "log" &&
        ((forkFault === "replaced-pin" && args[0]?.ref !== "main") ||
          (forkFault === "replaced-main" && args[0]?.ref === "main"))
      ) {
        fixture.generation(name, "replacement-generation");
        forkFault = undefined;
      }
      if ((forkFault === "pending" || forkFault === "lost") && operation === "fork") {
        await modelResult.promise;
        if (forkFault === "lost") return new Response(null, { status: 503 });
      }
      return response;
    },
  },
  outboundService: fixture.transport,
  durableObjectsPersist: join(directory, "state"),
};
const mf = new Miniflare(convertV4MiniflareOptions(configuration));
const names = new Set();
const snapshots = new Set();
const failures = [];
const save = () => writeFile(join(directory, "results.json"), JSON.stringify(evidence, null, 2));
const rpc = async (input) => {
  logDone = Promise.withResolvers();
  const response = await mf.dispatchFetch("http://localhost/", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(input),
    signal: AbortSignal.timeout(120_000),
  });
  const body = await response.text();
  assert.equal(response.status, 200, body);
  await logDone.promise;
  return JSON.parse(body);
};
const release = async (name) => {
  const result = await rpc({
    name,
    mode: "destroy",
    source: fixture.sources.base,
    requiredCommits: [],
  });
  if (result.snapshotId !== null) snapshots.add(result.snapshotId);
  names.delete(name);
};
async function run(
  name,
  mode,
  source,
  baseline,
  requiredCommits = [fixture.base, source.commitSha],
  extra = {},
) {
  names.add(name);
  const offsets = {
    operations: fixture.operations.length,
    transfers: fixture.transfers.length,
    events: events.length,
  };
  const start = performance.now();
  const record = { name, mode, completed: false };
  evidence.cases.push(record);
  try {
    Object.assign(
      record,
      await rpc({
        name,
        mode,
        source,
        requiredCommits,
        ...(baseline === undefined ? {} : { baseline }),
        ...extra,
      }),
    );
    record.completed = true;
    if (record.baseline) snapshots.add(record.baseline.checkpoint.snapshotId);
    return record;
  } catch (error) {
    record.failure = String(error);
    throw error;
  } finally {
    record.clientWallMillis = Math.round(performance.now() - start);
    record.operations = fixture.operations.slice(offsets.operations);
    record.transfers = fixture.transfers.slice(offsets.transfers);
    record.remoteFetches = record.transfers.filter(
      (entry) => entry.operation === "info/refs" && entry.service === "git-upload-pack",
    ).length;
    record.events = events.slice(offsets.events);
    const boot = record.events.find(
      (event) => event.kind === "span" && event.name === "container-boot+marker",
    );
    const harness = record.events.find((event) => event.kind === "harness");
    if (boot && harness) {
      const startup = record.events.filter(
        (event) => event.at >= boot.end && event.at < harness.at,
      );
      const counts = (kind) =>
        startup
          .filter((event) => event.kind === kind)
          .reduce((counts, event) => {
            counts[event.name] = (counts[event.name] ?? 0) + 1;
            return counts;
          }, {});
      record.postRestore = {
        millis: harness.at - boot.end,
        rpc: counts("rpc"),
        spans: counts("span"),
      };
    }
    await save();
    console.log(
      JSON.stringify({
        name,
        mode,
        completed: record.completed,
        failure: record.pipelineFailure,
        remoteFetches: record.remoteFetches,
        clientWallMillis: record.clientWallMillis,
        postRestore: record.postRestore,
      }),
    );
  }
}
function ready(result, source, installation) {
  assert.equal(result.pipelineFailure, null);
  assert.equal(result.ready.head, source.commitSha);
  assert.equal(result.head, source.commitSha);
  assert.equal(result.status, "");
  assert.equal(result.installCount, 1);
  assert.equal(result.ready.readiness.installation, installation);
  assert.equal(result.persisted.head, source.commitSha);
  assert.ok(result.fork);
  assert.equal(fixture.head(result.fork.repositoryName), source.commitSha);
}
function restored(result, source, baseline, reads = Number(expected)) {
  ready(result, source, "checkpoint");
  assert.equal(result.ready.readiness.capture, "reused");
  assert.equal(result.identity.restoredSnapshotId, baseline.checkpoint.snapshotId);
  assert.deepEqual(result.baseline.checkpoint, baseline.checkpoint);
  assert.equal(result.remoteFetches, reads);
  assert.equal(result.shallow, false);
  assert.ok(result.history.includes(fixture.base));
  assert.equal(result.history.length, 7);
}

const markerReads = (result) =>
  result.events.filter(
    (event) => event.kind === "rpc" && event.name === "readFileString" && event.marker,
  ).length;

async function checkpointBoundary(baseline) {
  for (const fault of ["replace", "interrupt"]) {
    const name = `checkpoint-${fault}`;
    const result = await run(name, "fork", fixture.sources.base, baseline, [fixture.base], {
      checkpointBoundary: fault,
    });
    const boundary = result.events.find((event) => event.kind === "checkpoint-boundary");
    assert.ok(boundary);
    assert.equal(result.ready, null);
    assert.equal(result.persisted, null);
    assert.equal(result.materializationId, null);
    assert.equal(result.containerInstanceId, null);
    assert.equal(result.events.filter((event) => event.kind === "harness").length, 0);
    assert.equal(markerReads(result), 1);
    assert.equal(
      result.events.filter((event) => event.name === "writeFile" && event.marker).length,
      0,
    );
    if (fault === "replace") {
      const replacement = result.events.find((event) => event.kind === "replacement");
      assert.ok(replacement);
      assert.notEqual(replacement.instanceId, boundary.instanceId);
      assert.equal(result.pipelineDiagnostic.failures[0].code, "LEASE_LOST");
    } else {
      assert.deepEqual(result.pipelineDiagnostic.failures, []);
      assert.ok(result.pipelineDiagnostic.interruptions > 0);
      await mf.setOptions(convertV4MiniflareOptions(configuration));
      const retried = await run(name, "fork", fixture.sources.base, baseline, [fixture.base], {
        recover: true,
      });
      assert.equal(retried.pipelineFailure, null, JSON.stringify(retried.pipelineDiagnostic));
      assert.equal(retried.ready.readiness.containerReused, true);
      assert.equal(retried.containerInstanceId, boundary.instanceId);
      assert.equal(markerReads(retried), 2, "An unadopted running restore must be read again");
      assert.equal(retried.events.filter((event) => event.kind === "harness").length, 1);
      assert.deepEqual(retried.forkIntent.source, result.forkIntent.source);
      assert.equal(retried.persisted.head, fixture.base);
    }
    await release(name);
  }
}

async function inspectBoundary(name, instanceId) {
  const grant = await run(name, "outbound-grant", fixture.sources.candidate);
  assert.equal(grant.identity.instanceId, instanceId);
  assert.equal(grant.outbound.exitCode, 0);
  assert.equal(fixture.tokens.size, 1);
  await mf.setOptions(convertV4MiniflareOptions(configuration));
  const reloaded = await run(name, "outbound-reloaded", fixture.sources.candidate);
  assert.equal(reloaded.identity.instanceId, instanceId);
  assert.equal(reloaded.outbound.exitCode, 128);
  assert.equal(reloaded.transfers.at(-1)?.status, 401, "Reload must not retain repository grants");
  const regranted = await run(name, "outbound-regrant", fixture.sources.candidate);
  assert.equal(regranted.identity.instanceId, instanceId);
  assert.equal(regranted.outbound.exitCode, 0);
  assert.equal(regranted.outbound.stdout.trim(), `${fixture.candidate}\tHEAD`);
  fixture.tokens.clear();
  const fences = await run(name, "fences", fixture.sources.candidate, undefined, [], {
    divergent: fixture.divergent,
    publication: fixture.sources.publication,
  });
  const inspections = fences.fences.inspections;
  assert.equal(fences.fences.absentRecipe, null);
  assert.equal(fences.fences.missingRecipeCommit, "RepositorySetupError");
  assert.equal(inspections.staleEnsure, "SandboxOperationError");
  assert.equal(inspections.staleRead, "SandboxOperationError");
  assert.equal(inspections.staleLease, "SandboxOperationError");
  assert.equal(inspections.lostWorkspace, "SandboxOperationError");
  assert.equal(inspections.lostDescription.running, false);
  assert.equal(inspections.lostDescription.current, null);
  const diagnostics = inspections.diagnostics;
  for (const [name, type, code, reason] of [
    ["lostWorkspace", "ContainerError", undefined, "workspace-lost"],
    ["staleEnsure", "ContainerError", undefined, "instance-replaced"],
    ["staleRead", "ContainerError", undefined],
    ["staleLease", "LeaseLostError", "LEASE_LOST"],
    ["missingFile", "ContainerError", "ENOENT"],
    ["projection", "TimeoutError", "ETIMEDOUT"],
  ]) {
    const failure = diagnostics[name].failures.find((entry) => entry.kind === "failure");
    assert.equal(failure.type, "SandboxOperationError");
    assert.equal(failure.code, code);
    assert.equal(failure.causes[0]?.type, type, JSON.stringify(diagnostics[name]));
    if (reason !== undefined)
      assert.equal(failure.causes[0]?.reason, reason, JSON.stringify(diagnostics[name]));
  }
  assert.equal(diagnostics.projection.interruptions, 1);
  assert.equal(diagnostics.projection.failures.length, 2);
  assert.equal(diagnostics.projection.failures[0].kind, "defect");
  assert.equal(diagnostics.projection.failures[0].type, "TypeError");
  assert.ok(!JSON.stringify(diagnostics).includes("fixture-private-"));
  assert.equal(inspections.instanceId, instanceId);
  assert.equal(inspections.truncated.stdout.length, 4);
  assert.equal(inspections.truncated.truncated, true);
  assert.equal(inspections.missing.exitCode, 128);
  assert.ok(inspections.timeout.timedOut === true || inspections.timeout.code === "ETIMEDOUT");
  assert.equal(inspections.droppedReply, "SandboxOperationError");
  assert.equal(inspections.retried.stdout.trim(), fixture.candidate);
  assert.equal(inspections.retried.exitCode, 0);
  assert.ok(
    inspections.processes.every((process) => process.state === "exited"),
    "Inspections leave no live or unconfirmed native process groups",
  );
  assert.equal(fences.fences.generation, "ArtifactsRepositoryGenerationMismatchError");
  assert.equal(fences.fences.pin, "RepositoryForkIntentError");
  assert.equal(fences.fences.snapshot, "GitCommandError");
  assert.equal(fences.fences.ancestry, "NonFastForwardPushError");
  assert.equal(fences.fences.published.head, fixture.candidate);
  assert.equal(fixture.head(fixture.sources.publication.repositoryName), fixture.candidate);
  assert.equal(fixture.tokens.size, 0, "Every scoped repository token was revoked");
}

async function forkRegression(baseline) {
  const source = {
    ...fixture.sources.candidate,
    commitSha: fixture.base,
    artifactsRef: `refs/heads/repair-source/${fixture.base}`,
  };
  fixture.pin(source.repositoryName, source.commitSha);
  const fault = (value) => {
    forkFault = value;
    modelResult = Promise.withResolvers();
    forkFinished = Promise.withResolvers();
  };
  const harnesses = (result) => result.events.filter((event) => event.kind === "harness");
  const forks = (result) => result.operations.filter((entry) => entry.operation === "fork");
  const rejected = (result, tag) => {
    assert.equal(result.pipelineFailure, tag, JSON.stringify(result.pipelineDiagnostic));
    assert.equal(result.persisted, null);
    assert.equal(result.completionCount, 0);
    assert.equal(harnesses(result).length, 0);
  };
  const probe = (name, extra = {}, admitted = source) =>
    run(name, "fork", admitted, baseline, [fixture.base], extra);

  fault("pending");
  const pendingDeadline = Date.now() + 60_000;
  const pending = await probe("pending-fork", {
    sourcePinTimeout: true,
    deadlineAtMs: pendingDeadline,
    startedAtMillis: Date.now() - 300_001,
  });
  assert.equal(pending.pipelineFailure, null, JSON.stringify(pending.pipelineDiagnostic));
  assert.equal(pending.ready.readiness.installation, "checkpoint");
  assert.equal(pending.ready.readiness.capture, "reused");
  assert.equal(pending.persisted.head, fixture.base);
  assert.equal(pending.completionCount, 1);
  assert.equal(pending.lifecycle, null, "Ordinary turns do not need an archive Ref");
  assert.deepEqual(
    pending.dialectEvents.map(({ event }) => event.type),
    ["fixture.start", "fixture.finish"],
  );
  assert.deepEqual(pending.steering, [{ _tag: "Accepted", turnId: "pending-fork:turn" }]);
  const dispatched = harnesses(pending);
  assert.equal(dispatched.length, 1);
  assert.ok(dispatched[0].forkIntent, "Retain authority before dispatching model input");
  assert.deepEqual(dispatched[0].forkIntent.source, source);
  assert.equal(dispatched[0].forkIntent.originalMainHead, fixture.candidate);
  assert.equal(dispatched[0].forkIntent.initialized, false);
  assert.equal(pending.forkIntent.initialized, true);
  assert.equal(pending.forkIntent.deadlineAtMillis, pendingDeadline);
  assert.equal(fixture.head(pending.forkIntent.forkName), fixture.base);
  assert.equal(forks(pending).length, 1);
  assert.equal(
    pending.operations.filter(
      (entry) =>
        entry.name === source.repositoryName &&
        entry.operation === "log" &&
        entry.ref === source.artifactsRef.replace("refs/heads/", ""),
    ).length,
    3,
    "Validate the fresh source once before retention and at the two fork mutation boundaries",
  );
  await release("pending-fork");

  fault(undefined);
  const stale = await probe("stale-pin", {}, { ...source, commitSha: fixture.candidate });
  rejected(stale, "RepositoryForkIntentError");
  assert.equal(stale.forkIntent, null);
  for (const changed of ["replaced-pin", "replaced-main"]) {
    fault(changed);
    try {
      const replaced = await probe(changed);
      rejected(replaced, "ArtifactsRepositoryGenerationMismatchError");
      assert.equal(replaced.forkIntent, null);
    } finally {
      fixture.generation(source.repositoryName, source.repositoryId);
      fault(undefined);
    }
  }

  fault("failed");
  const failed = await probe("failed-fork", { waitForFork: true });
  rejected(failed, "ArtifactsOperationError");
  assert.ok(failed.forkIntent);
  assert.equal(failed.forkIntent.initialized, false);
  await release("failed-fork");

  fault("lost");
  const lost = await probe("lost-fork-reply", { api: "legacy" });
  assert.equal(lost.pipelineFailure, "ArtifactsOperationError");
  assert.equal(lost.persisted, null);
  assert.equal(lost.completionCount, 0);
  assert.equal(harnesses(lost).length, 1);
  assert.equal(forks(lost).length, 1);
  assert.ok(lost.forkIntent);
  assert.ok(lost.lifecycle.preTurn);
  assert.ok(lost.lifecycle.observed);
  assert.equal(lost.lifecycle.postTurn, null);
  fault(undefined);
  const mismatch = await probe(
    "lost-fork-reply",
    { recover: true },
    {
      ...source,
      commitSha: fixture.candidate,
    },
  );
  rejected(mismatch, "RepositoryForkIntentError");
  assert.deepEqual(mismatch.forkIntent, lost.forkIntent);
  const recovered = await probe("lost-fork-reply", { recover: true, api: "legacy" });
  assert.equal(recovered.pipelineFailure, null, JSON.stringify(recovered.pipelineDiagnostic));
  assert.equal(markerReads(recovered), 1, "Recovery must read its retained materialization");
  assert.equal(
    harnesses(recovered).length,
    0,
    "Recover the accepted result without replaying input",
  );
  assert.equal(forks(recovered).length, 0, "Reconcile the retained mutation instead of reforking");
  assert.deepEqual(recovered.forkIntent, {
    ...lost.forkIntent,
    repositoryId: recovered.forkIntent.repositoryId,
    initialized: true,
  });
  assert.equal(recovered.persisted.head, fixture.base);
  assert.equal(recovered.completionCount, 1);
  assert.equal(recovered.lifecycle.postTurn.verifiedHead, fixture.base);
  assert.equal(recovered.lifecycle.postTurn.resumeToken, "fixture");
  assert.deepEqual(recovered.dialectEvents, []);
  assert.deepEqual(recovered.steering, []);
  await release("lost-fork-reply");

  fault(undefined);
  const expired = await probe("expired-fork", { deadlineAtMs: 0, waitForFork: true });
  rejected(expired, "ForkNotReadyError");
  assert.ok(expired.forkIntent);
  assert.equal(expired.forkIntent.deadlineAtMillis, 0);
  fault(undefined);
  const expiredRetry = await probe("expired-fork", {
    recover: true,
    deadlineAtMs: Date.now() + 60_000,
    waitForFork: true,
  });
  rejected(expiredRetry, "ForkNotReadyError");
  assert.deepEqual(expiredRetry.forkIntent, expired.forkIntent);
  assert.equal(forks(expired).length + forks(expiredRetry).length, 0);
  assert.equal(fixture.tokens.size, 0, "Every scoped repository token was revoked");
  await release("expired-fork");

  const callbackFailure = await probe("legacy-callback-failure", {
    api: "legacy",
    callbackFailure: true,
  });
  assert.equal(callbackFailure.pipelineFailure, "SandboxOperationError");
  assert.deepEqual(callbackFailure.callbackCause, {
    errorPreserved: true,
    defectPreserved: true,
    interruptPreserved: true,
  });
  assert.equal(callbackFailure.lifecycle.postTurn, null);
  await release("legacy-callback-failure");

  const generation = (name, extra) => probe(name, extra, fixture.sources.publication);
  const writes = (result) =>
    result.transfers.filter((entry) => entry.operation === "git-receive-pack");
  const missing = await generation("generation-missing-handoff", {
    api: "legacy",
    generationHandoff: "missing",
  });
  rejected(missing, "Defect");
  assert.equal(missing.attemptedHead, null);
  assert.equal(missing.preparedHead, null, "An ambient port cannot authorize a legacy call");
  assert.equal(writes(missing).length, 0);
  await release("generation-missing-handoff");

  const unchanged = await generation("generation-unchanged", { generationHandoff: "unchanged" });
  assert.equal(unchanged.pipelineFailure, null, JSON.stringify(unchanged.pipelineDiagnostic));
  assert.equal(unchanged.persisted.head, fixture.base);
  assert.equal(unchanged.attemptedHead, null, "Unchanged content needs no handoff");
  assert.equal(writes(unchanged).length, 0);
  fixture.moveMain(fixture.sources.publication.repositoryName, fixture.candidate);
  const rematerialized = await probe(
    "generation-unchanged",
    { generationHandoff: "unchanged" },
    { ...fixture.sources.publication, commitSha: fixture.candidate },
  );
  assert.equal(
    rematerialized.pipelineFailure,
    null,
    JSON.stringify(rematerialized.pipelineDiagnostic),
  );
  assert.equal(rematerialized.persisted.head, fixture.candidate);
  assert.equal(rematerialized.containerInstanceId, unchanged.containerInstanceId);
  assert.notEqual(rematerialized.materializationId, unchanged.materializationId);
  assert.equal(
    harnesses(rematerialized).length,
    1,
    "A capture conflict must not abort the next turn",
  );
  assert.equal(writes(rematerialized).length, 0);
  fixture.moveMain(fixture.sources.publication.repositoryName, fixture.base);
  await release("generation-unchanged");

  const deadlineAtMs = Date.now() + 60_000;
  const stopped = await generation("generation-handoff", {
    api: "legacy",
    generationHandoff: "fail",
    deadlineAtMs,
  });
  assert.equal(stopped.pipelineFailure, "SandboxOperationError");
  assert.deepEqual(stopped.callbackCause, callbackFailure.callbackCause);
  assert.ok(stopped.attemptedHead);
  assert.equal(stopped.preparedHead, null);
  assert.equal(stopped.persisted, null);
  assert.equal(writes(stopped).length, 0, "A failed handoff must prevent the push");
  assert.equal(fixture.head(fixture.sources.publication.repositoryName), fixture.base);
  const resumed = await generation("generation-handoff", {
    recover: true,
    generationHandoff: "accept",
    deadlineAtMs: deadlineAtMs + 60_000,
  });
  assert.equal(resumed.pipelineFailure, null, JSON.stringify(resumed.pipelineDiagnostic));
  assert.equal(
    harnesses(resumed).length,
    0,
    "Recover the original result across the API migration",
  );
  assert.equal(resumed.nativeDeadline, deadlineAtMs);
  assert.equal(resumed.preparedHead, stopped.attemptedHead);
  assert.equal(resumed.persisted.head, stopped.attemptedHead);
  assert.equal(fixture.head(fixture.sources.publication.repositoryName), stopped.attemptedHead);
  assert.equal(writes(resumed).length, 1);
  assert.equal(resumed.completionCount, 1);
  assert.equal(fixture.tokens.size, 0, "Callback failures and recovery release repository tokens");
  await release("generation-handoff");
}

async function regression() {
  await mf.ready;
  const source = suite === "inspect" ? fixture.sources.candidate : fixture.sources.base;
  const full = await run("prepared-full", "prepare", source, undefined, [source.commitSha], {
    includeHistory: true,
  });
  assert.equal(full.shallow, false);
  if (suite === "fork") {
    fixture.pin(source.repositoryName, source.commitSha);
    const result = await run("prepared-full", "pinned-read", {
      ...source,
      artifactsRef: `refs/heads/repair-source/${source.commitSha}`,
    });
    assert.ok(
      result.reads.every((read) => read.attempts > 0),
      "Every injected Artifacts read fault must be reached",
    );
    const expectedRead = (
      kind,
      attempts,
      timeouts,
      generationMismatch = false,
      defectPreserved = false,
      success = false,
      operation = "readCommit",
    ) => ({
      operation,
      kind,
      attempts,
      sameCommit: true,
      deadlinePreserved: true,
      sourcePreserved: true,
      retryWithinDeadline: true,
      timeouts,
      generationMismatch,
      defectPreserved,
      success,
    });
    assert.deepEqual(result.reads, [
      expectedRead("once", 2, 0, false, false, true),
      expectedRead("always", 2, 2),
      expectedRead("expired", 1, 1),
      expectedRead("replaced-before", 1, 1, true),
      expectedRead("replaced-after", 2, 1, true),
      expectedRead("mixed", 1, 0, false, true),
      expectedRead("once", 2, 0, false, false, true, "log"),
      expectedRead("expired", 1, 1, false, false, false, "log"),
      expectedRead("replaced-after", 2, 1, true, false, false, "log"),
      expectedRead("always", 2, 2, false, false, false, "log"),
    ]);
    assert.equal(
      result.operations.some(({ operation }) =>
        ["fork", "createToken", "revokeToken", "delete"].includes(operation),
      ),
      false,
      "Pinned read recovery must not replay a mutable operation",
    );
  }
  await release("prepared-full");
  if (suite === "fork") {
    await forkRegression(full.baseline);
    return;
  }
  if (suite === "inspect") {
    const inspected = await run("inspected", "turn", source, full.baseline);
    restored(inspected, source, full.baseline, 0);
    await inspectBoundary("inspected", inspected.identity.instanceId);
    return;
  }
  const fullRestore = await run("full-restore", "turn", fixture.sources.candidate, full.baseline, [
    fixture.candidate,
  ]);
  restored(fullRestore, fixture.sources.candidate, full.baseline);
  await release("full-restore");
  for (let sample = 1; sample <= 3; sample++) {
    const result = await run(`warm-full-${sample}`, "turn", fixture.sources.base, full.baseline, [
      fixture.base,
    ]);
    ready(result, fixture.sources.base, "checkpoint");
    assert.equal(result.identity.restoredSnapshotId, full.baseline.checkpoint.snapshotId);
    assert.equal(result.remoteFetches, 0);
    assert.equal(result.shallow, false);
    assert.equal(result.history.length, 6);
    assert.ok(result.postRestore);
    assert.equal(markerReads(result), 1, "A newly validated restore needs one marker read");
    assert.equal(result.postRestore.spans["container-boot+marker"], undefined);
    await release(result.name);
  }
  await checkpointBoundary(full.baseline);
  if (suite === "warm") return;
  const prepared = await run("prepared", "prepare", fixture.sources.base);
  assert.equal(prepared.shallow, true);
  assert.equal(prepared.baseline.checkpoint.head, fixture.base);
  assert.equal(prepared.installCount, 1);
  await release("prepared");
  const cold = await run("cold", "turn", fixture.sources.candidate);
  ready(cold, fixture.sources.candidate, "executed");
  assert.equal(cold.shallow, false);
  assert.equal(cold.history.length, 7);
  assert.equal(cold.remoteFetches, 2);
  assert.equal(cold.identity.restoredSnapshotId, null);
  assert.equal(markerReads(cold), 1);
  await release("cold");

  const unchanged = await run("unchanged-head", "turn", fixture.sources.base, prepared.baseline, [
    fixture.base,
  ]);
  ready(unchanged, fixture.sources.base, "checkpoint");
  assert.equal(unchanged.head, prepared.baseline.checkpoint.head);
  assert.equal(unchanged.remoteFetches, 1);
  assert.equal(unchanged.shallow, false);
  assert.equal(unchanged.history.length, 6);
  await release("unchanged-head");

  let saved;
  for (let sample = 1; sample <= 3; sample++) {
    const result = await run(
      `changed-head-${sample}`,
      "turn",
      fixture.sources.candidate,
      prepared.baseline,
    );
    restored(result, fixture.sources.candidate, prepared.baseline);
    saved = result;
    await release(result.name);
  }
  let continued;
  for (let sample = 1; sample <= 3; sample++) {
    const result = await run(`continuation-${sample}`, "turn", saved.fork, saved.baseline);
    restored(result, saved.fork, saved.baseline);
    continued = result;
    if (sample < 3) await release(result.name);
  }
  const retry = await run("continuation-3", "turn", saved.fork, saved.baseline);
  ready(retry, saved.fork, "in-place");
  assert.equal(retry.identity.instanceId, continued.identity.instanceId);
  assert.equal(retry.remoteFetches, 0);
  assert.equal(markerReads(retry), 2, "An adopted running restore keeps both marker reads");

  const unrequiredBase = await run(
    "unrequired-base",
    "turn",
    fixture.sources.candidate,
    prepared.baseline,
    [fixture.candidate],
  );
  restored(unrequiredBase, fixture.sources.candidate, prepared.baseline, 2);
  await release("unrequired-base");
  const noHistory = await run(
    "no-history",
    "turn",
    fixture.sources.candidate,
    prepared.baseline,
    [],
  );
  ready(noHistory, fixture.sources.candidate, "checkpoint");
  assert.equal(noHistory.shallow, true);
  assert.equal(noHistory.remoteFetches, 1);
  await release("no-history");

  const incompatible = await run(
    "changed-input",
    "turn",
    fixture.sources.changed,
    prepared.baseline,
  );
  ready(incompatible, fixture.sources.changed, "executed");
  assert.equal(incompatible.identity.restoredSnapshotId, null);
  assert.notEqual(
    incompatible.baseline.checkpoint.snapshotId,
    prepared.baseline.checkpoint.snapshotId,
  );
  assert.equal(incompatible.history.length, 8);
  assert.equal(markerReads(incompatible), 2, "A rejected restore cannot reuse its marker");
  await release("changed-input");

  await inspectBoundary("continuation-3", continued.identity.instanceId);
}

try {
  await regression();
} catch (error) {
  failures.push(error);
} finally {
  for (const name of names) {
    try {
      await release(name);
    } catch (error) {
      failures.push(error);
    }
  }
  try {
    await mf.dispose();
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
  evidence.failures = failures.map(String);
  evidence.passed = failures.length === 0;
  await save();
}
if (failures.length)
  throw new AggregateError(failures, `Native Git regression failed; evidence: ${directory}`);
console.log(
  `NATIVE_GIT_HISTORY_PASS: ${evidence.cases.length} cases; owned containers and snapshots removed`,
);
