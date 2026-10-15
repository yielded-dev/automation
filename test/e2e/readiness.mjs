import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { Miniflare, Log, LogLevel, convertV4MiniflareOptions } from "miniflare";
import { build } from "vite-plus/pack";

const [image, legacyRepository, ...extra] = process.argv.slice(process.argv[2] === "--" ? 3 : 2);
assert.ok(
  image && extra.length === 0,
  "Usage: vp run test:e2e -- <local image built from packages/sandbox/container/Dockerfile> [local legacy repository]",
);
assert.equal(process.env.MINIFLARE_WORKERD_PATH, undefined, "Use the pinned workerd binary");
const root = fileURLToPath(new URL("../../", import.meta.url));
const execute = (file, args) =>
  execFileSync(file, args, { cwd: root, encoding: "utf8", timeout: 30_000 }).trim();
const dockerHost =
  process.env.DOCKER_HOST ??
  execute("docker", ["context", "inspect", "--format", "{{.Endpoints.docker.Host}}"]);
assert.ok(dockerHost.startsWith("unix://"), "This fixture requires a local Docker Unix socket");
const docker = (...args) => execute("docker", ["--host", dockerHost, ...args]);
const imageId = docker("image", "inspect", image, "--format", "{{.Id}}");
assert.equal(
  docker("image", "inspect", imageId, "--format", "{{.Os}}/{{.Architecture}}"),
  "linux/amd64",
);
// This is Miniflare's pinned native egress image. Inspect before starting so a
// missing prerequisite fails locally instead of causing an implicit registry pull.
const proxyImage =
  "cloudflare/proxy-everything:3cb1195@sha256:0ef6716c52430096900b150d84a3302057d6cd2319dae7987128c85d0733e3c8";
const proxyId = docker("image", "inspect", proxyImage, "--format", "{{.Id}}");
const directory = await mkdtemp(join(tmpdir(), "repairs-readiness-"));
const bundle = join(directory, "bundle");
const previousPin = "b30515a45c290d190eb257d63c9d5de7138c06b3";
let previous;
if (legacyRepository) {
  const cache = join(root, ".vite");
  await mkdir(cache, { recursive: true });
  previous = await mkdtemp(join(cache, "readiness-previous-"));
  execFileSync("tar", ["-xf", "-", "-C", previous], {
    input: execFileSync("git", ["archive", previousPin, "dist"], {
      cwd: resolve(legacyRepository),
      maxBuffer: 16_000_000,
    }),
  });
}
const runName = `repairs-readiness-${randomUUID()}`;
const miniflarePackage = createRequire(import.meta.url)("miniflare/package.json");
const evidence = {
  passed: false,
  scope:
    "Synthetic native workerd/Docker preparation and HTTP readiness; no Artifacts, model turn, publication, or hosted speedup proof. Client time includes fixture setup and assertions. Spans overlap; do not sum them.",
  head: execute("git", ["rev-parse", "HEAD"]),
  dirty: execute("git", ["status", "--porcelain"]) !== "",
  node: process.version,
  miniflare: miniflarePackage.version,
  workerd: miniflarePackage.dependencies.workerd,
  imageId,
  proxyId,
  previousPin: legacyRepository ? previousPin : null,
  cases: [],
};
console.log(`Native readiness evidence: ${directory}`);
const buildOptions = {
  cwd: root,
  config: false,
  entry: { "readiness-worker": "test/e2e/readiness.worker.ts" },
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
};
await build(buildOptions);
const previousBundle = join(directory, "previous-bundle");
if (previous) {
  await build({
    ...buildOptions,
    outDir: previousBundle,
    plugins: [
      {
        name: "published-readiness-runtime",
        resolveId(source, importer) {
          const prefix = "@yielded/sandbox/";
          if (importer === join(root, "test/e2e/readiness.worker.ts") && source.startsWith(prefix))
            return join(previous, "dist", "sandbox", `${source.slice(prefix.length)}.mjs`);
        },
      },
    ],
  });
  await rm(previous, { recursive: true });
}
const spans = [];
const placementEvents = [];
let logDone;
const configuration = {
  name: runName,
  scriptPath: join(bundle, "readiness-worker.mjs"),
  modulesRoot: bundle,
  modules: true,
  compatibilityDate: "2026-09-30",
  compatibilityFlags: ["nodejs_compat"],
  cf: false,
  telemetry: { enabled: false },
  log: new Log(LogLevel.WARN),
  handleStructuredLogs({ message, level }) {
    if (message.startsWith("READINESS_SPAN "))
      spans.push(JSON.parse(message.slice("READINESS_SPAN ".length)));
    else if (message.startsWith("READINESS_PLACEMENT "))
      placementEvents.push(JSON.parse(message.slice("READINESS_PLACEMENT ".length)));
    else if (message === "READINESS_DONE") logDone?.resolve();
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
    PREPARATION: { className: "Preparation", useSQLite: true },
  },
  bindings: { SANDBOX_MAX_INSTANCES: "8" },
  durableObjectsPersist: join(directory, "state"),
};
const mf = new Miniflare(convertV4MiniflareOptions(configuration));
const names = new Set();
const snapshots = new Set();
const failures = [];
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
  // Drain the native log stream through the marker emitted after the RPC ends.
  await logDone.promise;
  return JSON.parse(body);
};
async function run(mode, name, baseline, expectedInstanceId, expectedLocateCalls = 1) {
  names.add(name);
  const offset = spans.length;
  const placementOffset = placementEvents.length;
  const start = performance.now();
  const result = await rpc({
    name,
    mode,
    ...(baseline === undefined ? {} : { baseline }),
    ...(expectedInstanceId === undefined ? {} : { expectedInstanceId }),
  });
  result.clientWallMillis = Math.round(performance.now() - start);
  result.spans = spans.slice(offset);
  result.placement = placementEvents.slice(placementOffset);
  result.locateCalls = result.placement.filter(
    (event) =>
      event.event === "enter" &&
      event.method === "locate" &&
      event.objectId === evidence.placement.coordinatorId,
  ).length;
  evidence.cases.push(result);
  if (result.checkpoint !== null) snapshots.add(result.checkpoint.snapshotId);
  await writeFile(join(directory, "results.json"), JSON.stringify(evidence, null, 2));
  assert.equal(result.rejectedWrongOwner, true);
  assert.equal(result.sameOwner, true);
  assert.equal(result.installCount, 1);
  assert.equal(
    result.locateCalls,
    expectedLocateCalls,
    "Exact boot handles must not repeat placement",
  );
  console.log(
    JSON.stringify({
      mode,
      name,
      result: result.result,
      installation: result.report?.installation,
      state: result.report?.state,
      clientWallMillis: result.clientWallMillis,
      locateCalls: result.locateCalls,
    }),
  );
  return result;
}
function span(result, name, outcome = "Success") {
  const found = result.spans.find((entry) => entry.name === name && entry.outcome === outcome);
  assert.ok(found, `Missing ${outcome} span ${name} in ${result.mode}`);
  assert.ok(found.durationMillis >= 0);
  return found;
}
function boot(result, start) {
  for (const name of [
    "bootOutputGate",
    "reserve",
    "reserveCapacity",
    "capacityReceiptOutputGate",
    "startOutputGate",
    "nativeStart",
    "refreshInactivity",
    "configureOutbound",
    "ensureSupervisor",
  ])
    span(result, `SandboxContainer.${name}`);
  const attributes = span(result, "SandboxContainer.ensure").attributes;
  assert.equal(attributes["sandbox.container.start"], start);
  assert.equal(attributes["sandbox.container.instanceId"], result.identity.instanceId);
  assert.equal(attributes["sandbox.container.snapshotId"], result.identity.restoredSnapshotId);
}
function ready(result) {
  assert.equal(result.result, "Success");
  assert.equal(result.report.state, "ready");
  assert.equal(result.receipt.prepared, true);
  assert.equal(result.startCount, 1);
  assert.deepEqual(result.app, { status: 200, body: "fixture-ready" });
  span(result, "RepositorySetup.prepare");
  span(result, "repository-services");
}
function retry(result, original) {
  ready(result);
  assert.equal(result.report.installation, "in-place");
  assert.deepEqual(result.identity, original.identity);
  assert.deepEqual(result.checkpoint, original.checkpoint);
  for (const name of [
    "repository-install",
    "repository-start",
    "workspace-checkpoint-capture",
    "SandboxContainer.nativeStart",
  ])
    assert.equal(
      result.spans.some((entry) => entry.name === name),
      false,
      `Retry replayed ${name}`,
    );
}

try {
  await mf.ready;
  const placement = await rpc({ name: "placement", mode: "placement" });
  evidence.placement = { ...placement, events: [...placementEvents] };
  await writeFile(join(directory, "results.json"), JSON.stringify(evidence, null, 2));
  assert.equal(new Set(placement.probes.map((probe) => probe.objectId)).size, 8);
  for (const probe of placement.probes) {
    const lookups = placementEvents.filter(
      (event) => event.event === "lookup" && event.name === probe.name,
    );
    if (probe.first === "recovery") {
      assert.deepEqual(
        lookups.map((event) => event.callerId),
        [placement.callerId],
      );
      assert.equal(probe.initial, "Failure");
      assert.equal(probe.retry, "Failure");
      assert.equal(
        placementEvents.some(
          (event) => event.objectId === probe.objectId && event.method === "locate",
        ),
        false,
      );
      continue;
    }
    assert.equal(lookups[0]?.callerId, placement.coordinatorId, probe.first);
    const direct = lookups.find((event) => event.callerId === placement.callerId);
    assert.ok(direct, `Missing direct lookup after placement: ${probe.first}`);
    const beforeDirect = placementEvents.slice(0, placementEvents.indexOf(direct));
    assert.ok(
      beforeDirect.some(
        (event) =>
          event.event === "return" &&
          event.objectId === probe.objectId &&
          event.method === "locate" &&
          event.outcome === "Success",
      ),
      `Child placement must finish before the caller looks it up: ${probe.first}`,
    );
    assert.equal(
      beforeDirect.findLast(
        (event) =>
          event.event === "return" &&
          event.objectId === placement.coordinatorId &&
          event.method === "locate",
      )?.outcome,
      "Success",
      `NativeReply failure must not authorize direct lookup: ${probe.first}`,
    );
    const entries = placementEvents.filter(
      (event) => event.event === "enter" && event.objectId === probe.objectId,
    );
    assert.equal(entries[0]?.method, "locate", probe.first);
    assert.equal(
      entries.filter((event) => event.method === "locate").length,
      probe.first === "reply" ? 2 : 1,
      `Only an unacknowledged placement retries the handshake: ${probe.first}`,
    );
    const rejected = ["describe", "grant", "recovery", "reply"].includes(probe.first);
    assert.equal(probe.initial, rejected ? "Failure" : "Success", probe.first);
    assert.equal(probe.retry, rejected && probe.first !== "reply" ? "Failure" : "Success");
    if (probe.first === "fetch") assert.equal(probe.value, 404);
    if (probe.first === "inspect") assert.deepEqual(probe.value, []);
    assert.notEqual(probe.objectId, placement.coordinatorId);
  }
  const reentrant = placement.probes.find((probe) => probe.first === "reentrant");
  const enterChild = placementEvents.findIndex(
    (event) =>
      event.event === "enter" && event.objectId === reentrant.objectId && event.method === "locate",
  );
  const returnChild = placementEvents.findIndex(
    (event) =>
      event.event === "return" &&
      event.objectId === reentrant.objectId &&
      event.method === "locate",
  );
  for (const method of ["reserveCapacity", "pinImage", "releaseCapacity"])
    assert.ok(
      placementEvents
        .slice(enterChild, returnChild)
        .some(
          (event) =>
            event.event === "return" &&
            event.objectId === placement.coordinatorId &&
            event.method === method &&
            event.outcome === "Success",
        ),
      `Coordinator must admit ${method} while awaiting placement`,
    );
  console.log("NATIVE_PLACEMENT_PASS: first lookups, replies, retries and reentrant callbacks");
  // An explicit historical repository adds the old-boot/current-owner upgrade proof.
  if (legacyRepository)
    await mf.setOptions(
      convertV4MiniflareOptions({
        ...configuration,
        scriptPath: join(previousBundle, "readiness-worker.mjs"),
        modulesRoot: previousBundle,
      }),
    );
  const cold = await run("cold", "cold", undefined, undefined, legacyRepository ? 0 : 1);
  ready(cold);
  boot(cold, "image");
  assert.equal(cold.report.installation, "executed");
  assert.equal(cold.report.checkpoint.capture, "captured");
  assert.equal(cold.identity.restoredSnapshotId, null);
  assert.equal(cold.checkpoint.classIdentity, placement.coordinatorId);
  for (const name of [
    "repository-install",
    "workspace-checkpoint-capture",
    "repository-start",
    "SandboxContainer.waitForPort",
  ])
    span(cold, name);

  await mf.setOptions(
    convertV4MiniflareOptions({
      ...configuration,
      bindings: { ...configuration.bindings, FIXTURE_REJECT_LOCATE: true },
    }),
  );
  retry(await run("retry", "cold", undefined, cold.identity.instanceId, 0), cold);
  await mf.setOptions(convertV4MiniflareOptions(configuration));

  const restored = await run("restored", "restored", cold.baseline);
  ready(restored);
  boot(restored, "snapshot");
  assert.equal(restored.compatible, true);
  assert.equal(restored.report.installation, "checkpoint");
  assert.equal(restored.report.checkpoint.capture, "reused");
  assert.deepEqual(restored.checkpoint, cold.checkpoint);
  assert.equal(restored.identity.restoredSnapshotId, cold.checkpoint.snapshotId);
  assert.equal(restored.fingerprint, cold.fingerprint);
  assert.notEqual(restored.sourceHead, cold.sourceHead);
  assert.notEqual(restored.identity.instanceId, cold.identity.instanceId);
  for (const name of ["repository-install", "workspace-checkpoint-capture"])
    assert.equal(
      restored.spans.some((entry) => entry.name === name),
      false,
      `Restore repeated ${name}`,
    );

  retry(await run("retry", "restored", undefined, restored.identity.instanceId, 0), restored);

  const incompatible = await run("changed-input", "incompatible", cold.baseline);
  assert.equal(incompatible.compatible, false);
  assert.notEqual(incompatible.fingerprint, cold.fingerprint);
  assert.equal(incompatible.result, "Failure");
  assert.equal(incompatible.startCount, 0);
  assert.equal(incompatible.app, null);
  span(incompatible, "RepositorySetup.prepare", "Failure");

  const failed = await run("failing-service", "failed");
  assert.equal(failed.result, "Success");
  assert.equal(failed.report.state, "failed");
  assert.equal(failed.receipt.prepared, true);
  assert.equal(failed.startCount, 1);
  assert.equal(failed.app, null);
  span(failed, "RepositorySetup.prepare");
  span(failed, "repository-services", "Failure");
  span(failed, "SandboxContainer.waitForPort", "Failure");
} catch (error) {
  failures.push(error);
} finally {
  for (const name of names) {
    try {
      const result = await rpc({ name, mode: "destroy" });
      if (result.snapshotId !== null) snapshots.add(result.snapshotId);
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
    // Pinned workerd leaves egress sidecars behind. Match only this run's UUID,
    // including on failure; never prune shared Docker resources.
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
    if (containers.length > 0) docker("rm", "--force", ...containers);
    for (const snapshot of snapshots) {
      assert.match(snapshot, /^[0-9a-f-]{36}$/);
      docker("image", "rm", `workerd-container-snap-${snapshot}:latest`);
    }
    await rm(join(directory, "state"), { recursive: true, force: true });
    await rm(bundle, { recursive: true, force: true });
    await rm(previousBundle, { recursive: true, force: true });
  } catch (error) {
    failures.push(error);
  }
}
if (failures.length > 0)
  throw new AggregateError(failures, `Native readiness failed; evidence: ${directory}`);
assert.equal(evidence.cases.length, 6, "All six native cases must run");
evidence.passed = true;
await writeFile(join(directory, "results.json"), JSON.stringify(evidence, null, 2));
console.log("NATIVE_READINESS_PASS: 6 cases; owned containers and snapshots removed");
