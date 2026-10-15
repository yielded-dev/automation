import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash, randomBytes, randomUUID } from "node:crypto";
import { createWriteStream } from "node:fs";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { finished } from "node:stream/promises";
import { fileURLToPath } from "node:url";
import { Miniflare, Log, LogLevel, convertV4MiniflareOptions } from "miniflare";
import { build } from "vite-plus/pack";

const expectedProbeRevision = "native-isolation-6";
const [image, stage = "bare", count = "3", selectedOrder = "all", payloadSize] = process.argv.slice(
  process.argv[2] === "--" ? 3 : 2,
);
assert.ok(
  image,
  "Usage: vp node test/e2e/inactivity.mjs <cached local image> [bare|snapshot|identity|sync|alarm|artifacts] [samples=3] [all|inactivity|inspect|exec] [payloadMiB=1..4096]",
);
assert.ok(["bare", "snapshot", "identity", "sync", "alarm", "artifacts"].includes(stage));
assert.ok(["all", "inactivity", "inspect", "exec"].includes(selectedOrder));
const payloadMiB = payloadSize === undefined ? undefined : Number(payloadSize);
if (payloadSize !== undefined) {
  assert.match(payloadSize, /^[1-9]\d{0,3}$/);
  assert.ok(payloadMiB <= 4096);
  assert.ok(["snapshot", "identity", "sync", "alarm"].includes(stage));
}
const samples = Number(count);
assert.ok(Number.isInteger(samples) && samples >= 1 && samples <= 5);
assert.equal(process.env.MINIFLARE_WORKERD_PATH, undefined, "Use installed workerd");
const root = fileURLToPath(new URL("../../", import.meta.url));
const execute = (file, args) =>
  execFileSync(file, args, {
    cwd: root,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  }).trim();
const dockerHost =
  process.env.DOCKER_HOST ??
  execute("docker", ["context", "inspect", "--format", "{{.Endpoints.docker.Host}}"]);
assert.ok(dockerHost.startsWith("unix://"), "Only a local Docker Unix socket is allowed");
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
const directory = await mkdtemp(join(tmpdir(), "repairs-inactivity-"));
const bundle = join(directory, "bundle");
const runName = `repairs-inactivity-${randomUUID()}`;
const token = randomBytes(32).toString("hex");
const mfPackage = createRequire(import.meta.url)("miniflare/package.json");
const evidence = {
  passed: false,
  runName,
  head: execute("git", ["rev-parse", "HEAD"]),
  dirty: execute("git", ["status", "--porcelain"]) !== "",
  workerHash: createHash("sha256")
    .update(await readFile(join(root, "test/e2e/inactivity.worker.ts")))
    .digest("hex"),
  node: process.version,
  miniflare: mfPackage.version,
  workerd: mfPackage.dependencies.workerd,
  docker: docker("version", "--format", "{{.Server.Version}} {{.Server.Os}}/{{.Server.Arch}}"),
  imageId,
  proxyId,
  samples,
  stage,
  selectedOrder,
  payloadMiB,
  command: process.argv.slice(1),
  workload:
    "Fresh native DO/container per sample; cached image; idle shell entrypoint; no Repairs, Effect, server, model, remote API, or artificial delay. /bin/true exit is command readiness, not application readiness.",
  clocks:
    "Worker performance.now advances at I/O boundaries and can round synchronous calls to zero. Client performance.now measures dispatch through response, or alarm submission through completion notification and result lookup. Docker log timestamps are a separate clock domain; do not subtract them from Worker or client spans. Spans overlap; never sum them.",
  cases: [],
  logs: [],
  failures: [],
  cleanup: {},
};
console.log(`Native inactivity evidence: ${directory}`);
const save = () => writeFile(join(directory, "results.json"), JSON.stringify(evidence, null, 2));
await save();
let mf;
const names = new Set();
const snapshots = new Set();
let capturedPayload;
const done = new Map();
const eventStream = createWriteStream(join(directory, "events.jsonl"));
const log = (entry) => {
  const event = { receivedAt: new Date().toISOString(), ...entry };
  evidence.logs.push(event);
  eventStream.write(JSON.stringify(event) + "\n");
};
const ownedContainers = () =>
  docker("ps", "--all", "--filter", `name=^/workerd-${runName}-`, "--format", "{{.ID}}")
    .split("\n")
    .filter(Boolean);
const rpc = async (path, name, params = {}) => {
  const response = await mf.dispatchFetch(`http://localhost/${path}`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ name, ...params }),
  });
  const body = await response.text();
  assert.equal(response.status, 200, body);
  return JSON.parse(body);
};
const destroy = async (name, purge = false) => {
  const result = await rpc(purge ? "purge" : "destroy", name);
  if (result.snapshot) snapshots.add(result.snapshot.id);
  assert.equal(result.running, false);
  if (purge) assert.equal(await rpc("result", name), null);
};
const scenarios = !["bare", "artifacts"].includes(stage)
  ? [{ name: "capture", order: "inactivity", capture: true }]
  : [];
for (let sample = 1; stage !== "artifacts" && sample <= samples; sample++) {
  const orders = selectedOrder === "all" ? ["inactivity", "inspect", "exec"] : [selectedOrder];
  // Rotate which control runs first without creating a product of optional features.
  for (let index = 0; index < orders.length; index++) {
    const order = orders[(index + sample - 1) % orders.length];
    scenarios.push({ name: `${order}-${sample}`, order, capture: false });
  }
}

try {
  await build({
    cwd: root,
    config: false,
    entry: { "inactivity-worker": "test/e2e/inactivity.worker.ts" },
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
  const options = {
    name: runName,
    host: "127.0.0.1",
    scriptPath: join(bundle, "inactivity-worker.mjs"),
    modulesRoot: bundle,
    modules: true,
    compatibilityDate: "2026-09-30",
    compatibilityFlags: ["nodejs_compat"],
    cf: false,
    telemetry: { enabled: false },
    log: new Log(LogLevel.WARN),
    handleStructuredLogs({ message, level }) {
      if (message.startsWith("INACTIVITY_EVENT "))
        log(JSON.parse(message.slice("INACTIVITY_EVENT ".length)));
      else if (message.startsWith("INACTIVITY_DONE "))
        done.get(message.slice("INACTIVITY_DONE ".length))?.resolve();
      else if (level === "error" || level === "warn") log({ level, message });
    },
    containerEngine: {
      localDocker: { socketPath: dockerHost, containerEgressInterceptorImage: proxyId },
    },
    durableObjects: {
      PROBE: {
        className: "InactivityProbe",
        useSQLite: true,
        container: { images: [{ name: "harness", image: imageId }] },
      },
    },
    ...(stage === "artifacts" ? { artifacts: { ARTIFACTS: { namespace: runName } } } : {}),
    isolatedResourcePersistencePath: join(directory, "state"),
  };
  mf = new Miniflare(convertV4MiniflareOptions(options));
  await mf.ready;
  evidence.gate = {
    missingConfiguration: (await mf.dispatchFetch("http://localhost/health", { method: "POST" }))
      .status,
  };
  assert.equal(evidence.gate.missingConfiguration, 503);
  // Reuse the exact runtime configuration; no actor has been accessed yet.
  await mf.setOptions(
    convertV4MiniflareOptions({ ...options, bindings: { ISOLATION_TEST_TOKEN: token } }),
  );
  await mf.ready;
  evidence.gate.missingToken = (
    await mf.dispatchFetch("http://localhost/health", { method: "POST" })
  ).status;
  evidence.gate.wrongToken = (
    await mf.dispatchFetch("http://localhost/health", {
      method: "POST",
      headers: { Authorization: `Bearer ${"0".repeat(64)}` },
    })
  ).status;
  evidence.gate.wrongMethod = (
    await mf.dispatchFetch("http://localhost/health", {
      headers: { Authorization: `Bearer ${token}` },
    })
  ).status;
  assert.equal(evidence.gate.missingToken, 401);
  assert.equal(evidence.gate.wrongToken, 401);
  assert.equal(evidence.gate.wrongMethod, 405);
  evidence.health = await rpc("health", "gate");
  assert.deepEqual(evidence.health, {
    ok: true,
    probeRevision: expectedProbeRevision,
    imageReference: "named:harness",
  });
  evidence.gate.authorized = true;
  if (stage === "artifacts") {
    const started = performance.now();
    evidence.artifacts = await rpc("artifacts", "support");
    evidence.artifacts.clientWallMillis = performance.now() - started;
    if (!evidence.artifacts.supported)
      assert.equal(
        evidence.artifacts.failure.message,
        "Binding ARTIFACTS needs to be run remotely",
      );
    else
      scenarios.push({
        name: "artifacts-boundary",
        order: selectedOrder === "all" ? "inactivity" : selectedOrder,
        capture: false,
      });
    await save();
    console.log(JSON.stringify(evidence.artifacts));
  }
  for (const { name, order, capture } of scenarios) {
    names.add(name);
    done.set(name, Promise.withResolvers());
    const item = { name, passed: false, startedAt: new Date().toISOString() };
    evidence.cases.push(item);
    await save();
    const started = performance.now();
    try {
      if (!["bare", "artifacts"].includes(stage) && !capture)
        assert.equal(snapshots.size, 1, "Capture must succeed; no image fallback");
      if (payloadMiB !== undefined && !capture)
        assert.ok(capturedPayload, "Payload capture must succeed before restore");
      const params = {
        order,
        ...(payloadMiB !== undefined
          ? {
              payloadMiB: String(payloadMiB),
              ...(!capture ? { expectedPayloadSha256: capturedPayload.sha256 } : {}),
            }
          : {}),
        stage: capture ? "snapshot" : stage === "artifacts" ? "identity" : stage,
        ...(capture
          ? { capture: "true" }
          : !["bare", "artifacts"].includes(stage)
            ? { snapshot: [...snapshots][0] }
            : {}),
      };
      if (stage === "artifacts") {
        item.artifactsRepo = `inactivity-${randomUUID()}`;
        params.artifactsRepo = item.artifactsRepo;
        await save();
      }
      if (stage === "alarm" && !capture) {
        const receipt = await rpc("schedule", name, params);
        item.acceptanceWallMillis = performance.now() - started;
        await done.get(name).promise;
        Object.assign(item, await rpc("result", name));
        assert.deepEqual(item.accepted, receipt);
      } else Object.assign(item, await rpc("run", name, params));
      item.clientWallMillis = performance.now() - started;
      if (item.capturedSnapshot) snapshots.add(item.capturedSnapshot.id);
      await done.get(name).promise;
      item.docker = ownedContainers()
        .map((id) => {
          const [info] = JSON.parse(docker("inspect", id));
          if (!info.Name.includes(item.id)) return undefined;
          return {
            id,
            name: info.Name,
            image: info.Config.Image,
            startedAt: info.State.StartedAt,
            running: info.State.Running,
            processMarker: docker("logs", "--timestamps", id)
              .split("\n")
              .filter((line) => line.includes("INACTIVITY_PROCESS_READY")),
          };
        })
        .filter(Boolean);
      assert.equal(item.passed, true, JSON.stringify(item.failure));
      assert.equal(item.probeRevision, expectedProbeRevision, "Actor revision changed");
      if (stage !== "alarm" || capture)
        assert.equal(item.callerRevision, expectedProbeRevision, "Caller revision changed");
      assert.equal(item.instance, "standard-1");
      if (payloadMiB !== undefined) {
        assert.equal(item.payload.sizeBytes, payloadMiB * 1024 * 1024);
        assert.match(item.payload.sha256, /^[a-f0-9]{64}$/);
        if (capture) capturedPayload = item.payload;
        else assert.deepEqual(item.payload, capturedPayload);
        const readyIndex = item.events.findIndex((event) => event.operation === "commandReady");
        const payloadIndex = item.events.findIndex((event) =>
          event.operation.startsWith(capture ? "generatePayload" : "verifyPayload"),
        );
        assert.ok(readyIndex >= 0 && payloadIndex > readyIndex, "Payload work preceded readiness");
      }
      if (params.snapshot) {
        assert.equal(item.restoredSnapshotId, params.snapshot);
        assert.equal(
          item.verification,
          payloadMiB === undefined ? "synthetic-marker" : "generated-payload",
        );
        assert.equal(item.imageReference, undefined);
      } else assert.equal(item.imageReference, imageId);
      if (stage === "alarm" && !capture) {
        assert.deepEqual(await rpc("schedule", name, params), item.accepted);
        item.acceptanceRetryStable = true;
        assert.equal(item.alarm.isRetry, false);
      }
      const process = item.docker.find((entry) => entry.processMarker.length === 1);
      assert.ok(process, "Missing independently observed PID1 marker");
      assert.equal(
        process.image,
        params.snapshot ? `workerd-container-snap-${params.snapshot}` : imageId,
        "Docker image did not match the requested image or snapshot",
      );
    } catch (cause) {
      item.passed = false;
      item.failure ??= String(cause.stack ?? cause);
      evidence.failures.push({ name, failure: item.failure });
    } finally {
      await save();
      await destroy(name);
    }
    console.log(
      JSON.stringify({
        name,
        passed: item.passed,
        clientWallMillis: item.clientWallMillis,
        commandReadyMillis: item.events?.find((event) => event.operation === "commandReady")
          ?.millis,
        snapshotSizeBytes: item.capturedSnapshot?.size,
        payload: item.payload,
        calls: item.events
          ?.filter(
            (event) =>
              event.phase === "end" &&
              item.events.some(
                (begin) => begin.operation === event.operation && begin.phase === "begin",
              ),
          )
          .map((event) => ({
            operation: event.operation,
            millis:
              event.millis -
              (item.events.find(
                (begin) => begin.operation === event.operation && begin.phase === "begin",
              )?.millis ?? 0),
          })),
      }),
    );
  }
} catch (cause) {
  evidence.failures.push({ failure: String(cause.stack ?? cause) });
} finally {
  if (mf) {
    for (const name of names) {
      try {
        await destroy(name, true);
      } catch (cause) {
        evidence.failures.push({ cleanup: name, failure: String(cause) });
      }
    }
    try {
      await mf.dispose();
    } catch (cause) {
      evidence.failures.push({ cleanup: "miniflare", failure: String(cause) });
    }
  }
  try {
    const ids = ownedContainers();
    if (ids.length) docker("rm", "--force", ...ids);
    evidence.cleanup = { removedContainers: ids, remainingContainers: ownedContainers() };
    evidence.cleanup.removedSnapshots = [];
    for (const id of snapshots) {
      assert.match(id, /^[0-9a-f-]{36}$/);
      docker("image", "rm", `workerd-container-snap-${id}:latest`);
      evidence.cleanup.removedSnapshots.push(id);
    }
    await rm(join(directory, "state"), { recursive: true, force: true });
    await rm(bundle, { recursive: true, force: true });
  } catch (cause) {
    evidence.failures.push({ cleanup: "docker/state", failure: String(cause) });
  }
  evidence.passed =
    evidence.failures.length === 0 &&
    evidence.cases.length === scenarios.length &&
    evidence.cases.every((item) => item.passed);
  await save();
  eventStream.end();
  await finished(eventStream);
}
assert.equal(evidence.passed, true, `Native inactivity probe failed; evidence: ${directory}`);
console.log(
  stage === "artifacts"
    ? evidence.artifacts.supported
      ? "NATIVE_ARTIFACTS_BOUNDARY_PASS: real repo retained across native container call; no Git checkout"
      : "NATIVE_ARTIFACTS_UNAVAILABLE: capability check only; container/repository operations skipped"
    : `NATIVE_INACTIVITY_PASS: ${evidence.cases.length} samples; owned resources removed`,
);
