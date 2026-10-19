import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { appendFile, mkdir, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Log, LogLevel, Miniflare, convertV4MiniflareOptions } from "miniflare";
import { build } from "vite-plus/pack";

const args = process.argv.slice(process.argv[2] === "--" ? 3 : 2);
const [target, directory, ...extra] = args;
assert.ok(
  target && directory && extra.length === 0,
  "Usage: vp exec node packages/sandbox/test/e2e/checkpoints.mjs -- <local image|https://worker> <evidence directory>",
);
const hosted = target.startsWith("https://");
const token = hosted ? process.env.CHECKPOINT_PROOF_TOKEN : randomUUID();
assert.ok(token, "Hosted proof requires CHECKPOINT_PROOF_TOKEN");
const root = fileURLToPath(new URL("../../", import.meta.url));
const runName = `checkpoint-proof-${randomUUID()}`;
const evidence = {
  runName,
  hosted,
  target,
  startedAt: new Date().toISOString(),
  passed: false,
  scope:
    "Native filesystem checkpoint correctness; hosted runs add three same-image, same-directory save/restore comparisons. No model, Git, or customer workspace.",
  requests: [],
  spans: [],
  comparisons: [],
  failures: [],
  cleanup: [],
};
const snapshots = new Set();
const names = new Set();
const backups = new Map();
let mf, docker;
await mkdir(directory, { recursive: true });
const persist = () => writeFile(join(directory, "results.json"), JSON.stringify(evidence, null, 2));
const execute = (file, args) =>
  execFileSync(file, args, { cwd: root, encoding: "utf8", timeout: 60_000 }).trim();
const remember = (result) => {
  if (result?.checkpoint?.snapshotId) snapshots.add(result.checkpoint.snapshotId);
  if (result?.backup?.id) backups.set(result.backup.id, result.backup);
};
async function request(name, mode, options = {}) {
  names.add(name);
  const input = { name, mode, ...options };
  const attempt = { input, startedAt: new Date().toISOString() };
  const started = performance.now();
  try {
    const init = {
      method: "POST",
      headers: { "content-type": "application/json", authorization: `Bearer ${token}` },
      body: JSON.stringify(input),
      signal: AbortSignal.timeout(240_000),
    };
    const response = hosted
      ? await fetch(target, init)
      : await mf.dispatchFetch("http://checkpoint.fixture", init);
    attempt.status = response.status;
    const text = await response.text();
    try {
      attempt.response = JSON.parse(text);
    } catch {
      throw new Error(`Non-JSON fixture response (${response.status}): ${text.slice(0, 1000)}`);
    }
    remember(attempt.response.result);
    return attempt.response;
  } catch (error) {
    attempt.failure = String(error);
    if (error.cause)
      attempt.failureCause = {
        name: error.cause.name,
        message: error.cause.message,
        code: error.cause.code,
      };
    throw error;
  } finally {
    attempt.clientMs = performance.now() - started;
    evidence.requests.push(attempt);
    await appendFile(join(directory, "attempts.jsonl"), JSON.stringify(attempt) + "\n");
    await persist();
  }
}
async function rpc(name, mode, options) {
  const response = await request(name, mode, options);
  assert.equal(response.ok, true, JSON.stringify(response));
  return response.result;
}
const content = (version) => `module.exports = { version: ${version} };\n`;
function capture(result, identity) {
  assert.equal(result.checkpoint.kind, "container");
  assert.equal(result.checkpoint.instanceId, identity.instanceId);
  assert.equal(result.checkpoint.imageDigest, identity.imageDigest);
  assert.deepEqual(
    Object.keys(result.checkpoint).sort(),
    [
      "kind",
      "snapshotId",
      "imageDigest",
      "classIdentity",
      "instanceId",
      "capturedAt",
      "expiresAt",
    ].sort(),
  );
  assert.equal(result.after.running, true, "Capture stopped the container");
  assert.equal(result.after.current.instanceId, identity.instanceId);
  assert.deepEqual(result.after.checkpoint, result.checkpoint);
  assert.equal(result.heartbeatBefore.alive, true);
  assert.equal(result.heartbeatAfter.alive, true, "Capture stopped the heartbeat process");
  assert.equal(result.heartbeatAfter.pid, result.heartbeatBefore.pid);
  assert.equal(result.heartbeatAfter.startedAt, result.heartbeatBefore.startedAt);
  assert.ok(result.heartbeatAfter.ticks >= result.heartbeatBefore.ticks);
}
async function verified(name, seed, version) {
  const actual = await rpc(name, "inspect");
  assert.deepEqual(actual.workload, { ...seed.workload, version: content(version) });
  assert.notEqual(actual.heartbeat.alive, true, "A process from the snapshot must not resume");
  return actual;
}

try {
  evidence.head = execute("git", ["rev-parse", "HEAD"]);
  evidence.dirty = execute("git", ["status", "--porcelain"]) !== "";
  evidence.node = process.version;
  if (!hosted) {
    assert.equal(process.env.MINIFLARE_WORKERD_PATH, undefined, "Use pinned workerd");
    const host =
      process.env.DOCKER_HOST ??
      execute("docker", ["context", "inspect", "--format", "{{.Endpoints.docker.Host}}"]);
    assert.ok(host.startsWith("unix://"));
    docker = (...args) => execute("docker", ["--host", host, ...args]);
    const imageId = docker("image", "inspect", target, "--format", "{{.Id}}");
    assert.equal(
      docker("image", "inspect", imageId, "--format", "{{.Os}}/{{.Architecture}}"),
      "linux/amd64",
    );
    const proxy =
      "cloudflare/proxy-everything:3cb1195@sha256:0ef6716c52430096900b150d84a3302057d6cd2319dae7987128c85d0733e3c8";
    const proxyId = docker("image", "inspect", proxy, "--format", "{{.Id}}");
    evidence.imageId = imageId;
    const miniflare = createRequire(import.meta.url)("miniflare/package.json");
    evidence.miniflare = miniflare.version;
    evidence.workerd = miniflare.dependencies.workerd;
    const bundle = join(directory, "bundle");
    await build({
      cwd: root,
      config: false,
      entry: { checkpoints: "test/e2e/checkpoints.worker.ts" },
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
    mf = new Miniflare(
      convertV4MiniflareOptions({
        name: runName,
        scriptPath: join(bundle, "checkpoints.mjs"),
        modulesRoot: bundle,
        modules: true,
        compatibilityDate: "2026-09-30",
        compatibilityFlags: ["nodejs_compat"],
        cf: false,
        telemetry: { enabled: false },
        log: new Log(LogLevel.WARN),
        handleStructuredLogs({ message, level }) {
          if (message.startsWith("CHECKPOINT_SPAN "))
            evidence.spans.push(JSON.parse(message.slice(16)));
          else if (level === "warn" || level === "error") console.error(message);
        },
        containerEngine: {
          localDocker: { socketPath: host, containerEgressInterceptorImage: proxyId },
        },
        durableObjects: {
          SANDBOX: {
            className: "Sandbox",
            useSQLite: true,
            container: { images: [{ name: "harness", image: imageId }] },
          },
        },
        r2Buckets: ["BACKUPS"],
        bindings: { SANDBOX_MAX_INSTANCES: "4", CHECKPOINT_PROOF_TOKEN: token },
        durableObjectsPersist: join(directory, "state"),
        r2Persist: join(directory, "r2"),
      }),
    );
    await mf.ready;
  }
  console.log(`Checkpoint proof: ${directory}`);
  const source = `${runName}-source`;
  const seed = await rpc(source, "seed", { representative: hosted });
  evidence.workload = seed.workload;
  evidence.imageDigest = seed.identity.imageDigest;
  assert.equal(seed.workload.git, false);
  if (hosted) {
    assert.equal(seed.workload.bytes + Buffer.byteLength(content(0)), 100 * 1024 * 1024);
    assert.equal(seed.workload.dependencies, true);
  }
  const first = await rpc(source, "checkpoint", {
    input: { instanceId: seed.identity.instanceId, sequence: 0 },
  });
  capture(first, seed.identity);
  const retry = await rpc(source, "checkpoint", {
    input: { instanceId: seed.identity.instanceId, sequence: 0 },
  });
  assert.deepEqual(
    retry.checkpoint,
    first.checkpoint,
    "Latest same-sequence retry changed the receipt",
  );
  await rpc(source, "version", { version: 1 });
  const second = await rpc(source, "checkpoint", {
    input: { instanceId: seed.identity.instanceId, sequence: 1 },
  });
  capture(second, seed.identity);
  assert.notEqual(second.checkpoint.snapshotId, first.checkpoint.snapshotId);
  const stale = await request(source, "checkpoint", {
    input: { instanceId: seed.identity.instanceId, sequence: 0 },
  });
  assert.equal(stale.ok, false, "Stale sequence was accepted");
  assert.deepEqual((await rpc(source, "describe")).checkpoint, second.checkpoint);
  const pairs = [];
  if (hosted) {
    pairs.push({
      native: second,
      archive: await rpc(source, "backup"),
      order: ["native", "archive"],
    });
    for (let sequence = 2; sequence <= 3; sequence++) {
      const order = sequence === 2 ? ["archive", "native"] : ["native", "archive"];
      const pair = { order };
      for (const mode of order)
        pair[mode] =
          mode === "native"
            ? await rpc(source, "checkpoint", {
                input: { instanceId: seed.identity.instanceId, sequence },
              })
            : await rpc(source, "backup");
      capture(pair.native, seed.identity);
      pairs.push(pair);
    }
  }
  assert.equal((await rpc(source, "park")).running, false);
  const restored0 = await rpc(source, "restore", { checkpoint: first.checkpoint });
  assert.equal(restored0.identity.restoredSnapshotId, first.checkpoint.snapshotId);
  assert.notEqual(restored0.identity.instanceId, seed.identity.instanceId);
  assert.equal(restored0.content, content(0));
  await verified(source, seed, 0);
  const liveRetry = await rpc(source, "restore", { checkpoint: first.checkpoint });
  assert.deepEqual(
    liveRetry.identity,
    restored0.identity,
    "Matching live restore started a new boot",
  );
  const liveMismatch = await request(source, "restore", { checkpoint: second.checkpoint });
  assert.equal(
    liveMismatch.ok,
    false,
    "Explicit restore silently adopted a different live snapshot",
  );
  await rpc(source, "park");
  const restored1 = await rpc(source, "restore", { checkpoint: second.checkpoint });
  assert.equal(restored1.identity.restoredSnapshotId, second.checkpoint.snapshotId);
  assert.equal(restored1.content, content(1));
  await verified(source, seed, 1);
  await rpc(source, "park");
  const rejected = `${runName}-unavailable`;
  for (const [reason, patch] of [
    ["expired", { expiresAt: 0 }],
    ["image", { imageDigest: "sha256:" + "0".repeat(64) }],
    ["missing", { snapshotId: randomUUID() }],
  ]) {
    const unavailable = await request(rejected, "restore", {
      checkpoint: { ...second.checkpoint, ...patch },
    });
    assert.equal(unavailable.ok, false, `${reason} checkpoint silently fresh-booted`);
    if (reason !== "missing")
      assert.equal(
        unavailable.error?.code,
        "CHECKPOINT_UNAVAILABLE",
        `${reason} did not classify unavailable`,
      );
    const state = await rpc(rejected, "describe");
    assert.equal(state.running, false, `${reason} left a fresh container running`);
  }
  for (const snapshotAllowed of [true, false]) {
    const name = `${runName}-legacy-${snapshotAllowed ? "captured" : "disabled"}`;
    const legacy = await rpc(name, "seed-legacy", {
      checkpoint: first.checkpoint,
      snapshotAllowed,
    });
    const response = await request(name, "checkpoint", {
      input: { instanceId: first.checkpoint.instanceId, sequence: 0 },
    });
    if (snapshotAllowed) {
      assert.equal(response.ok, true);
      assert.deepEqual(
        response.result.checkpoint,
        legacy.result,
        "Legacy receipt lost repository metadata",
      );
    } else {
      assert.equal(response.ok, false, "A disabled legacy capture must not be recaptured");
    }
    const state = await rpc(name, "describe");
    assert.equal(state.running, false, "Legacy receipt lookup must not start a container");
  }
  evidence.correctnessPassed = true;
  if (!hosted)
    assert.equal(
      evidence.spans.filter((span) => span.name === "SandboxContainer.nativeSnapshot").length,
      2,
      "Retries and stale sequences must not invoke another native snapshot",
    );
  for (const [index, pair] of pairs.entries()) {
    const name = `${runName}-comparison-${index}`;
    const comparison = {
      index,
      order: pair.order,
      native: { saveMs: pair.native.saveMs },
      archive: { saveMs: pair.archive.saveMs, compressedBytes: pair.archive.backup.size },
    };
    evidence.comparisons.push(comparison);
    for (const mode of pair.order) {
      try {
        const restored =
          mode === "native"
            ? await rpc(name, "restore", { checkpoint: pair.native.checkpoint })
            : await rpc(name, "restore-directory", { backup: pair.archive.backup });
        comparison[mode].restore = restored;
        assert.equal(restored.content, content(1));
        assert.equal(restored.identity.imageDigest, seed.identity.imageDigest);
        assert.equal(
          restored.identity.restoredSnapshotId,
          mode === "native" ? pair.native.checkpoint.snapshotId : null,
        );
        comparison[mode].verification = await verified(name, seed, 1);
        comparison[mode].passed = true;
      } catch (error) {
        comparison[mode].failure = String(error);
        evidence.failures.push(String(error));
      } finally {
        await rpc(name, "park");
      }
    }
  }
  evidence.passed = evidence.failures.length === 0;
} catch (error) {
  evidence.failures.push(error.stack ?? String(error));
} finally {
  for (const backup of backups.values()) {
    try {
      await rpc(`${runName}-source`, "delete-backup", { backup });
      evidence.cleanup.push({ backup: backup.id, deleted: true });
    } catch (error) {
      evidence.cleanup.push({ backup: backup.id, failure: String(error) });
    }
  }
  for (const name of names) {
    try {
      await rpc(name, "destroy");
      evidence.cleanup.push({ name, destroyed: true });
    } catch (error) {
      evidence.cleanup.push({ name, failure: String(error) });
    }
  }
  try {
    await mf?.dispose();
    if (docker) {
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
      for (const id of snapshots) {
        assert.match(id, /^[0-9a-f-]{36}$/);
        docker("image", "rm", `workerd-container-snap-${id}:latest`);
      }
      await rm(join(directory, "state"), { recursive: true, force: true });
      await rm(join(directory, "r2"), { recursive: true, force: true });
    }
  } catch (error) {
    evidence.cleanup.push({ failure: String(error) });
  }
  if (evidence.cleanup.some((entry) => entry.failure)) evidence.passed = false;
  evidence.finishedAt = new Date().toISOString();
  evidence.snapshots = [...snapshots];
  await persist();
}
console.log(
  JSON.stringify({
    passed: evidence.passed,
    directory,
    correctnessPassed: evidence.correctnessPassed,
    comparisons: evidence.comparisons.length,
    failures: evidence.failures,
    cleanupFailures: evidence.cleanup.filter((entry) => entry.failure),
  }),
);
if (!evidence.passed) process.exitCode = 1;
