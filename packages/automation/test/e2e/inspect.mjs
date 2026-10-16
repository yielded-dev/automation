import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { Miniflare, Log, LogLevel, convertV4MiniflareOptions } from "miniflare";
import { build } from "vite-plus/pack";
import { v5 as uuidV5 } from "uuid";

assert.equal(process.env.MINIFLARE_WORKERD_PATH, undefined, "Use the pinned workerd binary");
const [legacyRepository, ...extra] = process.argv.slice(process.argv[2] === "--" ? 3 : 2);
assert.equal(
  extra.length,
  0,
  "Usage: vp run -F @yielded/automation test:e2e -- [local legacy repository]",
);
const root = fileURLToPath(new URL("../../", import.meta.url));
const legacyPin = "82e08f0f5b819d9e69dc697fc87dda14cb71ea07";
const cache = join(root, ".vite");
await mkdir(cache, { recursive: true });
const directory = await mkdtemp(join(cache, "inspect-"));
const legacy = join(directory, "legacy");
if (legacyRepository) {
  await mkdir(legacy);
  // Only the explicitly supplied local repository supplies historical code; never fetch it.
  execFileSync("tar", ["-xf", "-", "-C", legacy], {
    input: execFileSync("git", ["archive", legacyPin, "dist"], {
      cwd: resolve(legacyRepository),
      maxBuffer: 16_000_000,
    }),
  });
}
const serverLogs = [];
const blocked = [];
const completion = {
  scope:
    "Real Repairs recovery, SQLite, task alarms and reload from seeded model/candidate receipts. Foreign host queue acceptance, Artifacts metadata and compute-cleanup endpoints only. No live Queue, model, checkout, container or provider proof.",
  deliveries: [],
  metadataReads: [],
  cleanup: [],
  cases: [],
};
const deliveryModes = new Map();
const acceptedCompletions = new Set();
const heldReplies = new Map();
const mfPackage = createRequire(import.meta.url)("miniflare/package.json");
const evidence = {
  scope: legacyRepository
    ? "Actual pre-fingerprint Repairs DO and current typed client in native workerd, reading a disposable running SQLite record with a launch receipt. No container, model or provider calls."
    : "Current automation DO and typed client in native workerd, reading a disposable running SQLite record with a launch receipt. Current recovery, alarms and reload; no historical compatibility, container, model or provider proof.",
  server: legacyRepository ? legacyPin : "current",
  clientBase: execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).trim(),
  miniflare: mfPackage.version,
  workerd: mfPackage.dependencies.workerd,
  serverLogs,
  completion,
  passed: false,
};
console.log(`Native inspect evidence: ${directory}`);
const bundles = {};
for (const [name, entry] of [
  ["server", "test/e2e/fixtures/inspect-server.worker.ts"],
  ["client", "test/e2e/inspect.worker.ts"],
  ["completion", "test/e2e/completion.worker.ts"],
]) {
  const outDir = join(directory, name);
  await build({
    cwd: root,
    config: false,
    entry: { [name]: entry },
    outDir,
    outExtensions: () => ({ js: ".mjs" }),
    platform: "neutral",
    target: "es2022",
    dts: false,
    deps: {
      alwaysBundle: [/^(?!cloudflare:|node:)/],
      neverBundle: [/^(cloudflare:|node:)/],
      onlyBundle: false,
    },
    plugins:
      name === "server" && legacyRepository
        ? [
            {
              name: "published-inspect-server",
              resolveId(source, importer) {
                if (source === "@cloudflare/sandbox")
                  return this.resolve(
                    source,
                    fileURLToPath(import.meta.resolve("@yielded/sandbox")),
                  );
                if (importer !== join(root, entry)) return;
                const automationPrefix = "../../../src/";
                if (source.startsWith(automationPrefix))
                  return join(legacy, "dist", `${source.slice(automationPrefix.length)}.mjs`);
                const sandboxPrefix = "@yielded/sandbox/";
                if (source.startsWith(sandboxPrefix))
                  return join(
                    legacy,
                    "dist",
                    "sandbox",
                    `${source.slice(sandboxPrefix.length)}.mjs`,
                  );
              },
            },
          ]
        : [],
  });
  bundles[name] = join(outDir, `${name}.mjs`);
  evidence[`${name}BundleSha256`] = createHash("sha256")
    .update(await readFile(bundles[name]))
    .digest("hex");
}
const deny = async (request) => {
  blocked.push(new URL(request.url).origin);
  return new Response("Unexpected external operation", { status: 503 });
};
const candidate = "e".repeat(40);
let metadataDelayMillis = 0;
const metadata = async (request) => {
  const input = await request.json();
  if (metadataDelayMillis > 0)
    await new Promise((resolve) => setTimeout(resolve, metadataDelayMillis));
  completion.metadataReads.push(input);
  assert.equal(input.name, "completion-candidate");
  if (input.operation === "info")
    return Response.json({
      id: "completion-generation",
      name: input.name,
      description: null,
      defaultBranch: "main",
      createdAt: "2026-10-14T00:00:00Z",
      updatedAt: "2026-10-14T00:00:00Z",
      lastPushAt: null,
      source: null,
      readOnly: false,
      remote: "https://fixture.invalid/completion.git",
    });
  assert.equal(
    input.operation,
    "readCommit",
    "No checkout, token mint or repository mutation is allowed",
  );
  assert.deepEqual(input.args, [candidate]);
  return Response.json({
    hash: candidate,
    treeHash: "f".repeat(40),
    message: "fixture",
    parents: [],
    author: { name: "Fixture", email: "fixture@example.invalid" },
    committer: { name: "Fixture", email: "fixture@example.invalid" },
    authoredAt: 0,
    committedAt: 0,
  });
};
const notification = async (request) => {
  const event = await request.json();
  assert.deepEqual(Object.keys(event), ["taskId"], "The host only receives the bounded identity");
  const observed = await completionPost("evidence", { id: event.taskId });
  assert.ok(
    ["succeeded", "failed", "cancelled"].includes(observed.state.status),
    "The receiver must observe committed terminal state",
  );
  assert.equal(
    pending(observed, "run"),
    null,
    "Model recovery must be cancelled before notification",
  );
  assert.ok(pending(observed, "settled"), "Delivery must pre-arm its durable retry before sending");
  assert.ok(pending(observed, "settled").attempts > 0);
  assert.ok(
    pending(observed, "settled").retryAt > pending(observed, "settled").runAt,
    "An in-flight send needs a future durable wake, not only its consumed alarm row",
  );
  const mode = deliveryModes.get(event.taskId);
  const duplicate = acceptedCompletions.has(event.taskId);
  const accepted = mode !== "reject";
  if (accepted) acceptedCompletions.add(event.taskId);
  completion.deliveries.push({ event, mode, accepted, duplicate, observed });
  if (mode === "interrupted")
    return new Promise((resolve) => heldReplies.set(event.taskId, resolve));
  // A lost reply is injected after durable host acceptance, leaving an ambiguous sender outcome.
  return new Response(null, { status: mode === "accept" ? 204 : 503 });
};
const common = {
  modules: true,
  modulesRoot: directory,
  compatibilityDate: "2026-09-30",
  compatibilityFlags: ["nodejs_compat"],
  outboundService: deny,
};
const options = convertV4MiniflareOptions({
  cf: false,
  telemetry: { enabled: false },
  log: new Log(LogLevel.WARN),
  handleStructuredLogs({ message }) {
    if (message.startsWith("INSPECT_")) serverLogs.push(message);
    if (message.startsWith("COMPLETION_CLEANUP ")) completion.cleanup.push(message);
    assert.ok(!message.includes("COMPLETION_UNEXPECTED_MODEL_REPLAY"), "Never rerun the model");
  },
  durableObjectsPersist: join(directory, "state"),
  workers: [
    {
      ...common,
      name: "inspect-client",
      scriptPath: bundles.client,
      serviceBindings: { SERVER: "inspect-server", COMPLETION: "completion" },
      durableObjects: {
        RUNS: { className: "RepairRun", scriptName: "inspect-server", useSQLite: true },
      },
    },
    {
      ...common,
      name: "inspect-server",
      scriptPath: bundles.server,
      serviceBindings: { FIXTURE_ARTIFACTS: deny },
      durableObjects: { RUNS: { className: "RepairRun", useSQLite: true } },
    },
    {
      ...common,
      name: "completion",
      scriptPath: bundles.completion,
      serviceBindings: { FIXTURE_ARTIFACTS: metadata, NOTIFICATIONS: notification },
      durableObjects: {
        RUNS: { className: "CompletionRun", useSQLite: true },
        QUIET_RUNS: { className: "QuietRun", useSQLite: true },
        SANDBOXES: { className: "CleanupSandbox", useSQLite: true },
      },
    },
  ],
});
const mf = new Miniflare(options);
const completionPost = async (path, input) => {
  const response = await mf.dispatchFetch(`http://localhost/completion/${path}`, {
    method: "POST",
    body: JSON.stringify(input),
  });
  const body = await response.text();
  assert.equal(response.status, 200, body);
  return JSON.parse(body);
};
const pending = (observed, tag) => observed.pending.find((item) => item.tag === tag).alarm;
const eventually = async (read, ready, label) => {
  const deadline = Date.now() + 20_000;
  let result;
  do {
    result = await read();
    if (ready(result)) return result;
    await new Promise((resolve) => setTimeout(resolve, 50));
  } while (Date.now() < deadline);
  assert.fail(`${label}: ${JSON.stringify(result)}`);
};
try {
  const id = randomUUID();
  const commandId = randomUUID();
  const fingerprint = "a".repeat(64);
  const repair = {
    repairId: randomUUID(),
    attemptId: randomUUID(),
    repositoryId: "fixture/project",
    evidenceHash: "c".repeat(64),
    proofPolicyHash: "d".repeat(64),
  };
  const turnId = uuidV5(`task-turn:${commandId}`, id);
  const saved = {
    request: {
      id,
      commandId,
      title: "Native running inspect regression",
      prompt: "Unused synthetic task",
      modelSettings: { model: "fixture", reasoningEffort: "high", fast: false },
      execution: "agent",
      source: {
        _tag: "SeedRepository",
        repositoryName: "fixture-seed",
        artifactsRepositoryId: "fixture-seed-generation",
        commitSha: "b".repeat(40),
      },
      repair,
      deadlineAtMillis: Date.now() + 60_000,
      outputSchema: {},
      requiredCommits: [],
    },
    fingerprint,
    createdAtMillis: Date.now(),
    status: "running",
    cancelled: false,
    launchReceipt: {
      _tag: "SandboxTaskLaunchReceipt",
      commandId,
      requestFingerprint: fingerprint,
      result: { id, turnId, outcome: "launched" },
    },
    savedCheckpoint: null,
    latestSequence: 1,
  };
  const post = async (path, body) => {
    const response = await mf.dispatchFetch(`http://localhost/${path}`, {
      method: "POST",
      body: JSON.stringify(body),
    });
    const text = await response.text();
    assert.equal(response.status, 200, text);
    return JSON.parse(text);
  };
  evidence.seed = await post("seed", saved);
  assert.deepEqual(evidence.seed, { id, status: "running", receipt: true });
  evidence.inspect = await post("inspect", { id });
  assert.equal(evidence.inspect.exit, "Success", evidence.inspect.failure);
  assert.deepEqual(evidence.inspect.value, {
    id,
    ...(legacyRepository ? {} : { requestFingerprint: fingerprint }),
    launchAssociation: { _tag: "Repair", ...repair },
    status: "running",
    activeTurnId: turnId,
    latestSequence: 1,
  });
  await mf.setOptions(options);
  evidence.reloaded = await post("inspect", { id });
  assert.deepEqual(evidence.reloaded, evidence.inspect);
  assert.ok(
    serverLogs.includes('INSPECT_SERVER {"name":"RepairSandboxTask.inspect","exit":"Success"}'),
  );
  assert.deepEqual(blocked, [], "No provider operation is allowed");
  console.log(
    legacyRepository
      ? "Native old-server/new-client running inspect passed, including reload"
      : "Native current-server/current-client running inspect passed, including reload",
  );

  const seed = (changes = {}) => {
    const id = randomUUID();
    const commandId = randomUUID();
    const turnId = uuidV5(`task-turn:${commandId}`, id);
    return {
      request: {
        id,
        commandId,
        title: "Native completion regression",
        prompt: "Private synthetic prompt",
        modelSettings: { model: "fixture", reasoningEffort: "high", fast: false },
        execution: "agent",
        source: {
          _tag: "AttachedRepository",
          repositoryName: "completion-candidate",
          artifactsRepositoryId: "completion-generation",
          commitSha: "b".repeat(40),
        },
        repair,
        deadlineAtMillis: Date.now() + 120_000,
        outputSchema: {},
        requiredCommits: [],
        ...changes,
      },
      pipeline: {
        id: turnId,
        materializationId: "fixture-materialization",
        head: candidate,
        baseHead: "b".repeat(40),
        resumeToken: null,
        harnessVersion: "fixture-no-model",
        startedAtMillis: Date.now(),
        persisted: { head: candidate, changedPaths: ["fixture.txt"] },
      },
      codex: {
        id: turnId,
        processId: "fixture-process",
        token: "private-synthetic-token",
        configuration: {
          command: ["false"],
          cwd: "/workspace/repo",
          env: {},
          mode: "codex",
          port: 4500,
          nativePort: 4501,
        },
        threadParams: {},
        turnParams: {},
        resumeToken: null,
        deadlineAt: Date.now() + 120_000,
        threadId: "fixture-thread",
        turnId: "fixture-turn",
        previousTurnId: null,
        startDispatched: true,
        result: {
          resumeToken: "fixture-thread",
          terminalEventType: "turn.completed",
          lastAgentMessage: "fixture candidate",
          usage: null,
          stderrTail: "",
        },
      },
    };
  };
  const deliveriesFor = (id) => completion.deliveries.filter((item) => item.event.taskId === id);
  const launch = async (saved, mode = "accept", quiet = false) => {
    const id = saved.request.id;
    deliveryModes.set(id, mode);
    return { id, quiet, receipt: await completionPost("seed", { id, quiet, seed: saved }) };
  };
  const observe = (task) => completionPost("evidence", task);
  const settled = (task) =>
    eventually(
      () => observe(task),
      (value) =>
        ["succeeded", "failed", "cancelled"].includes(value.state.status) &&
        pending(value, "settled") === null,
      "Completion delivery did not settle",
    );
  const firstDelivery = (task) =>
    eventually(
      async () => deliveriesFor(task.id),
      (items) => items.length > 0,
      "No durable completion notification",
    );

  for (const mode of ["reject", "lost-reply", "interrupted"]) {
    const task = await launch(seed(), mode);
    const first = (await firstDelivery(task))[0];
    const retained = await observe(task);
    const metadataCount = completion.metadataReads.length;
    assert.equal(first.accepted, mode !== "reject");
    assert.equal(retained.state.status, "succeeded");
    assert.equal(pending(retained, "cleanup").runAt, retained.state.request.deadlineAtMillis);
    assert.deepEqual(retained.state.launchReceipt, task.receipt);
    deliveryModes.set(task.id, "accept");
    let reloadTimeout;
    try {
      await Promise.race([
        mf.setOptions(options),
        new Promise((_, reject) => {
          reloadTimeout = setTimeout(
            () => reject(new Error("Native reload did not finish")),
            10_000,
          );
        }),
      ]);
    } finally {
      clearTimeout(reloadTimeout);
      heldReplies.get(task.id)?.(new Response(null, { status: 503 }));
      heldReplies.delete(task.id);
    }
    assert.deepEqual(await completionPost("replay", task), task.receipt);
    const final = await settled(task);
    assert.deepEqual(
      final.state,
      retained.state,
      "Delivery retry cannot change terminal execution",
    );
    assert.equal(
      completion.metadataReads.length,
      metadataCount,
      "Delivery and duplicate create cannot replay candidate recovery",
    );
    const attempts = deliveriesFor(task.id);
    assert.ok(attempts.length >= 2);
    assert.ok(attempts.every((item) => JSON.stringify(item.event) === JSON.stringify(first.event)));
    assert.equal(attempts.at(-1).duplicate, mode !== "reject");
    completion.cases.push({ name: mode, taskId: task.id, attempts: attempts.length, final });
  }

  const blockedSeed = seed();
  blockedSeed.blockCompletion = true;
  const atomic = await launch(blockedSeed);
  const rolledBack = await eventually(
    () => observe(atomic),
    (value) => value.events.some((event) => event.method === "agent.recovering"),
    "Completion transaction did not fail",
  );
  assert.equal(rolledBack.state.status, "running");
  assert.ok(pending(rolledBack, "run"));
  assert.equal(pending(rolledBack, "settled"), null);
  assert.equal(deliveriesFor(atomic.id).length, 0);
  await mf.setOptions(options);
  await completionPost("allow", atomic);
  const atomicFinal = await settled(atomic);
  assert.ok(acceptedCompletions.has(atomic.id));
  completion.cases.push({ name: "atomic-terminal-and-delivery", rolledBack, final: atomicFinal });

  const cleanup = await launch(seed(), "reject");
  await firstDelivery(cleanup);
  const beforeCleanup = await observe(cleanup);
  const mixed = await completionPost("mixed", cleanup);
  assert.deepEqual(mixed, {
    exit: "Failure",
    reasons: ["Fail", "Die", "Interrupt"],
    originalFailure: true,
    originalDefect: true,
    originalInterrupt: true,
  });
  await completionPost("cancel", cleanup);
  const afterCleanup = await observe(cleanup);
  assert.equal(afterCleanup.state.status, "succeeded");
  assert.equal(afterCleanup.state.cancelled, true);
  assert.ok(pending(afterCleanup, "settled"), "Compute cleanup must retain pending completion");
  assert.deepEqual(afterCleanup.state.savedCheckpoint, beforeCleanup.state.savedCheckpoint);
  deliveryModes.set(cleanup.id, "accept");
  const cleanupFinal = await settled(cleanup);
  completion.cases.push({ name: "mixed-cause-and-cleanup", mixed, final: cleanupFinal });

  const quiet = await launch(seed(), "accept", true);
  const quietFinal = await settled(quiet);
  assert.equal(deliveriesFor(quiet.id).length, 0);
  completion.cases.push({ name: "no-hook", final: quietFinal });

  for (const [name, saved] of [
    ["commands", seed({ execution: "commands", source: { _tag: "None" } })],
    ["expired", seed({ deadlineAtMillis: Date.now() - 1 })],
    ["failed", seed()],
    ["cancelled", { ...seed(), cancelBeforeLaunch: true }],
    ["deadline", seed({ deadlineAtMillis: Date.now() + 1000 })],
  ]) {
    if (name === "failed") saved.codex.result.terminalEventType = "turn.failed";
    else if (name !== "deadline") {
      delete saved.pipeline;
      delete saved.codex;
    }
    if (name === "deadline") {
      saved.request.deadlineAtMillis = Date.now() + 500;
      metadataDelayMillis = 1000;
    }
    const task = await launch(saved);
    if (name !== "commands") await firstDelivery(task);
    const final = await settled(task);
    metadataDelayMillis = 0;
    assert.equal(
      final.state.status,
      name === "commands" ? "succeeded" : name === "failed" ? "failed" : "cancelled",
    );
    assert.equal(pending(final, "settled"), null);
    assert.equal(pending(final, "run"), null);
    assert.equal(deliveriesFor(task.id).length, name === "commands" ? 0 : 1);
    const delivered = deliveriesFor(task.id).length;
    await completionPost("cancel", task);
    await completionPost("cancel", task);
    await completionPost("replay", task);
    const repeated = await observe(task);
    assert.equal(
      pending(repeated, "settled"),
      null,
      "Repeated cleanup must not re-enroll settled tasks",
    );
    assert.equal(deliveriesFor(task.id).length, delivered);
    completion.cases.push({ name, final });
  }
  await mf.setOptions(options);
  assert.equal(deliveriesFor(quiet.id).length, 0);
  assert.deepEqual(blocked, []);
  evidence.passed = true;
  console.log(
    "Native terminal commit, rejection/lost-reply retries, reload, no replay and cleanup passed",
  );
} finally {
  for (const release of heldReplies.values()) release(new Response(null, { status: 503 }));
  await writeFile(join(directory, "results.json"), JSON.stringify(evidence, null, 2));
  await mf.dispose();
}
