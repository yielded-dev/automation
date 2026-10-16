import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { InMemory } from "@yielded/agent";
import { Config, Effect, Layer, Schema } from "effect";
import { FetchHttpClient } from "effect/http";
import { Log, LogLevel, Miniflare, convertV4MiniflareOptions } from "miniflare";
import { build } from "vite-plus/pack";
import { gitArtifacts } from "../../test/fixtures/git-artifacts.mjs";
import { CodingError, CodingResult, CodingTask, runAssistant } from "./agent.ts";

const [image] = process.argv.slice(process.argv[2] === "--" ? 3 : 2);
assert.ok(image, "Usage: vp run -F @yielded/example-agent-sandbox live -- <local image>");
assert.ok(
  process.env.OPENAI_API_KEY,
  "Provide OPENAI_API_KEY to run this paid, real-provider example",
);
const model = process.env.OPENAI_MODEL;
assert.ok(model, "Set OPENAI_MODEL to a model available to your provider account");
const root = fileURLToPath(new URL("./", import.meta.url));
const directory = await mkdtemp(join(tmpdir(), "agent-sandbox-"));
const runName = `agent-sandbox-${randomUUID()}`;
const require = createRequire(import.meta.url);
const execute = (cwd, file, args) =>
  execFileSync(file, args, {
    cwd,
    encoding: "utf8",
    timeout: 30_000,
    stdio: ["pipe", "pipe", "pipe"],
    env: { ...process.env, GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null" },
  }).trim();
const dockerHost =
  process.env.DOCKER_HOST ??
  execute(root, "docker", ["context", "inspect", "--format", "{{.Endpoints.docker.Host}}"]);
assert.ok(dockerHost.startsWith("unix://"), "Use a local Docker Unix socket");
const docker = (...args) => execute(root, "docker", ["--host", dockerHost, ...args]);
const imageId = docker("image", "inspect", image, "--format", "{{.Id}}");
const proxyId = docker(
  "image",
  "inspect",
  "cloudflare/proxy-everything:3cb1195@sha256:0ef6716c52430096900b150d84a3302057d6cd2319dae7987128c85d0733e3c8",
  "--format",
  "{{.Id}}",
);
const evidence = {
  passed: false,
  directory,
  runName,
  imageId,
  node: process.version,
  model,
  sandboxEntry: require.resolve("@yielded/sandbox"),
  agentVersion: JSON.parse(
    await readFile(join(dirname(require.resolve("@yielded/agent")), "../package.json"), "utf8"),
  ).version,
  scope:
    "Real Yielded Agent and Codex provider calls; native workerd, SQLite Durable Objects, Docker and Git. Artifacts metadata uses the local fixture. No hosted deployment or application adoption.",
  toolCalls: [],
  providerRequests: [],
  events: [],
  failures: [],
};
let mf;
const snapshots = new Set();
console.log(`Live agent evidence: ${directory}`);
const rpc = async (path, input) => {
  const response = await mf.dispatchFetch(`http://localhost${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(input),
    signal: AbortSignal.timeout(300_000),
  });
  const text = await response.text();
  assert.equal(response.status, 200, text);
  return JSON.parse(text);
};
try {
  const fixture = await gitArtifacts(directory);
  const sourceDir = join(directory, "source");
  const seed = join(directory, "repositories", "prepared-source.git");
  const git = (...args) => execute(sourceDir, "git", args);
  git("checkout", "--detach", fixture.base);
  await writeFile(
    join(sourceDir, ".fixture-setup.json"),
    JSON.stringify({
      install: "true",
      installationInputs: [".fixture-setup.json"],
      start: [],
      services: [],
      tools: ["node", "git"],
      requiredSecrets: [],
    }),
  );
  await writeFile(
    join(sourceDir, "slugify.mjs"),
    'export const slugify = (input) => input.toLowerCase().replaceAll(" ", "-");\n',
  );
  await writeFile(
    join(sourceDir, "slugify.test.mjs"),
    `import assert from "node:assert/strict";
import { test } from "node:test";
import { slugify } from "./slugify.mjs";
test("normalizes URL slugs", () => {
  assert.equal(slugify("  Hello, WORLD!  "), "hello-world");
  assert.equal(slugify("déjà vu"), "deja-vu");
  assert.equal(slugify("a\\tb  c"), "a-b-c");
  assert.equal(slugify("---"), "");
});
`,
  );
  assert.throws(() => execute(sourceDir, process.execPath, ["--test", "slugify.test.mjs"]), {
    status: 1,
  });
  evidence.originalTestFailed = true;
  git("add", ".");
  git("commit", "-qm", "Add the standalone coding task");
  const base = git("rev-parse", "HEAD");
  git("push", seed, `${base}:refs/heads/main`);
  const source = { ...fixture.sources.base, commitSha: base };
  evidence.base = base;
  const bundle = join(directory, "bundle");
  await build({
    cwd: root,
    config: false,
    entry: { worker: "worker.ts" },
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
  const configuration = convertV4MiniflareOptions({
    name: runName,
    scriptPath: join(bundle, "worker.mjs"),
    modulesRoot: bundle,
    modules: true,
    compatibilityDate: "2026-09-30",
    compatibilityFlags: ["nodejs_compat"],
    unsafeObservability: true,
    cf: false,
    telemetry: { enabled: false },
    log: new Log(LogLevel.WARN),
    handleStructuredLogs({ message, level }) {
      if (message.startsWith("CODING_EVENT ")) evidence.events.push(JSON.parse(message.slice(13)));
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
      CODING: { className: "CodingSession", useSQLite: true },
    },
    bindings: {
      SANDBOX_MAX_INSTANCES: "2",
      SESSION_ID: runName,
      MODEL: model,
      SOURCE: JSON.stringify(source),
      OPENAI_API_KEY: process.env.OPENAI_API_KEY,
    },
    serviceBindings: { ARTIFACTS: fixture.control },
    outboundService: async (request) => {
      if (new URL(request.url).hostname !== "api.openai.com") return fixture.transport(request);
      const response = await fetch(request.url, {
        method: request.method,
        headers: request.headers,
        body: request.body,
        duplex: "half",
        redirect: "manual",
      });
      evidence.providerRequests.push({
        path: new URL(request.url).pathname,
        status: response.status,
        requestId: response.headers.get("x-request-id"),
      });
      return response;
    },
    resourcePersistencePath: join(directory, "state"),
  });
  mf = new Miniflare(configuration);
  const tools = Layer.succeed(CodingTask, {
    run: (instructions) =>
      Effect.tryPromise({
        try: async () => {
          evidence.toolCalls.push({ instructions });
          const result = await rpc("/code", { instructions });
          evidence.toolCalls.at(-1).result = result;
          return Schema.decodeUnknownSync(CodingResult)(result);
        },
        catch: (cause) =>
          new CodingError({
            message: "The native coding task failed; inspect the local worker evidence",
            cause,
          }),
      }),
  });
  const provider = OpenAiClient.layerConfig({ apiKey: Config.Redacted("OPENAI_API_KEY") }).pipe(
    Layer.provide(FetchHttpClient.layer),
  );
  const app = Layer.mergeAll(
    tools,
    InMemory.layer,
    OpenAiLanguageModel.model(model, { reasoning: { effort: "low" } }),
  ).pipe(Layer.provide(provider));
  evidence.agent = await Effect.runPromise(
    runAssistant(
      "Fix slugify.mjs in the authorized repository. Run node --test slugify.test.mjs, correct the implementation so every assertion passes, and rerun the test. Preserve the test and setup files. Report the saved commit.",
    ).pipe(Effect.provide(app)),
  );
  assert.equal(
    evidence.toolCalls.length,
    1,
    "The real outer agent must invoke the coding tool once",
  );
  const accepted = evidence.toolCalls[0];
  assert.ok(accepted.result);
  assert.notEqual(accepted.result.head, base);
  assert.deepEqual(accepted.result.changedPaths, ["slugify.mjs"]);
  assert.ok(
    evidence.providerRequests.some(
      ({ path, status }) => path.includes("responses") && status === 200,
    ),
    "The native Codex harness must call the real provider",
  );
  const commands = evidence.events
    .filter(({ type, event }) => type === "codex.event" && event.type === "item.completed")
    .map(({ event }) => event.item)
    .filter(
      (item) =>
        item?.type === "command_execution" && item.command.includes("node --test slugify.test.mjs"),
    );
  assert.ok(
    commands.some((item) => item.exit_code === 1),
    "Codex must observe the failing test",
  );
  assert.ok(
    commands.some((item) => item.exit_code === 0),
    "Codex must rerun the test successfully",
  );
  assert.equal(evidence.agent.output.head, accepted.result.head);
  const candidate = join(directory, "candidate");
  execute(directory, "git", [
    "clone",
    join(directory, "repositories", `${accepted.result.forkName}.git`),
    candidate,
  ]);
  execute(candidate, "git", ["checkout", "--detach", accepted.result.head]);
  assert.equal(
    execute(candidate, "git", ["diff", "--name-only", base, accepted.result.head]),
    "slugify.mjs",
  );
  evidence.validation = execute(candidate, process.execPath, ["--test", "slugify.test.mjs"]);
  evidence.code = await readFile(join(candidate, "slugify.mjs"), "utf8");
  evidence.baseCode = git("show", `${base}:slugify.mjs`);
  assert.equal(
    fixture.head(source.repositoryName),
    base,
    "The authorized source must stay unchanged",
  );
  const stopped = await rpc("/stop", {});
  if (stopped.snapshot) snapshots.add(stopped.snapshot);
  await mf.dispose();
  mf = new Miniflare(configuration);
  const beforeReplay = {
    requests: evidence.providerRequests.length,
    events: evidence.events.length,
  };
  evidence.replayed = await rpc("/code", { instructions: accepted.instructions });
  assert.deepEqual(evidence.replayed, accepted.result);
  assert.equal(
    evidence.providerRequests.length,
    beforeReplay.requests,
    "Completed replay must not call the provider",
  );
  assert.ok(
    evidence.events.slice(beforeReplay.events).every((event) => event.type !== "codex.event"),
  );
  evidence.replayedWithoutModel = true;
} catch (error) {
  evidence.failures.push(String(error.stack ?? error));
} finally {
  if (mf) {
    try {
      const stopped = await rpc("/stop", {});
      if (stopped.snapshot) snapshots.add(stopped.snapshot);
    } catch (error) {
      evidence.failures.push(`Cleanup: ${error}`);
    }
    try {
      await mf.dispose();
    } catch (error) {
      evidence.failures.push(`Dispose: ${error}`);
    }
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
  } catch (error) {
    evidence.failures.push(`Resource cleanup: ${error}`);
  }
  evidence.passed = evidence.failures.length === 0;
  await writeFile(join(directory, "results.json"), JSON.stringify(evidence, null, 2));
}
assert.equal(evidence.passed, true, evidence.failures.join("\n"));
console.log(
  "LIVE_AGENT_PASS: real tool invocation, native coding, independently validated commit, retained replay",
);
