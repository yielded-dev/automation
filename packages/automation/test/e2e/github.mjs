import assert from "node:assert/strict";
import { createHmac, generateKeyPairSync, randomUUID, verify } from "node:crypto";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Miniflare, Log, LogLevel, convertV4MiniflareOptions } from "miniflare";
import { build } from "vite-plus/pack";

const root = fileURLToPath(new URL("../../", import.meta.url));
const directory = await mkdtemp(join(tmpdir(), "repairs-github-"));
const evidence = {
  scope:
    "Real workerd HTTP intake, HMAC, RSA App auth and GitHub client. Scripted foreign GitHub API; no live provider, action execution, merge or deployment.",
  cases: [],
  passed: false,
};
console.log(`Native Github evidence: ${directory}`);
await build({
  cwd: root,
  config: false,
  entry: { github: "test/e2e/github.worker.ts" },
  outDir: directory,
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
const { privateKey, publicKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
const secret = randomUUID();
const head = "a".repeat(40);
let conversions = 0;
let limited = false;
let verifiedJwt = false;
const calls = [];
const failures = [];
const outboundService = async (request) => {
  try {
    const url = new URL(request.url);
    calls.push({ path: url.pathname, method: request.method });
    assert.equal(url.hostname, "api.github.com");
    if (url.pathname.startsWith("/app-manifests/")) {
      conversions++;
      return new Response(null, { status: 503 });
    }
    if (url.pathname === "/app/installations/205/access_tokens") {
      const jwt = request.headers.get("authorization").replace(/^Bearer /i, "");
      const [header, payload, signature] = jwt.split(".");
      assert.equal(
        verify(
          "RSA-SHA256",
          Buffer.from(`${header}.${payload}`),
          publicKey,
          Buffer.from(signature, "base64url"),
        ),
        true,
      );
      assert.equal(JSON.parse(Buffer.from(payload, "base64url")).iss, "104");
      verifiedJwt = true;
      return Response.json({
        token: "synthetic-token",
        expires_at: new Date(Date.now() + 3_600_000).toISOString(),
      });
    }
    assert.equal(request.headers.get("authorization"), "Bearer synthetic-token");
    if (url.pathname === "/repos/fixture/project/pulls/17") {
      if (limited) return new Response(null, { status: 429, headers: { "retry-after": "30" } });
      return Response.json({
        number: 17,
        updated_at: new Date().toISOString(),
        state: "open",
        draft: false,
        merged: false,
        mergeable: true,
        mergeable_state: "clean",
        title: "Fixture",
        body: "Fixture",
        additions: 1,
        deletions: 0,
        changed_files: 1,
        user: { login: "fixture[bot]" },
        head: { sha: head, ref: "repair/fixture", repo: { id: 306 } },
        base: { sha: head, ref: "main", repo: { id: 306 } },
      });
    }
    throw new Error(`Unexpected GitHub request ${request.method} ${url.pathname}`);
  } catch (error) {
    failures.push(error);
    return new Response(null, { status: 500 });
  }
};
const mf = new Miniflare(
  convertV4MiniflareOptions({
    cf: false,
    telemetry: { enabled: false },
    log: new Log(LogLevel.ERROR),
    workers: [
      {
        name: "github-proof",
        modules: true,
        scriptPath: join(directory, "github.mjs"),
        modulesRoot: directory,
        compatibilityDate: "2026-10-05",
        compatibilityFlags: ["nodejs_compat"],
        bindings: {
          WEBHOOK_SECRET: secret,
          APP_PRIVATE_KEY: privateKey.export({ type: "pkcs8", format: "der" }).toString("base64"),
        },
        outboundService,
      },
    ],
  }),
);
const send = (payload, { event = "push", signature, method = "POST", raw } = {}) => {
  const body = raw ?? JSON.stringify(payload);
  return mf.dispatchFetch("http://fixture/webhook", {
    method,
    ...(method === "GET" ? {} : { body }),
    headers: {
      "x-github-event": event,
      "x-hub-signature-256":
        signature ?? `sha256=${createHmac("sha256", secret).update(body).digest("hex")}`,
    },
  });
};
try {
  const manifest = await (await mf.dispatchFetch("http://fixture/manifest")).json();
  assert.equal(manifest.default_permissions.checks, "read");
  assert.equal(manifest.default_permissions.statuses, "read");
  assert.equal(manifest.default_permissions.workflows, undefined);
  assert.equal(manifest.request_oauth_on_install, false);
  evidence.cases.push(
    "App manifest requests explicit publication permissions, checks/status read and no workflow grant by default",
  );
  const push = {
    repository: { id: 306, full_name: "fixture/project" },
    installation: { id: 205 },
    ref: "refs/heads/main",
    deleted: false,
  };
  const valid = await send(push);
  assert.equal(valid.status, 200);
  assert.deepEqual((await valid.json()).event, {
    _tag: "push",
    repository: "fixture/project",
    repositoryId: 306,
    installationId: 205,
    ref: "refs/heads/main",
  });
  assert.equal((await send(push, { signature: `sha256=${"0".repeat(64)}` })).status, 401);
  assert.equal((await send(push, { method: "GET" })).status, 405);
  assert.equal((await send(push, { raw: "x".repeat(1024 * 1024 + 1) })).status, 413);
  assert.equal((await send(push, { raw: "not JSON" })).status, 400);
  assert.equal((await (await send({ ...push, deleted: true })).json()).event, null);
  assert.equal((await (await send(push, { event: "unknown" })).json()).event, null);
  const comment = {
    ...push,
    action: "created",
    issue: { number: 17, pull_request: {} },
    comment: {
      id: 1,
      body: "Untrusted feedback",
      html_url: "https://github.com/fixture/project/pull/17#issuecomment-1",
      user: { login: "fixture[bot]", type: "Bot" },
    },
  };
  assert.equal((await (await send(comment, { event: "issue_comment" })).json()).event, null);
  assert.equal(
    calls.length,
    0,
    "Webhook intake must not acquire provider credentials or fetch live state",
  );
  evidence.cases.push(
    "Signed bounded native intake shares the established event decoder, rejects bad signatures/malformed input, and ignores deleted pushes and bot comments without external calls",
  );
  const pr = await mf.dispatchFetch("http://fixture/pr");
  assert.equal(pr.status, 200);
  assert.equal((await pr.json()).head.sha, head);
  assert.equal(verifiedJwt, true);
  limited = true;
  const before = calls.length;
  assert.equal((await mf.dispatchFetch("http://fixture/pr")).status, 503);
  assert.equal(calls.slice(before).filter((call) => call.path.endsWith("/pulls/17")).length, 1);
  evidence.cases.push(
    "Native App auth signs a valid RSA JWT; the existing GitHub client reads the exact PR and leaves provider rate-limit recovery to the caller",
  );
  assert.equal((await mf.dispatchFetch("http://fixture/register")).status, 503);
  assert.equal(conversions, 1);
  evidence.cases.push("Ambiguous one-use manifest conversion is not automatically retried");
  assert.deepEqual(failures, []);
  evidence.passed = true;
} finally {
  await mf.dispose();
  await writeFile(join(directory, "results.json"), JSON.stringify(evidence, null, 2));
}
console.log(JSON.stringify(evidence));
