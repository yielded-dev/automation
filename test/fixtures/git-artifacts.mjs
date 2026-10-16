import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";

/** Synthetic Artifacts metadata around real bare repositories and git http-backend. */
export async function gitArtifacts(directory) {
  const work = join(directory, "source");
  const repositories = join(directory, "repositories");
  await mkdir(work, { recursive: true });
  await mkdir(repositories);
  const git = (cwd, ...args) =>
    execFileSync("git", args, {
      cwd,
      encoding: "utf8",
      timeout: 30_000,
      env: {
        ...process.env,
        GIT_CONFIG_NOSYSTEM: "1",
        GIT_CONFIG_GLOBAL: "/dev/null",
        GIT_AUTHOR_DATE: "2026-10-01T00:00:00Z",
        GIT_COMMITTER_DATE: "2026-10-01T00:00:00Z",
      },
      stdio: ["pipe", "pipe", "pipe"],
    }).trim();
  git(work, "init", "--initial-branch", "main");
  git(work, "config", "user.name", "Fixture");
  git(work, "config", "user.email", "fixture@example.invalid");
  const recipe = {
    install: "printf 'install\\n' >> /workspace/fixture-install-count",
    installationInputs: [".fixture-setup.json", "install-input.txt"],
    start: [],
    services: [],
    tools: ["git"],
    requiredSecrets: [],
  };
  await writeFile(join(work, ".fixture-setup.json"), JSON.stringify(recipe));
  await writeFile(
    join(work, ".fixture-disabled-setup.json"),
    JSON.stringify({ ...recipe, requiredSecrets: ["FIXTURE_REQUIRED_SECRET"] }),
  );
  await writeFile(join(work, "install-input.txt"), "dependencies-v1\n");
  const commit = async (message, path = "source.txt") => {
    await writeFile(join(work, path), `${message}\n`);
    git(work, "add", ".");
    git(work, "commit", "-qm", message);
    return git(work, "rev-parse", "HEAD");
  };
  for (let index = 0; index < 5; index++) await commit(`history-${index}`);
  const base = await commit("prepared-base");
  const candidate = await commit("saved-candidate");
  git(work, "branch", "saved", candidate);
  const changed = await commit("dependencies-v2", "install-input.txt");
  git(work, "branch", "changed", changed);
  git(work, "checkout", "--orphan", "divergent");
  const divergent = await commit("unrelated-root");
  git(work, "checkout", "main");

  const metadata = new Map();
  const tokens = new Map();
  const operations = [];
  const transfers = [];
  const pathFor = (name) => {
    assert.match(name, /^[a-zA-Z0-9][a-zA-Z0-9_-]{0,100}$/);
    return join(repositories, `${name}.git`);
  };
  const add = (name, source, head, description = null, parent = null) => {
    const path = pathFor(name);
    git(repositories, "clone", "--bare", source, path);
    git(path, "symbolic-ref", "HEAD", "refs/heads/main");
    git(path, "update-ref", "refs/heads/main", head);
    git(path, "config", "http.receivepack", "true");
    const now = "2026-10-01T00:00:00Z";
    const info = {
      id: randomUUID(),
      name,
      description,
      defaultBranch: "main",
      createdAt: now,
      updatedAt: now,
      lastPushAt: now,
      source: parent,
      readOnly: false,
      remote: `https://fixture.invalid/git/fixture/${name}.git`,
    };
    metadata.set(name, info);
    return { repositoryName: name, repositoryId: info.id, commitSha: head };
  };
  const sources = {
    base: add("prepared-source", work, base),
    candidate: add("candidate-source", work, candidate),
    changed: add("changed-source", work, changed),
    publication: add("publication", work, base),
  };
  const readCommit = (name, hash) => {
    assert.match(hash, /^[0-9a-f]{40}$/);
    const parts = git(pathFor(name), "show", "-s", "--format=%H%n%T%n%P%n%B", hash).split("\n");
    return {
      hash: parts[0],
      treeHash: parts[1],
      parents: parts[2] ? parts[2].split(" ") : [],
      message: parts.slice(3).join("\n"),
      author: { name: "Fixture", email: "fixture@example.invalid" },
      committer: { name: "Fixture", email: "fixture@example.invalid" },
      authoredAt: 1790812800,
      committedAt: 1790812800,
    };
  };
  const control = async (request) => {
    const { operation, name, args } = await request.json();
    operations.push({
      operation,
      name,
      ...(operation === "log" ? { ref: args[0]?.ref ?? "main" } : {}),
    });
    if (operation === "create") {
      if (metadata.has(name)) return new Response(null, { status: 409 });
      const path = pathFor(name);
      git(repositories, "init", "--bare", "--initial-branch", "main", path);
      const now = "2026-10-01T00:00:00Z";
      const info = {
        id: randomUUID(),
        name,
        description: null,
        defaultBranch: "main",
        createdAt: now,
        updatedAt: now,
        lastPushAt: null,
        source: null,
        readOnly: false,
        remote: `https://fixture.invalid/git/fixture/${name}.git`,
      };
      metadata.set(name, info);
      return Response.json({ ...info, token: "unused", tokenExpiresAt: now });
    }
    const info = metadata.get(name);
    if (!info) return new Response(null, { status: 404 });
    let result;
    switch (operation) {
      case "info":
        result = info;
        break;
      case "readCommit": {
        try {
          result = readCommit(name, args[0]);
        } catch {
          return new Response(null, { status: 404 });
        }
        break;
      }
      case "log": {
        const { ref = "main" } = args[0] ?? {};
        assert.match(ref, /^[a-zA-Z0-9/_-]+$/);
        try {
          result = [readCommit(name, git(pathFor(name), "rev-parse", `refs/heads/${ref}`))];
        } catch {
          result = [];
        }
        break;
      }
      case "createToken": {
        const id = randomUUID();
        const scope = args[0] ?? "write";
        tokens.set(id, { name, scope });
        result = {
          id,
          plaintext: id,
          scope,
          expiresAt: new Date(Date.now() + 300_000).toISOString(),
        };
        break;
      }
      case "revokeToken":
        result = tokens.delete(args[0]);
        break;
      case "fork": {
        const [target, options] = args;
        const head = git(pathFor(name), "rev-parse", "main");
        add(target, pathFor(name), head, options.description, `artifacts:fixture/${name}`);
        result = {
          ...metadata.get(target),
          token: "unused",
          tokenExpiresAt: "2026-10-01T00:00:00Z",
        };
        break;
      }
      default:
        throw new Error(`Unsupported fixture operation: ${operation}`);
    }
    return Response.json(result);
  };
  const transport = async (request) => {
    const url = new URL(request.url);
    assert.equal(url.hostname, "fixture.invalid");
    const match =
      /^\/git\/fixture\/([a-zA-Z0-9_-]+)\.git\/(info\/refs|git-upload-pack|git-receive-pack)$/.exec(
        url.pathname,
      );
    assert.ok(match, "Only fixture Git transport is allowed");
    const [, name, operation] = match;
    const token = tokens.get(request.headers.get("authorization")?.replace(/^Bearer /, ""));
    if (token?.name !== name) {
      transfers.push({ name, operation, status: 401 });
      return new Response("Fixture repository authorization is missing", { status: 401 });
    }
    if (operation === "git-receive-pack" || url.searchParams.get("service") === "git-receive-pack")
      assert.equal(token.scope, "write");
    const body = Buffer.from(await request.arrayBuffer());
    const output = execFileSync("git", ["http-backend"], {
      input: body,
      timeout: 30_000,
      maxBuffer: 16 * 1024 * 1024,
      env: {
        ...process.env,
        GIT_CONFIG_NOSYSTEM: "1",
        GIT_CONFIG_GLOBAL: "/dev/null",
        GIT_PROJECT_ROOT: repositories,
        GIT_HTTP_EXPORT_ALL: "1",
        PATH_INFO: `/${name}.git/${operation}`,
        QUERY_STRING: url.search.slice(1),
        REQUEST_METHOD: request.method,
        CONTENT_TYPE: request.headers.get("content-type") ?? "",
        CONTENT_LENGTH: String(body.length),
        HTTP_GIT_PROTOCOL: request.headers.get("git-protocol") ?? "",
        REMOTE_USER: "fixture",
      },
    });
    const boundary = output.indexOf("\r\n\r\n");
    assert.ok(boundary > 0);
    const headers = new Headers();
    let status = 200;
    for (const line of output.subarray(0, boundary).toString().split("\r\n")) {
      const colon = line.indexOf(":");
      const key = line.slice(0, colon);
      const value = line.slice(colon + 1).trim();
      if (key.toLowerCase() === "status") status = Number(value.split(" ")[0]);
      else headers.set(key, value);
    }
    transfers.push({
      name,
      operation,
      service: url.searchParams.get("service"),
      depthOne: body.includes(Buffer.from("deepen 1")),
      bytes: output.length - boundary - 4,
      status,
    });
    return new Response(output.subarray(boundary + 4), { status, headers });
  };
  return {
    sources,
    base,
    candidate,
    changed,
    divergent,
    operations,
    transfers,
    tokens,
    control,
    transport,
    failedSetup: async (name = "setup-failure-source", failure = "install") => {
      git(work, "checkout", "--detach", base);
      const contents =
        failure === "invalid"
          ? "{"
          : JSON.stringify(failure === "install" ? { ...recipe, install: "exit 23" } : recipe);
      await writeFile(
        join(work, ".fixture-setup.json"),
        failure === "oversized" ? contents.padEnd(32 * 1024) : contents,
      );
      const head = await commit("terminal-setup-failure");
      git(work, "checkout", "main");
      return add(name, work, head);
    },
    pin: (name, sha) => git(pathFor(name), "update-ref", `refs/heads/repair-source/${sha}`, sha),
    moveMain: (name, sha) => git(pathFor(name), "update-ref", "refs/heads/main", sha),
    generation: (name, id) => {
      metadata.get(name).id = id;
    },
    head: (name) => git(pathFor(name), "rev-parse", "main"),
  };
}
