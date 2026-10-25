import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import {
  copyFile,
  mkdir,
  mkdtemp,
  readFile,
  realpath,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { dirname, join, sep } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

// Run after the workspace build. Consumers cannot resolve this checkout's dependencies.
const root = fileURLToPath(new URL("../", import.meta.url));
const args = process.argv.slice(process.argv[2] === "--" ? 3 : 2);
assert.ok(args.length <= 1, "Usage: node scripts/check-packages.mjs [local container image]");
const [image] = args;
assert.equal(process.env.MINIFLARE_WORKERD_PATH, undefined, "Use the pinned workerd binary");
const manifest = JSON.parse(await readFile(join(root, "package.json"), "utf8"));
const { compilerOptions } = JSON.parse(await readFile(join(root, "tsconfig.json"), "utf8"));
const directory = await realpath(await mkdtemp(join(tmpdir(), "package-consumers-")));
assert.ok(
  !directory.startsWith(`${await realpath(root)}${sep}`),
  "Consumers must be outside the workspace",
);
const environment = { ...process.env };
delete environment.NODE_PATH;
const run = (cwd, command, args) =>
  execFileSync(command, args, { cwd, env: environment, stdio: "inherit" });
const versions = (...names) =>
  Object.fromEntries(
    names.map((name) => {
      const version = manifest.dependencies?.[name] ?? manifest.devDependencies[name];
      assert.ok(version && !version.startsWith("workspace:"), `Missing tool pin: ${name}`);
      return [name, version];
    }),
  );
const tools = versions(
  "miniflare",
  "vite-plus",
  "typescript",
  "@yielded/effect-tsrs",
  "@cloudflare/workers-types",
  "@types/node",
);

async function consumer(name, dependencies, devDependencies, fixtures, absent, gitSource) {
  const cwd = join(directory, `${name}${gitSource ? "-git" : ""}`);
  await mkdir(cwd);
  let dependency = gitSource;
  const source = join(root, "packages", name);
  if (!dependency) {
    dependency = join(directory, `${name}.tgz`);
    await stat(join(source, "dist", "index.mjs"));
    run(source, "bun", ["pm", "pack", "--ignore-scripts", "--filename", dependency, "--quiet"]);
  }
  await writeFile(
    join(cwd, "package.json"),
    JSON.stringify({
      private: true,
      type: "module",
      packageManager: manifest.packageManager,
      scripts: { typecheck: manifest.scripts.typecheck },
      dependencies: { [`@yielded/${name}`]: dependency, ...versions("effect"), ...dependencies },
      devDependencies: { ...tools, ...devDependencies },
      overrides: manifest.overrides,
      trustedDependencies: gitSource ? [`@yielded/${name}`] : [],
    }),
  );
  for (const fixture of fixtures) {
    const target = join(cwd, fixture);
    await mkdir(dirname(target), { recursive: true });
    await copyFile(join(root, fixture), target);
  }
  await writeFile(
    join(cwd, "tsconfig.json"),
    JSON.stringify({
      compilerOptions: {
        target: "ES2022",
        lib: ["ES2022"],
        module: "ESNext",
        moduleResolution: "Bundler",
        strict: true,
        noEmit: true,
        skipLibCheck: true,
        types: ["@cloudflare/workers-types", "node"],
        plugins: compilerOptions.plugins,
      },
      include: ["packages/*/test/e2e/**/*.ts", "test/fixtures/**/*.ts"],
    }),
  );
  run(cwd, "vp", gitSource ? ["install"] : ["install", "--ignore-scripts"]);
  const require = createRequire(join(cwd, "package.json"));
  const entry = await realpath(require.resolve(`@yielded/${name}`));
  assert.ok(entry.startsWith(`${cwd}${sep}`), "Package must resolve inside its isolated consumer");
  for (const resolve of [require, createRequire(entry)]) {
    for (const dependency of absent) {
      assert.throws(() => resolve.resolve(dependency), { code: "MODULE_NOT_FOUND" });
    }
  }
  await writeFile(join(cwd, "import-root.mjs"), `import "@yielded/${name}";\n`);
  run(cwd, process.execPath, ["import-root.mjs"]);
  run(cwd, "vp", ["run", "typecheck"]);
  console.log(`${name}: isolated root import and fixture typecheck passed`);
  return { cwd, require };
}

async function gitSnapshot() {
  const source = join(directory, "git-source");
  await mkdir(source);
  const files = execFileSync("git", ["ls-files", "-z"], { cwd: root, encoding: "utf8" })
    .split("\0")
    .filter(Boolean);
  for (const file of files) {
    const destination = join(source, file);
    await mkdir(dirname(destination), { recursive: true });
    await copyFile(join(root, file), destination);
  }
  await assert.rejects(stat(join(source, "packages/automation/dist")), { code: "ENOENT" });
  run(source, "git", ["init", "--quiet"]);
  run(source, "git", ["add", "."]);
  run(source, "git", [
    "-c",
    "user.name=Package fixture",
    "-c",
    "user.email=fixture@example.invalid",
    "-c",
    "commit.gpgsign=false",
    "commit",
    "--quiet",
    "-m",
    "Source package fixture",
  ]);
  const revision = execFileSync("git", ["rev-parse", "HEAD"], {
    cwd: source,
    encoding: "utf8",
  }).trim();
  return `git+${pathToFileURL(source).href}#${revision}`;
}

console.log(`Independent package consumers: ${directory}`);
try {
  const automation = await consumer(
    "automation",
    {},
    {},
    ["packages/automation/test/e2e/github.mjs", "packages/automation/test/e2e/github.worker.ts"],
    ["@yielded/sandbox", "effect-cf", "@cloudflare/sandbox", "drizzle-orm"],
  );
  run(automation.cwd, process.execPath, ["packages/automation/test/e2e/github.mjs"]);
  console.log("automation: native GitHub fixture passed without sandbox or effect-cf");

  const automationManifest = JSON.parse(
    await readFile(join(root, "packages/automation/package.json"), "utf8"),
  );
  const prefix = (value) =>
    typeof value === "string"
      ? `./packages/automation/${value.slice(2)}`
      : Object.fromEntries(Object.entries(value).map(([key, target]) => [key, prefix(target)]));
  assert.deepEqual(
    manifest.exports,
    Object.fromEntries(
      Object.entries(automationManifest.exports).map(([key, target]) => [key, prefix(target)]),
    ),
  );
  assert.deepEqual(manifest.peerDependencies, automationManifest.peerDependencies);
  assert.deepEqual(manifest.peerDependenciesMeta, automationManifest.peerDependenciesMeta);
  for (const [name, version] of Object.entries(automationManifest.dependencies)) {
    assert.equal(manifest.dependencies[name], version);
  }
  const gitAutomation = await consumer(
    "automation",
    {},
    {},
    ["packages/automation/test/e2e/github.mjs", "packages/automation/test/e2e/github.worker.ts"],
    ["@yielded/sandbox", "effect-cf", "@cloudflare/sandbox", "drizzle-orm"],
    await gitSnapshot(),
  );
  run(gitAutomation.cwd, process.execPath, ["packages/automation/test/e2e/github.mjs"]);
  console.log("automation: source Git pin prepared and passed the native GitHub fixture");

  const sandbox = await consumer(
    "sandbox",
    versions("effect-cf"),
    versions("@effect/sql-d1", "@effect/sql-sqlite-do"),
    [
      "packages/sandbox/test/e2e/git-history.mjs",
      "packages/sandbox/test/e2e/git-history.worker.ts",
      "test/fixtures/artifacts.worker.ts",
      "test/fixtures/git-artifacts.mjs",
    ],
    ["@yielded/automation"],
  );
  for (const asset of [
    "Dockerfile",
    "playwright-cli.config.json",
    "scripts/task-preview.sh",
    "scripts/repository-secret-scan.cjs",
  ]) {
    const path = await realpath(sandbox.require.resolve(`@yielded/sandbox/container/${asset}`));
    assert.ok(
      path.startsWith(`${sandbox.cwd}${sep}`),
      "Container asset must come from the tarball",
    );
    assert.ok((await stat(path)).size > 0, `Empty container asset: ${asset}`);
  }
  if (image) {
    run(sandbox.cwd, process.execPath, [
      "packages/sandbox/test/e2e/git-history.mjs",
      image,
      "1",
      "fork",
    ]);
    console.log("sandbox: native Git-history fork fixture passed without automation");
  } else {
    await writeFile(
      join(sandbox.cwd, "bundle.mjs"),
      `import { build } from "vite-plus/pack";
await build({
  cwd: process.cwd(),
  config: false,
  entry: { "git-history-worker": "packages/sandbox/test/e2e/git-history.worker.ts" },
  outDir: "bundle",
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
`,
    );
    run(sandbox.cwd, process.execPath, ["bundle.mjs"]);
    assert.ok((await stat(join(sandbox.cwd, "bundle/git-history-worker.mjs"))).size > 0);
    console.log(
      "sandbox: neutral Git-history worker bundled without automation; container assets resolved",
    );
  }
} catch (error) {
  console.error(`Failed package consumers retained at ${directory}`);
  throw error;
}
await rm(directory, { recursive: true, force: true });
