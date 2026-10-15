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
import { fileURLToPath } from "node:url";

// Run after the workspace build. Only packed files and copied public fixtures enter consumers.
const root = fileURLToPath(new URL("../", import.meta.url));
const args = process.argv.slice(process.argv[2] === "--" ? 3 : 2);
assert.ok(args.length <= 1, "Usage: node scripts/check-packages.mjs [local container image]");
const [image] = args;
assert.equal(process.env.MINIFLARE_WORKERD_PATH, undefined, "Use the pinned workerd binary");
const manifest = JSON.parse(await readFile(join(root, "package.json"), "utf8"));
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
      const version = manifest.devDependencies[name];
      assert.ok(version && !version.startsWith("workspace:"), `Missing tool pin: ${name}`);
      return [name, version];
    }),
  );
const tools = versions(
  "miniflare",
  "vite-plus",
  "typescript",
  "@effect/tsgo",
  "@cloudflare/workers-types",
  "@types/node",
);

async function consumer(name, dependencies, devDependencies, fixtures, absent) {
  const cwd = join(directory, name);
  await mkdir(cwd);
  const tarball = join(directory, `${name}.tgz`);
  const source = join(root, "packages", name);
  await stat(join(source, "dist", "index.mjs"));
  run(source, "bun", ["pm", "pack", "--ignore-scripts", "--filename", tarball, "--quiet"]);
  await writeFile(
    join(cwd, "package.json"),
    JSON.stringify({
      private: true,
      type: "module",
      packageManager: manifest.packageManager,
      scripts: { typecheck: manifest.scripts.typecheck },
      dependencies: { [`@yielded/${name}`]: tarball, ...versions("effect"), ...dependencies },
      devDependencies: { ...tools, ...devDependencies },
      overrides: manifest.overrides,
    }),
  );
  for (const fixture of fixtures) {
    const target = join(cwd, "test/e2e", fixture);
    await mkdir(dirname(target), { recursive: true });
    await copyFile(join(root, "test/e2e", fixture), target);
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
      },
      include: ["test/e2e/**/*.ts"],
    }),
  );
  run(cwd, "vp", ["install", "--ignore-scripts"]);
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

console.log(`Independent package consumers: ${directory}`);
try {
  const automation = await consumer(
    "automation",
    {},
    {},
    ["github.mjs", "github.worker.ts"],
    ["@yielded/sandbox", "effect-cf", "@cloudflare/sandbox"],
  );
  run(automation.cwd, process.execPath, ["test/e2e/github.mjs"]);
  console.log("automation: native GitHub fixture passed without sandbox or effect-cf");

  const sandbox = await consumer(
    "sandbox",
    versions("effect-cf"),
    versions("@effect/sql-d1", "@effect/sql-sqlite-do"),
    [
      "git-history.mjs",
      "git-history.worker.ts",
      "fixtures/artifacts.worker.ts",
      "fixtures/git-artifacts.mjs",
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
    run(sandbox.cwd, process.execPath, ["test/e2e/git-history.mjs", image, "1", "fork"]);
    console.log("sandbox: native Git-history fork fixture passed without automation");
  } else {
    await writeFile(
      join(sandbox.cwd, "bundle.mjs"),
      `import { build } from "vite-plus/pack";
await build({
  cwd: process.cwd(),
  config: false,
  entry: { "git-history-worker": "test/e2e/git-history.worker.ts" },
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
