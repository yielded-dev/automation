import { Cause, Clock, Effect, Layer, Schema, Tracer } from "effect";
import { Worker, WorkerEnvironment } from "effect-cf";
import {
  DirectoryBackupGateway,
  makeSandboxAgentContainer,
  makeSandboxAgentOutbound,
} from "../../src/cloudflare/Container";
import {
  CheckpointInput,
  NativeSandboxNamespace,
  SandboxCheckpoint,
  makeNativeSandbox,
} from "../../src/cloudflare/NativeSandbox";
import { failureDiagnostic } from "../../src/observability/FailureDiagnostic";

const traceLayer = Layer.succeed(
  Tracer.Tracer,
  Tracer.make({
    span(options) {
      const span = new Tracer.NativeSpan(options);
      const end = span.end.bind(span);
      span.end = (time, exit) => {
        end(time, exit);
        if (/SandboxContainer\.(nativeSnapshot|nativeStart|ensureSupervisor)/.test(options.name))
          console.log(
            "CHECKPOINT_SPAN " +
              JSON.stringify({
                name: options.name,
                durationMs: Number((time - options.startTime) / 1_000_000n),
                outcome: exit._tag,
                attributes: Object.fromEntries(
                  [...span.attributes].filter(([key]) =>
                    /^sandbox\.(snapshot|container)\./.test(key),
                  ),
                ),
              }),
          );
      };
      return span;
    },
  }),
);

const containerOptions = {
  namespaceBinding: "SANDBOX",
  backupBucketBinding: "BACKUPS",
  allowedHosts: () => [],
  eventLayer: traceLayer,
};
export class Sandbox extends makeSandboxAgentContainer(containerOptions) {
  constructor(
    readonly fixtureState: globalThis.DurableObjectState,
    env: Cloudflare.Env,
  ) {
    super(fixtureState, env);
  }
  async seedLegacy(checkpoint: SandboxCheckpoint, snapshotAllowed: boolean) {
    const { name: _name, ...legacyCheckpoint } = checkpoint;
    const repository = {
      setupFingerprint: "legacy-setup",
      head: "a".repeat(40),
      materializationId: "legacy-materialization",
    };
    const record = {
      input: {
        captureId: "legacy-capture",
        instanceId: checkpoint.instanceId,
        ...repository,
        snapshotAllowed,
      },
      result: snapshotAllowed ? { ...legacyCheckpoint, ...repository } : null,
    };
    await this.fixtureState.storage.put("workspace-checkpoint", record);
    return record;
  }
}
export class SandboxOutbound extends makeSandboxAgentOutbound(containerOptions) {}
export { DirectoryBackupGateway };

const Name = Schema.String.check(Schema.isPattern(/^checkpoint-proof-[a-z0-9-]+$/));
const Backup = Schema.Struct({
  id: Schema.String,
  dir: Schema.Literal("/workspace/checkpoint-proof"),
  size: Schema.Natural,
  sha256: Schema.String,
  format: Schema.Literal("tar+zstd/1"),
});
const Input = Schema.Union([
  Schema.Struct({
    name: Name,
    mode: Schema.Literal("seed-legacy"),
    checkpoint: SandboxCheckpoint,
    snapshotAllowed: Schema.Boolean,
  }),
  Schema.Struct({ name: Name, mode: Schema.Literal("seed"), representative: Schema.Boolean }),
  Schema.Struct({ name: Name, mode: Schema.Literal("version"), version: Schema.Natural }),
  Schema.Struct({ name: Name, mode: Schema.Literal("checkpoint"), input: CheckpointInput }),
  Schema.Struct({ name: Name, mode: Schema.Literal("restore"), checkpoint: SandboxCheckpoint }),
  Schema.Struct({ name: Name, mode: Schema.Literal("restore-directory"), backup: Backup }),
  Schema.Struct({ name: Name, mode: Schema.Literal("delete-backup"), backup: Backup }),
  Schema.Struct({
    name: Name,
    mode: Schema.Literals(["backup", "inspect", "read-files", "describe", "park", "destroy"]),
  }),
]);

const directory = "/workspace/checkpoint-proof";
const firstFile = `${directory}/src/version.js`;
const heartbeatPath = "/tmp/checkpoint-proof-heartbeat.json";
const imports = `
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const root = ${JSON.stringify(directory)};
`;
const summary = `
const hash = crypto.createHash('sha256');
let bytes = 0, files = 0, links = 0;
function walk(dir) {
  for (const item of fs.readdirSync(dir).sort()) {
    const p = path.join(dir, item), relative = path.relative(root, p), stat = fs.lstatSync(p);
    if (stat.isDirectory()) { walk(p); continue; }
    if (relative === 'src/version.js') continue;
    hash.update(relative + '\\0');
    if (stat.isSymbolicLink()) { hash.update('link:' + fs.readlinkSync(p)); links++; }
    else { const data = fs.readFileSync(p); hash.update(data); bytes += data.length; files++; }
  }
}
walk(root);
console.log(JSON.stringify({bytes, files, links, sha256:hash.digest('hex'),
  version:fs.readFileSync(path.join(root, 'src/version.js'), 'utf8'),
  git:fs.existsSync(path.join(root,'.git')), dependencies:fs.existsSync(path.join(root,'node_modules/npm/package.json'))}));
`;
const heartbeat = `
const fs = require('node:fs');
const p = ${JSON.stringify(heartbeatPath)};
const startedAt = Date.now(); let lastAt=startedAt, ticks=0, maxGapMs=0; const gaps=[];
function tick() {
  const now=Date.now(), gap=now-lastAt;
  if(gap>50){gaps.push({from:lastAt,to:now,millis:gap});if(gaps.length>20)gaps.shift();}
  maxGapMs=Math.max(maxGapMs,gap); lastAt=now;
  fs.writeFileSync(p+'.tmp',JSON.stringify({pid:process.pid,startedAt,lastAt,ticks:++ticks,maxGapMs,gaps}));
  fs.renameSync(p+'.tmp',p);
}
tick(); setInterval(tick,20);
`;
const heartbeatRead = `
const fs = require('node:fs'); const p=${JSON.stringify(heartbeatPath)};
if (!fs.existsSync(p)) console.log(JSON.stringify({present:false}));
else { const value=JSON.parse(fs.readFileSync(p,'utf8')); let alive=false;
  try { alive=fs.readFileSync('/proc/'+value.pid+'/cmdline','utf8').split(String.fromCharCode(0)).includes('checkpoint-proof-heartbeat'); } catch {}
  console.log(JSON.stringify({...value,present:true,alive,observedAt:Date.now()})); }
`;

const operation = Effect.fnUntraced(function* (input: typeof Input.Type) {
  if (input.mode === "seed-legacy") {
    const env = (yield* WorkerEnvironment) as { SANDBOX: DurableObjectNamespace<Sandbox> };
    return yield* Effect.tryPromise(() =>
      env.SANDBOX.getByName(input.name).seedLegacy(input.checkpoint, input.snapshotAllowed),
    );
  }
  const native = yield* makeNativeSandbox(input.name, { instance: "standard-1" });
  const command = Effect.fnUntraced(function* (script: string) {
    const process = yield* native.exec(["node", "-e", script], { timeout: 120_000 });
    const output = yield* process.outputText({ timeout: 125_000, maxBytes: 16_384 });
    if (output.exitCode !== 0) return yield* Effect.die(`Fixture command failed: ${output.stderr}`);
    return output.stdout.trim();
  });
  const pulse = Effect.fnUntraced(function* () {
    const output = yield* native.inspect(["node", "-e", heartbeatRead], {
      timeout: 10_000,
      maxBytes: 4096,
    });
    if (output.exitCode !== 0 || output.timedOut || output.truncated)
      return yield* Effect.die(`Fixture heartbeat failed: ${JSON.stringify(output)}`);
    return JSON.parse(output.stdout);
  });
  switch (input.mode) {
    case "seed": {
      const started = yield* Clock.currentTimeMillis;
      const identity = yield* native.ensure();
      const bootMs = (yield* Clock.currentTimeMillis) - started;
      const workload = JSON.parse(
        yield* command(`${imports}
fs.mkdirSync(path.join(root,'src'),{recursive:true});
fs.mkdirSync(path.join(root,'assets'),{recursive:true});
fs.writeFileSync(path.join(root,'src/version.js'),'module.exports = { version: 0 };\\n');
for(let i=0;i<${input.representative ? 256 : 8};i++)
  fs.writeFileSync(path.join(root,'src','module-'+i+'.js'),'export const item'+i+' = '+i+';\\n'+ '// fixture source\\n'.repeat(128));
${
  input.representative
    ? `
fs.mkdirSync(path.join(root,'node_modules'),{recursive:true});
fs.cpSync('/usr/local/lib/node_modules/npm',path.join(root,'node_modules/npm'),{recursive:true});
let existing=0;
function count(p){for(const n of fs.readdirSync(p)){const f=path.join(p,n),s=fs.lstatSync(f);if(s.isDirectory())count(f);else if(s.isFile())existing+=s.size;}}
count(root);
const cipher=crypto.createCipheriv('aes-256-ctr',Buffer.alloc(32,19),Buffer.alloc(16,10));
for(let remaining=100*1024*1024-existing,i=0;remaining>0;i++){
  const size=Math.min(2*1024*1024,remaining);
  fs.writeFileSync(path.join(root,'assets','asset-'+i+'.bin'),cipher.update(Buffer.alloc(size)));
  remaining-=size;
}
`
    : ""
}
const child=require('node:child_process').spawn(process.execPath,['-e',${JSON.stringify(heartbeat)},'checkpoint-proof-heartbeat'],{detached:true,stdio:'ignore'}); child.unref();
${summary}`),
      );
      return { identity, bootMs, workload };
    }
    case "version":
      yield* native.writeFile(firstFile, `module.exports = { version: ${input.version} };\n`);
      return { version: yield* native.readFileString(firstFile) };
    case "describe":
      return yield* native.describe;
    case "inspect":
      return { workload: JSON.parse(yield* command(imports + summary)), heartbeat: yield* pulse() };
    case "read-files": {
      // Small reads after restore exposed the SDK's stdout/stderr deadlock (#951).
      const expected = yield* native.readFileString(firstFile);
      for (let index = 0; index < 32; index++) {
        if ((yield* native.readFileString(firstFile)) !== expected)
          return yield* Effect.die("Repeated read changed file contents");
      }
      const bytes = Uint8Array.from({ length: 256 * 1024 + 19 }, (_, index) => index % 256);
      const path = `${directory}/binary ' file`;
      yield* native.writeFile(path, bytes);
      const response = yield* native.readFile(path);
      const actual = new Uint8Array(yield* Effect.promise(() => response.arrayBuffer()));
      if (actual.length !== bytes.length || actual.some((byte, index) => byte !== bytes[index]))
        return yield* Effect.die("Streamed binary read changed file contents");
      const endless = yield* native.readFile("/dev/zero");
      yield* Effect.promise(() => endless.body!.cancel());
      if ((yield* native.readFileString(firstFile)) !== expected)
        return yield* Effect.die("Read after response cancellation failed");
      return { repeatedReads: 32, binaryBytes: actual.length, cancelled: true };
    }
    case "checkpoint": {
      const before = yield* native.describe;
      const heartbeatBefore = before.running ? yield* pulse() : null;
      const started = yield* Clock.currentTimeMillis;
      const checkpoint = yield* native.checkpoint(input.input);
      const saveMs = (yield* Clock.currentTimeMillis) - started;
      const after = yield* native.describe;
      const heartbeatAfter = after.running ? yield* pulse() : null;
      return {
        checkpoint,
        saveMs,
        startedAt: started,
        finishedAt: started + saveMs,
        before,
        after,
        heartbeatBefore,
        heartbeatAfter,
      };
    }
    case "restore": {
      const started = yield* Clock.currentTimeMillis;
      const identity = yield* native.restore(input.checkpoint);
      const ready = yield* Clock.currentTimeMillis;
      const content = yield* native.readFileString(firstFile);
      const usable = yield* Clock.currentTimeMillis;
      return {
        identity,
        content,
        restoreMs: ready - started,
        firstReadMs: usable - ready,
        usableMs: usable - started,
      };
    }
    case "backup": {
      yield* native.describe;
      const started = yield* Clock.currentTimeMillis;
      const backup = yield* native.backupDirectory({ dir: directory });
      return { backup, saveMs: (yield* Clock.currentTimeMillis) - started };
    }
    case "restore-directory": {
      const started = yield* Clock.currentTimeMillis;
      const identity = yield* native.ensure();
      const ready = yield* Clock.currentTimeMillis;
      yield* native.restoreDirectory(input.backup);
      const restored = yield* Clock.currentTimeMillis;
      const content = yield* native.readFileString(firstFile);
      const usable = yield* Clock.currentTimeMillis;
      return {
        identity,
        content,
        bootMs: ready - started,
        restoreMs: restored - ready,
        firstReadMs: usable - restored,
        usableMs: usable - started,
      };
    }
    case "delete-backup":
      yield* native.deleteDirectoryBackup(input.backup);
      return { deleted: true };
    case "park":
      yield* native.park;
      return yield* native.describe;
    case "destroy": {
      const before = yield* native.describe;
      yield* native.destroy;
      return { destroyed: true, checkpoint: before.checkpoint };
    }
  }
});

export default Worker.make(Layer.empty, {
  fetch: Effect.gen(function* () {
    const request = yield* Worker.NativeRequest;
    const env = (yield* WorkerEnvironment) as { readonly CHECKPOINT_PROOF_TOKEN?: string };
    if (
      !env.CHECKPOINT_PROOF_TOKEN ||
      request.headers.get("authorization") !== `Bearer ${env.CHECKPOINT_PROOF_TOKEN}`
    )
      return new Response(null, { status: 401 });
    const input = yield* Schema.decodeEffect(Schema.fromJsonString(Input))(
      yield* Effect.tryPromise(() => request.text()),
    );
    return yield* operation(input).pipe(
      Effect.provide(NativeSandboxNamespace.layer({ binding: "SANDBOX" })),
      Effect.map((result) => Response.json({ ok: true, result })),
      Effect.catchTag("SandboxOperationError", (error) =>
        Effect.succeed(
          Response.json({
            ok: false,
            error: { tag: error._tag, code: error.code ?? null, operation: error.operation },
            diagnostic: failureDiagnostic(Cause.fail(error), { operation: "CheckpointFixture" }),
          }),
        ),
      ),
      Effect.catchCause((cause) =>
        Effect.succeed(
          Response.json(
            {
              ok: false,
              diagnostic: failureDiagnostic(cause, { operation: "CheckpointFixture" }),
            },
            { status: 500 },
          ),
        ),
      ),
    );
  }),
});
