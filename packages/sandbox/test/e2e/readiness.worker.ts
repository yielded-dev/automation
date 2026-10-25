import { webCryptoLayer } from "@yielded/sandbox/cloudflare/Bindings";
import * as Crypto from "effect/Crypto";
import { Cause, Effect, Layer, Schema, Tracer } from "effect";
import { DurableObject, DurableObjectState } from "effect-cf";
import {
  makeSandboxAgentContainer,
  makeSandboxAgentOutbound,
} from "@yielded/sandbox/cloudflare/Container";
import {
  NativeSandboxNamespace,
  makeNativeSandbox,
} from "@yielded/sandbox/cloudflare/NativeSandbox";
import { error } from "@yielded/sandbox/cloudflare/ContainerState";
import { SessionSandbox } from "@yielded/sandbox/SessionSandbox";
import { SessionStateStore } from "@yielded/sandbox/cloudflare/SessionStateStore";
import { SandboxAgentConfig } from "@yielded/sandbox/cloudflare/Config";
import { WorkspaceServiceCoordinator } from "@yielded/sandbox/cloudflare/WorkspaceProcesses";
import { RepositoryInstallAuthorization } from "@yielded/sandbox/cloudflare/RepositoryInstallAuthorization";
import { NativeWorkspaceIdentity } from "@yielded/sandbox/cloudflare/NativeWorkspaceIdentity";
import {
  prepareRepositorySetup,
  readRepositorySetup,
  repositorySetupFingerprint,
  repositorySetupCheckpointMatches,
} from "@yielded/sandbox/cloudflare/RepositorySetup";
import {
  WorkspaceBaseline,
  WorkspaceBaselineRepository,
} from "@yielded/sandbox/cloudflare/WorkspaceBaselineRepository";
import { MATERIALIZATION_MARKER, REPO_DIR } from "@yielded/sandbox/workspace-paths";
import { failureDiagnostic } from "@yielded/sandbox/observability/FailureDiagnostic";

const boundaries = new Set([
  "SandboxContainer.ensure",
  "SandboxContainer.bootOutputGate",
  "SandboxContainer.reserve",
  "SandboxContainer.reserveCapacity",
  "SandboxContainer.capacityReceiptOutputGate",
  "SandboxContainer.startOutputGate",
  "SandboxContainer.nativeStart",
  "SandboxContainer.refreshInactivity",
  "SandboxContainer.configureOutbound",
  "SandboxContainer.ensureSupervisor",
  "SandboxContainer.waitForPort",
  "RepositorySetup.prepare",
  "repository-install",
  "workspace-checkpoint-capture",
  "repository-start",
  "repository-services",
]);
const traceLayer = Layer.succeed(
  Tracer.Tracer,
  Tracer.make({
    span(options) {
      const span = new Tracer.NativeSpan(options);
      const end = span.end.bind(span);
      span.end = (endTime, exit) => {
        end(endTime, exit);
        if (boundaries.has(options.name)) {
          console.log(
            "READINESS_SPAN " +
              JSON.stringify({
                name: options.name,
                durationMillis: Number((endTime - options.startTime) / 1_000_000n),
                outcome: exit._tag,
                attributes: Object.fromEntries(
                  [...span.attributes].filter(([key]) =>
                    /^(sandbox\.(container|process|port)|repository\.setup)/.test(key),
                  ),
                ),
              }),
          );
        }
      };
      return span;
    },
  }),
);
const options = { namespaceBinding: "SANDBOX", allowedHosts: () => [], eventLayer: traceLayer };
const NativeContainer = makeSandboxAgentContainer(options);
const CAPACITY_NAME = "sandbox-capacity";
type FixtureEnv = {
  SANDBOX: DurableObjectNamespace<InstanceType<typeof NativeContainer>>;
  FIXTURE_REJECT_LOCATE?: boolean;
};
const placementEvent = (event: object) =>
  console.log("READINESS_PLACEMENT " + JSON.stringify(event));
// Observe real namespace lookups, including their caller, without replacing any RPC.
const placementEnvironment = (env: FixtureEnv, callerId: string): FixtureEnv => ({
  ...env,
  SANDBOX: new Proxy(env.SANDBOX, {
    get(target, key) {
      const value = Reflect.get(target, key, target);
      if (typeof value !== "function") return value;
      return (...args: unknown[]) => {
        if (key === "getByName") placementEvent({ event: "lookup", callerId, name: args[0] });
        return Reflect.apply(value, target, args);
      };
    },
  }),
});
export class Sandbox extends NativeContainer {
  readonly fixtureEnv: FixtureEnv;
  constructor(
    readonly fixtureState: globalThis.DurableObjectState,
    env: FixtureEnv,
  ) {
    const traced = placementEnvironment(env, fixtureState.id.toString());
    super(fixtureState, traced);
    this.fixtureEnv = traced;
  }
}
for (const method of [
  "locate",
  "describe",
  "ensure",
  "grantPreview",
  "processes",
  "lease",
  "fetch",
  "pinImage",
  "reserveCapacity",
  "releaseCapacity",
] as const)
  Object.defineProperty(Sandbox.prototype, method, {
    value: async function (this: Sandbox, ...args: unknown[]) {
      const objectId = this.fixtureState.id.toString();
      placementEvent({ event: "enter", objectId, method });
      if (method === "locate" && this.fixtureEnv.FIXTURE_REJECT_LOCATE)
        throw new Error("Placement is unavailable in this fixture deployment");
      if (method === "locate" && args[0] === "placement-reentrant") {
        const namespace = this.fixtureEnv.SANDBOX;
        if (objectId === namespace.idFromName("placement-reentrant").toString()) {
          // The coordinator must admit callbacks while awaiting the child's first touch.
          const coordinator = namespace.getByName(CAPACITY_NAME);
          const reserved = await coordinator.reserveCapacity(objectId, "fixture-callback");
          if (reserved._tag !== "Success" || !reserved.value)
            throw new Error("Reentrant capacity reservation failed");
          const pinned = await coordinator.pinImage(null);
          const released = await coordinator.releaseCapacity(objectId, "fixture-callback");
          if (pinned._tag !== "Success" || released._tag !== "Success")
            throw new Error("Reentrant coordinator callback failed");
        }
      }
      let result = await Reflect.apply(NativeContainer.prototype[method], this, args);
      if (
        method === "locate" &&
        args[0] === "placement-reply" &&
        objectId === this.fixtureEnv.SANDBOX.idFromName(CAPACITY_NAME).toString() &&
        !(await this.fixtureState.storage.get("fixture-locate-failed"))
      ) {
        await this.fixtureState.storage.put("fixture-locate-failed", true);
        // RPC transport succeeded, but its NativeReply must still stop the caller's lookup.
        result = {
          _tag: "Failure",
          diagnostic: failureDiagnostic(Cause.fail(error("locate", "Injected placement reply")), {
            operation: "SandboxContainer.locate",
          }),
        };
      }
      placementEvent({
        event: "return",
        objectId,
        method,
        outcome:
          result instanceof Response
            ? result.status
            : Schema.is(Schema.Struct({ _tag: Schema.String }))(result)
              ? result._tag
              : null,
      });
      return result;
    },
  });
export const SandboxOutbound = makeSandboxAgentOutbound(options);

const fixtureLayer = Layer.mergeAll(
  webCryptoLayer,
  SessionStateStore.layer,
  WorkspaceServiceCoordinator.layer,
  RepositoryInstallAuthorization.emptyLayer,
  WorkspaceBaselineRepository.noopLayer,
  NativeSandboxNamespace.layer({ binding: "SANDBOX" }),
  SandboxAgentConfig.layer({
    repositorySetupPath: ".fixture-setup.json",
    defaultModel: "unused",
    artifactsNamespace: "fixture",
    artifactsHostname: "fixture.invalid",
  }),
);
const Input = Schema.Union([
  Schema.Struct({
    name: Schema.String,
    mode: Schema.Literals(["cold", "retry", "failing-service", "destroy", "placement"]),
    expectedInstanceId: Schema.optionalKey(Schema.String),
  }),
  Schema.Struct({
    name: Schema.String,
    mode: Schema.Literals(["restored", "changed-input"]),
    baseline: WorkspaceBaseline,
  }),
]);
const command = Effect.fnUntraced(function* (script: string) {
  const process = yield* (yield* SessionSandbox).exec(["sh", "-c", script], { cwd: REPO_DIR });
  const result = yield* process.outputText({ timeout: 30_000, maxBytes: 4096 });
  if (result.exitCode !== 0 || result.timedOut || result.truncated)
    return yield* Effect.die(`Fixture command failed: ${result.exitCode}`);
  return result.stdout.trim();
});

const prepare = Effect.fn("ReadinessFixture.prepare")(
  function* (input: unknown) {
    const value = yield* Schema.decodeUnknownEffect(Input)(input);
    if (value.mode === "placement") {
      const namespace = yield* NativeSandboxNamespace;
      const caller = yield* DurableObjectState.DurableObjectState;
      const probes = [];
      for (const first of [
        "describe",
        "grant",
        "inspect",
        "lease",
        "recovery",
        "fetch",
        "reply",
        "reentrant",
      ] as const) {
        const name = `placement-${first}`;
        const sandbox = yield* makeNativeSandbox(name, {
          expectedInstanceId: "missing-instance",
          recovery: first === "recovery" ? "exactWorkspaceRequired" : "freshAllowed",
          imageKey: first === "describe" ? "missing-image" : "harness",
        });
        const operation = Effect.gen(function* () {
          switch (first) {
            case "grant":
              return yield* sandbox.grantPreview({
                port: 5173,
                token: "a".repeat(32),
                materializationId: "missing-materialization",
                generation: 1,
                instanceId: "missing-instance",
                processId: "missing-process",
              });
            case "inspect":
              return yield* sandbox.listProcesses;
            case "lease":
              return yield* sandbox.lease("missing-lease", false);
            case "recovery":
              return yield* sandbox.ensure();
            case "fetch":
              return (yield* sandbox.forwardPort(5173, new Request("http://localhost:5173/")))
                .status;
            default:
              return yield* sandbox.describe;
          }
        });
        const initial = yield* operation.pipe(Effect.exit);
        const retry = yield* operation.pipe(Effect.exit);
        // Cleanup cannot depend on a configured image or replace a lost accepted boot.
        yield* sandbox.destroy;
        probes.push({
          first,
          name,
          objectId: (yield* namespace.idFromName(name)).toString(),
          initial: initial._tag,
          retry: retry._tag,
          value: initial._tag === "Success" ? initial.value : null,
          failure: initial._tag === "Failure" ? Cause.pretty(initial.cause) : null,
        });
      }
      return {
        mode: value.mode,
        callerId: caller.id.toString(),
        coordinatorId: (yield* namespace.idFromName(CAPACITY_NAME)).toString(),
        probes,
      };
    }
    const baseline = "baseline" in value ? value.baseline : undefined;
    const unresolved = yield* makeNativeSandbox(value.name, {
      instance: "standard-1",
      ...(value.mode === "retry" && value.expectedInstanceId !== undefined
        ? { recovery: "exactWorkspaceRequired", expectedInstanceId: value.expectedInstanceId }
        : {}),
    });
    if (value.mode === "destroy") {
      const snapshotId = (yield* unresolved.describe).checkpoint?.snapshotId ?? null;
      yield* unresolved.destroy;
      return { destroyed: true, snapshotId };
    }
    const identity = yield* unresolved.ensure(baseline ? { checkpoint: baseline.checkpoint } : {});
    const sandbox = yield* makeNativeSandbox(value.name, {
      instance: "standard-1",
      expectedInstanceId: identity.instanceId,
      recovery: "exactWorkspaceRequired",
    });
    return yield* Effect.gen(function* () {
      const store = yield* SessionStateStore;
      if (value.mode === "cold" || value.mode === "failing-service") {
        yield* sandbox.mkdir(REPO_DIR, { recursive: true });
        const recipe = {
          install: "printf 'install\\n' >> /workspace/fixture-install-count",
          installationInputs: [".fixture-setup.json", "install-input.txt"],
          start: ["printf 'start\\n' >> /workspace/fixture-start-count"],
          services: [
            {
              name: "fixture-http",
              command:
                value.mode === "failing-service"
                  ? "exit 17"
                  : 'exec bun -e \'Bun.serve({hostname:"0.0.0.0",port:5173,fetch:()=>new Response("fixture-ready")})\'',
              port: 5173,
            },
          ],
          tools: ["git", "bun"],
          requiredSecrets: [],
        };
        yield* sandbox.writeFile(`${REPO_DIR}/.fixture-setup.json`, JSON.stringify(recipe));
        yield* sandbox.writeFile(`${REPO_DIR}/install-input.txt`, "fixture-dependencies-v1\n");
        yield* sandbox.writeFile(`${REPO_DIR}/source.txt`, "fixture-source-v1\n");
        yield* command(
          "git init --initial-branch main >/dev/null && git config user.name Fixture && git config user.email fixture@example.invalid && git add . && GIT_AUTHOR_DATE=2026-10-01T00:00:00Z GIT_COMMITTER_DATE=2026-10-01T00:00:00Z git commit -qm fixture",
        );
        const marker = yield* (yield* Crypto.Crypto).randomUUIDv4().pipe(Effect.orDie);
        yield* sandbox.writeFile(MATERIALIZATION_MARKER, marker);
        yield* store.materializationId.put(marker);
      } else if (baseline !== undefined) {
        if (identity.restoredSnapshotId !== baseline.checkpoint.snapshotId)
          return yield* Effect.die("Native snapshot was not restored");
        const marker = (yield* sandbox.readFileString(MATERIALIZATION_MARKER)).trim();
        if (marker !== baseline.checkpoint.materializationId)
          return yield* Effect.die("Snapshot materialization changed");
        if ((yield* command("git rev-parse HEAD")) !== baseline.checkpoint.head)
          return yield* Effect.die("Snapshot source changed");
        yield* store.materializationId.put(marker);
        yield* sandbox.writeFile(
          `${REPO_DIR}/${value.mode === "changed-input" ? "install-input.txt" : "source.txt"}`,
          "fixture-v2\n",
        );
        yield* command(
          "git add . && GIT_AUTHOR_DATE=2026-10-01T00:01:00Z GIT_COMMITTER_DATE=2026-10-01T00:01:00Z git commit -qm fixture-update",
        );
      }
      const sourceHead = yield* command("git rev-parse HEAD");
      const recipe = yield* readRepositorySetup(sourceHead);
      const fingerprint = yield* repositorySetupFingerprint(sourceHead, recipe);
      const compatible =
        baseline === undefined
          ? null
          : repositorySetupCheckpointMatches(baseline, recipe, fingerprint);
      const marker = yield* store.materializationId.get;
      const exit = yield* prepareRepositorySetup({
        sourceKey: "fixture",
        sourceHead,
        startPreview: true,
        ...(value.mode === "retry"
          ? {}
          : {
              snapshot: { repository: "fixture", identity, share: false },
              ...(baseline === undefined ? {} : { installed: baseline }),
            }),
      }).pipe(
        Effect.provideService(NativeWorkspaceIdentity, {
          path: MATERIALIZATION_MARKER,
          id: marker,
        }),
        Effect.exit,
      );
      const report = exit._tag === "Success" ? exit.value : null;
      const checkpoint = yield* store.workspaceCheckpoint.get;
      const receipt = yield* store.repositorySetupReceipt.get;
      const installCount = yield* command("wc -l < /workspace/fixture-install-count");
      const startCount = yield* command(
        "if test -e /workspace/fixture-start-count; then wc -l < /workspace/fixture-start-count; else printf 0; fi",
      );
      const wrongOwner = yield* unresolved
        .ensure({ expectedInstanceId: "not-the-owner", recovery: "exactWorkspaceRequired" })
        .pipe(Effect.exit);
      const after = yield* sandbox.ensure();
      let app = null;
      if (report?.state === "ready") {
        const response = yield* sandbox.forwardPort(5173, new Request("http://localhost:5173/"));
        app = { status: response.status, body: yield* Effect.promise(() => response.text()) };
      }
      return {
        mode: value.mode,
        identity,
        compatible,
        sourceHead,
        fingerprint,
        result: exit._tag,
        report,
        checkpoint,
        receipt:
          receipt === null ? null : { prepared: receipt.prepared, sourceHead: receipt.sourceHead },
        installCount: Number(installCount),
        startCount: Number(startCount),
        rejectedWrongOwner: wrongOwner._tag === "Failure",
        sameOwner: after.instanceId === identity.instanceId,
        app,
        baseline: checkpoint && report ? { repository: "fixture", checkpoint, report } : null,
      };
    }).pipe(Effect.provideService(SessionSandbox, sandbox));
  },
  Effect.scoped,
  Effect.tapCause((cause) =>
    Effect.sync(() =>
      console.error(failureDiagnostic(cause, { operation: "ReadinessFixture.prepare" })),
    ),
  ),
);

const NativePreparation = DurableObject.make(fixtureLayer, {
  eventLayer: traceLayer,
  rpc: { prepare },
});
export class Preparation extends NativePreparation {
  constructor(state: globalThis.DurableObjectState, env: FixtureEnv) {
    super(state, placementEnvironment(env, state.id.toString()));
  }
}

export default {
  async fetch(
    request: Request,
    env: { PREPARATION: DurableObjectNamespace<InstanceType<typeof Preparation>> },
  ) {
    const input = Schema.decodeUnknownSync(Input)(await request.json());
    const result = await env.PREPARATION.getByName(input.name).prepare(input);
    console.log("READINESS_DONE");
    return Response.json(result);
  },
};
