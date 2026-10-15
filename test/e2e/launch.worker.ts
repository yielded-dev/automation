import { Cause, Clock, DateTime, Effect, Layer, Redacted, Schema, Tracer } from "effect";
import { FetchHttpClient } from "effect/http";
import { SqlClient } from "effect/sql";
import { PgClient } from "@effect/sql-pg";
import {
  DurableObject,
  DurableObjectAlarm,
  DurableObjectContainer,
  DurableObjectSqlite,
  DurableObjectState as ObjectState,
  WorkerEnvironment,
} from "effect-cf";
import { fixtureArtifacts } from "./fixtures/artifacts.worker";
import { RunLaunch } from "@yielded/automation/domain/RepairExecution";
import { RepairReportJson } from "@yielded/automation/domain/RepairReport";
import { RepairExecutionRepository } from "@yielded/automation/server/database/RepairExecutionRepository";
import { RepairExecutionContext } from "@yielded/automation/server/RepairExecutionContext";
import {
  startRepairExecution,
  inspectRepairExecution,
  reportRepairExecution,
  reconcileRepairExecution,
  retireRepairExecution,
} from "@yielded/automation/server/RepairExecution";
import { authorizeRun } from "@yielded/automation/server/RepairExecutionAuthority";
import { readRepairSource } from "@yielded/automation/server/Source";
import { RepairAuthorization } from "@yielded/automation/server/RepairAuthorization";
import { RepairHostError } from "@yielded/automation/server/RepairHostError";
import { RepairModel } from "@yielded/automation/server/RepairModel";
import { RepairPresentation } from "@yielded/automation/server/RepairPresentation";
import { RepairErrorReporter } from "@yielded/automation/server/RepairErrorReporter";
import { GithubAppAuth } from "@yielded/automation/server/GithubAppAuth";
import { RunConfig } from "@yielded/automation/server/RunConfig";
import { contentHash } from "@yielded/automation/server/hash";
import {
  RepairSandboxTaskClient,
  RepairSandboxTaskTransportError,
} from "@yielded/automation/server/RepairSandboxTaskClient";
import { SandboxTaskMachineCreate } from "@yielded/automation/server/sandbox-contracts/sandbox-task-machine";
import type { SandboxTaskDetail } from "@yielded/automation/server/sandbox-contracts/sandbox-task";
import { RepairSandboxTaskClientLive } from "../../packages/automation/src/cloudflare/RepairSandboxTaskClientLive";
import { makeRepairSandboxTaskDurableObject } from "../../packages/automation/src/cloudflare/RepairSandboxTaskDurableObject";
import { RepairSandboxTaskNotifications } from "../../packages/automation/src/cloudflare/RepairSandboxTaskNotifications";
import {
  RepairSandboxPreparation,
  RepairSandboxPreparationError,
} from "../../packages/automation/src/cloudflare/RepairSandboxPreparation";
import {
  RepairSandboxTaskStore,
  RepairSandboxState,
} from "../../packages/automation/src/cloudflare/RepairSandboxTaskStore";
import { repairSandboxTaskSandbox } from "../../packages/automation/src/cloudflare/RepairSandboxTaskSandbox";
import { repairSandboxTaskObjectName } from "../../packages/automation/src/cloudflare/RepairSandboxTaskObject";
import {
  makeSandboxAgentContainer,
  makeSandboxAgentOutbound,
} from "@yielded/sandbox/cloudflare/Container";
import { SandboxAgentArtifacts, sandboxFor } from "@yielded/sandbox/cloudflare/Bindings";
import { SandboxAgentConfig } from "@yielded/sandbox/cloudflare/Config";
import { OutboundAuthority } from "@yielded/sandbox/cloudflare/OutboundAuthority";
import { ArtifactsCheckout } from "@yielded/sandbox/cloudflare/ArtifactsCheckout";
import { ContainerState, error, reply } from "@yielded/sandbox/cloudflare/ContainerState";
import { prepareWorkspaceBaseline } from "@yielded/sandbox/cloudflare/WorkspaceBaselinePreparation";
import {
  WorkspaceBaselineError,
  WorkspaceBaselineRepository,
} from "@yielded/sandbox/cloudflare/WorkspaceBaselineRepository";
import { refreshWorkspaceBaselines } from "@yielded/sandbox/cloudflare/WorkspaceBaselineRefresh";
import { resourceHash, sandboxNameFor } from "@yielded/sandbox/cloudflare/resource-names";
import { SessionStateStore } from "@yielded/sandbox/cloudflare/SessionStateStore";
import { SessionRunSink } from "@yielded/sandbox/cloudflare/RunSink";
import { SessionSandbox } from "@yielded/sandbox/SessionSandbox";
import { RepositorySetupError } from "@yielded/sandbox/model/RepositorySetup";
import { Harness } from "@yielded/sandbox/harness/Harness";
import { REPO_DIR } from "@yielded/sandbox/workspace-paths";
import {
  RepositoryPreparationError,
  RepositoryPreparationExecution,
  RepositoryPreparationObject,
  RepositoryPreparationSource,
  RepositoryPreparationSources,
  RepositoryPreparationState,
  repositoryPreparationName,
  requestRepositoryPreparation,
} from "@yielded/sandbox/cloudflare/RepositoryPreparation";
import { makeRepositoryPreparationDurableObject } from "@yielded/sandbox/cloudflare/RepositoryPreparationRuntime";

// Local Effect operation timings only, not Cloudflare collector/export proof.
// RPC entries are recorded independently at the native method boundary below.
const traceLayer = Layer.succeed(
  Tracer.Tracer,
  Tracer.make({
    ...Tracer.nativeTracer,
    span(options) {
      const span = Tracer.nativeTracer.span(options);
      const end = span.end.bind(span);
      span.end = (time, exit) => {
        end(time, exit);
        if (
          exit._tag === "Failure" ||
          span.attributes.has("db.query.text") ||
          /^(RepairExecution\.|RepairExecutionRepository\.|RepairSandboxTask\.(admit|create)|RepairSource\.read|ArtifactsCheckout\.|Artifacts\.|SandboxContainer\.|WorkspaceBaseline\.|RepositorySetup\.|repository-install|workspace-checkpoint|repository-restore|message-pipeline)/.test(
            options.name,
          )
        )
          console.log(
            "LAUNCH_SPAN " +
              JSON.stringify({
                name: options.name,
                startedAtMillis: Number(options.startTime) / 1e6,
                endedAtMillis: Number(time) / 1e6,
                durationMillis: Number(time - options.startTime) / 1e6,
                query: span.attributes.get("db.query.text"),
                outcome: exit._tag,
                failure: exit._tag === "Failure" ? Cause.pretty(exit.cause) : undefined,
              }),
          );
      };
      return span;
    },
  }),
);
const sandboxOptions = {
  namespaceBinding: "SANDBOX",
  allowedHosts: () => ["fixture.invalid"],
  eventLayer: traceLayer,
};
const NativeContainer = makeSandboxAgentContainer(sandboxOptions);
export class Sandbox extends NativeContainer {
  constructor(
    state: DurableObjectState,
    readonly fixtureEnv: Env,
  ) {
    super(state, fixtureEnv);
  }
}
export const SandboxOutbound = makeSandboxAgentOutbound(sandboxOptions);

Object.defineProperty(Sandbox.prototype, "inspect", {
  value: async function (this: Sandbox, ...args: Parameters<Sandbox["inspect"]>) {
    const recipeRead = args[0][5] === "repository-setup-read";
    if (recipeRead && this.fixtureEnv.FIXTURE_RECIPE_STDERR) {
      const [executable, ...argv] = args[0];
      args[0] = [
        executable,
        ...argv.map((arg, index) => (index === 3 ? `${arg}; printf '%32768s' '' >&2` : arg)),
      ];
    }
    const result = await NativeContainer.prototype.inspect.apply(this, args);
    if (recipeRead && result._tag === "Success")
      console.log(
        "LAUNCH_RECIPE_READ " +
          JSON.stringify({
            sourceHead: args[0][6],
            maxBytes: args[1].maxBytes,
            stdoutBytes: new TextEncoder().encode(result.value.stdout).length,
            stderrBytes: new TextEncoder().encode(result.value.stderr).length,
            exitCode: result.value.exitCode,
            timedOut: result.value.timedOut,
            truncated: result.value.truncated,
          }),
      );
    return result;
  },
});

// Fail the real removal only after proving that Git imported the transport's
// objects. Its RPC argument supplies the private path, which survives reload
// through the runner's fixture binding rather than a production path export.
Object.defineProperty(Sandbox.prototype, "remove", {
  value: function (this: Sandbox, ...args: Parameters<Sandbox["remove"]>) {
    if (!this.fixtureEnv.FIXTURE_FAIL_HISTORY_CLEANUP)
      return NativeContainer.prototype.remove.apply(this, args);
    return this[DurableObject.RunSymbol](
      reply(
        "SandboxContainer.remove",
        Effect.gen(function* () {
          const container = yield* DurableObjectContainer.DurableObjectContainer;
          const process = yield* container.execScoped(
            [
              "sh",
              "-c",
              'set -eu; git rev-parse --is-shallow-repository; git show HEAD^:source.txt; git rev-list --count HEAD; git --git-dir "$1/repository.git" rev-parse --is-bare-repository; git --git-dir "$1/repository.git" rev-list --count FETCH_HEAD; if [ -e /workspace/fixture-install-count ]; then wc -l < /workspace/fixture-install-count; else echo 0; fi',
              "fixture-history-cleanup",
              args[0],
            ],
            { cwd: REPO_DIR },
          );
          const output = yield* process.outputText;
          if (output.exitCode !== 0)
            return yield* Effect.die("Fixture transport inspection failed: " + output.stderr);
          const [shallow, parentSource, commits, bare, transportCommits, installs] = output.stdout
            .trim()
            .split("\n");
          console.log(
            "LAUNCH_HISTORY_CLEANUP " +
              JSON.stringify({
                path: args[0],
                instanceId: args[3].expectedInstanceId,
                shallow,
                parentSource,
                commits: Number(commits),
                bare,
                transportCommits: Number(transportCommits),
                installs: Number(installs),
              }),
          );
          return yield* error("fixture.history-cleanup", "Injected history cleanup failure");
        }),
      ).pipe(Effect.scoped),
    );
  },
});
Object.defineProperty(Sandbox.prototype, "checkpoint", {
  value: async function (this: Sandbox, ...args: Parameters<Sandbox["checkpoint"]>) {
    const path = this.fixtureEnv.FIXTURE_HISTORY_TRANSPORT_PATH;
    // New evidence reads happen after capture; the history-cleanup case inspects before it.
    const captured =
      path === undefined ? await NativeContainer.prototype.checkpoint.apply(this, args) : undefined;
    if (
      path !== undefined ||
      (this.fixtureEnv.FIXTURE_CHECKPOINT_EVIDENCE &&
        captured?._tag === "Success" &&
        captured.value !== null)
    ) {
      const observed = await this[DurableObject.RunSymbol](
        Effect.gen(function* () {
          const container = yield* DurableObjectContainer.DurableObjectContainer;
          const process = yield* container.execScoped(
            [
              "sh",
              "-c",
              'set -eu; if [ -e "$1" ]; then echo present; else echo absent; fi; wc -l < /workspace/fixture-install-count; git rev-parse HEAD; git rev-parse --is-shallow-repository; git rev-list --count HEAD; if [ -e fixture-result.txt ] || [ -e /workspace/fixture-turn-count ]; then echo used; else echo pristine; fi',
              "fixture-capture",
              path ?? "/workspace/absent-fixture-transport",
            ],
            { cwd: REPO_DIR },
          );
          const output = yield* process.outputText;
          if (output.exitCode !== 0)
            return yield* Effect.die("Fixture capture inspection failed: " + output.stderr);
          const [transport, installs, head, shallow, commits, state] = output.stdout
            .trim()
            .split("\n");
          return {
            transportExists: transport === "present",
            installs: Number(installs),
            head,
            shallow,
            commits: Number(commits),
            pristine: state === "pristine",
            restoredSnapshotId: (yield* (yield* ContainerState).identity())?.restoredSnapshotId,
          };
        }).pipe(Effect.scoped),
      );
      console.log(
        "LAUNCH_PREPARATION_CAPTURE " +
          JSON.stringify({ ...observed, instanceId: args[0].instanceId }),
      );
    }
    const result = captured ?? (await NativeContainer.prototype.checkpoint.apply(this, args));
    if (
      this.fixtureEnv.FIXTURE_CHECKPOINT_EVIDENCE &&
      result._tag === "Success" &&
      result.value !== null
    )
      await this.fixtureEnv.FIXTURE_CHECKPOINT.fetch("http://fixture/checkpoint", {
        method: "POST",
        body: JSON.stringify({ input: args[0], checkpoint: result.value }),
      });
    return result;
  },
});

// Replace only the model port. Native alarms run the production pipeline,
// checkout, Git persistence and saved-checkpoint retention in real containers.
const harnessLayer = Layer.succeed(Harness, {
  name: "codex",
  binary: "codex",
  version: "fixture-no-model",
  runTurn: (request) =>
    Effect.gen(function* () {
      const sandbox = yield* SessionSandbox;
      const inspection = yield* sandbox.exec(
        [
          "sh",
          "-c",
          "set -eu; git rev-parse --is-shallow-repository; git show HEAD^:source.txt; wc -l < /workspace/fixture-install-count; git rev-list HEAD",
        ],
        { cwd: REPO_DIR },
      );
      const history = yield* inspection.outputText({ timeout: 30_000, maxBytes: 4096 });
      if (history.exitCode !== 0 || history.timedOut || history.truncated)
        return yield* Effect.die("Fixture ancestry inspection failed");
      const [shallow, parentSource, installs, ...commits] = history.stdout.trim().split("\n");
      console.log(
        "LAUNCH_HISTORY " +
          JSON.stringify({
            turnId: request.turnId,
            shallow,
            parentSource,
            commits,
            installs: Number(installs),
          }),
      );
      const process = yield* sandbox.exec(
        [
          "sh",
          "-c",
          "printf 'turn\\n' >> /workspace/fixture-turn-count; printf 'repaired\\n' >> fixture-result.txt; git add fixture-result.txt; git -c user.name=Fixture -c user.email=fixture@example.invalid commit -qm fixture-repair; git rev-parse HEAD",
        ],
        { cwd: REPO_DIR },
      );
      const output = yield* process.outputText({ timeout: 30_000, maxBytes: 4096 });
      if (output.exitCode !== 0 || output.timedOut || output.truncated)
        return yield* Effect.die("Fixture Git turn failed");
      const result = {
        resumeToken: "fixture-thread",
        terminalEventType: "turn.completed",
        lastAgentMessage: yield* Schema.encodeEffect(RepairReportJson)({
          candidateSha: output.stdout.trim(),
          summary: {
            title: "Synthetic saved repair",
            problem: "Disposable launch fixture",
            fix: "Retain a real Git candidate",
            behaviorChange: null,
            keyChange: null,
            reviewNotes: [],
            visualEvidence: null,
          },
          notes: "Fixture only; no model or hosted publication.",
          reviewResponses: [],
        }).pipe(Effect.orDie),
        usage: null,
        stderrTail: "",
      };
      yield* (yield* SessionStateStore).codexTurn.put({
        id: request.turnId,
        processId: process.id,
        token: "fixture-only",
        configuration: {
          command: ["false"],
          cwd: REPO_DIR,
          env: {},
          mode: "codex",
          port: 4500,
          nativePort: 4501,
        },
        threadParams: {},
        turnParams: {},
        resumeToken: null,
        deadlineAt: request.deadlineAtMs ?? (yield* Clock.currentTimeMillis) + 60_000,
        threadId: "fixture-thread",
        turnId: "fixture-turn",
        previousTurnId: null,
        startDispatched: true,
        result,
      });
      return result;
    }),
});
const workspaceConfig = Layer.unwrap(
  Effect.map(WorkerEnvironment, (env) =>
    SandboxAgentConfig.layer({
      repositorySetupPath: (env as Env).FIXTURE_REPOSITORY_SETUP_PATH ?? ".fixture-setup.json",
      defaultModel: "unused",
      artifactsNamespace: "fixture",
      artifactsHostname: "fixture.invalid",
    }),
  ),
);
const baselineLayer = WorkspaceBaselineRepository.postgresLayer(
  Layer.unwrap(
    Effect.map(WorkerEnvironment, (env) =>
      PgClient.layer({
        url: Redacted.make((env as Pick<Env, "DATABASE">).DATABASE.connectionString),
        maxConnections: 1,
      }),
    ),
  ),
);
const preparationSources = Layer.effect(
  RepositoryPreparationSources,
  Effect.gen(function* () {
    const env = (yield* WorkerEnvironment) as Env;
    return RepositoryPreparationSources.of({
      authorize: (repository) =>
        repository === "fixture/project"
          ? Effect.void
          : Effect.fail(RepositoryPreparationError.make({ reason: "unauthorized" })),
      select: () =>
        Effect.tryPromise({
          try: async () =>
            await (await env.FIXTURE_PREPARATION.fetch("https://fixture/source")).json(),
          catch: (cause) =>
            RepositoryPreparationError.make({ reason: "source-unavailable", cause }),
        }).pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(RepositoryPreparationSource)),
          Effect.mapError((cause) =>
            RepositoryPreparationError.make({ reason: "source-unavailable", cause }),
          ),
        ),
    });
  }),
);
const NativePreparation = makeRepositoryPreparationDurableObject({
  bindings: { artifacts: "ARTIFACTS", sandbox: "SANDBOX" },
  coordinatorBinding: "PREPARATIONS",
  configLayer: workspaceConfig,
  workspaceBaselineLayer: baselineLayer,
  sourcesLayer: preparationSources,
  diagnosticsLayer: traceLayer,
});
export class PreparationOwner extends NativePreparation {
  constructor(
    state: DurableObjectState,
    readonly fixtureEnv: Env,
  ) {
    super(state, { ...fixtureEnv, ARTIFACTS: fixtureArtifacts(fixtureEnv.FIXTURE_ARTIFACTS) });
  }
  evidence() {
    return this[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const stored = yield* (yield* ObjectState.DurableObjectState).storage.get(
          "repositoryPreparation",
        );
        const alarm = yield* (yield* DurableObjectAlarm.DurableObjectAlarm).getAlarmStatus({
          tag: "repository-preparation",
          id: "main",
        });
        return {
          state:
            stored === undefined
              ? null
              : yield* Schema.decodeUnknownEffect(RepositoryPreparationState)(stored),
          alarm:
            alarm === undefined
              ? undefined
              : {
                  ...alarm,
                  runAt: DateTime.toEpochMillis(alarm.runAt),
                  retryAt:
                    alarm.retryAt === undefined ? undefined : DateTime.toEpochMillis(alarm.retryAt),
                },
        };
      }),
    );
  }
}
// The original host method returned native undefined after scheduling its alarm.
// Keep that deployed reply at this boundary while exercising the typed shared client.
Object.defineProperty(PreparationOwner.prototype, "requestRepositoryPreparation", {
  value: async function (this: PreparationOwner, repository: string) {
    await NativePreparation.prototype.requestRepositoryPreparation.call(this, repository);
  },
});
Object.defineProperty(PreparationOwner.prototype, "executeRepositoryPreparation", {
  value: async function (
    this: PreparationOwner,
    input: typeof RepositoryPreparationExecution.Type,
  ) {
    const result = await NativePreparation.prototype.executeRepositoryPreparation.call(this, input);
    const response = await this.fixtureEnv.FIXTURE_PREPARATION.fetch("https://fixture/executed", {
      method: "POST",
      body: JSON.stringify({ input, result }),
    });
    return response.status === 503 ? { status: "unavailable" as const } : result;
  },
});
const CompatiblePreparation = makeRepairSandboxTaskDurableObject({
  bindings: { artifacts: "ARTIFACTS", sandbox: "SANDBOX" },
  coordinatorBinding: "COMPAT_PREPARATIONS",
  configLayer: workspaceConfig,
  workspaceBaselineLayer: baselineLayer,
  repositoryPreparationLayer: preparationSources,
  diagnosticsLayer: traceLayer,
  turnPolicy: { restrictModelRequests: true, sentryMcp: false, startRepositoryPreview: false },
  harnessLayer: Layer.effect(Harness, Effect.die("Preparation acquired the task harness")),
  runSinkLayer: Layer.effect(SessionRunSink, Effect.die("Preparation acquired the task run sink")),
  preparationLayer: Layer.effect(
    RepairSandboxPreparation,
    Effect.die("Preparation acquired task preparation"),
  ),
  taskNotificationsLayer: Layer.effect(
    RepairSandboxTaskNotifications,
    Effect.die("Preparation acquired task notifications"),
  ),
});
export class CompatiblePreparationOwner extends CompatiblePreparation {
  constructor(state: DurableObjectState, env: Env) {
    super(state, { ...env, ARTIFACTS: fixtureArtifacts(env.FIXTURE_ARTIFACTS) });
  }
  async evidence() {
    const state = await this[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const object = yield* ObjectState.DurableObjectState;
        return yield* object.storage.get("repositoryPreparation");
      }),
    );
    return {
      state: state ?? null,
      tables: this.ctx.storage.sql
        .exec<{ name: string }>("SELECT name FROM sqlite_master WHERE type = 'table'")
        .toArray()
        .map((row) => row.name),
    };
  }
}
const NativeRun = makeRepairSandboxTaskDurableObject({
  bindings: { artifacts: "ARTIFACTS", sandbox: "SANDBOX" },
  coordinatorBinding: "RUNS",
  configLayer: workspaceConfig,
  harnessLayer,
  preparationLayer: Layer.effect(
    RepairSandboxPreparation,
    Effect.gen(function* (): Effect.fn.Return<
      RepairSandboxPreparation["Service"],
      never,
      WorkerEnvironment
    > {
      const env = (yield* WorkerEnvironment) as Env;
      return {
        prepare: Effect.fnUntraced(function* (request: SandboxTaskMachineCreate) {
          if (request.repair?.repositoryId !== "fixture/prepared") return;
          if (request.source._tag !== "SeedRepository")
            return yield* Effect.die("Fixture preparation requires retained source");
          const source = request.source;
          const sessionId = `fixture-preparation:${source.commitSha}`;
          console.log("LAUNCH_PREPARATION " + request.id);
          const prepared = yield* Effect.tryPromise(
            async () =>
              await env.RUNS.getByName(sessionId).prepare({
                sessionId,
                turnId: sessionId,
                source: {
                  repositoryName: source.repositoryName,
                  repositoryId: source.artifactsRepositoryId,
                  commitSha: source.commitSha,
                  artifactsRef: source.artifactsRef,
                },
              }),
          ).pipe(Effect.mapError((cause) => RepairSandboxPreparationError.make({ cause })));
          if (prepared.status === "failed")
            return yield* RepairSandboxPreparationError.make({ cause: prepared.failure });
        }),
      };
    }),
  ),
  workspaceBaselineLayer: baselineLayer,
  diagnosticsLayer: traceLayer,
  turnPolicy: { restrictModelRequests: true, sentryMcp: false, startRepositoryPreview: false },
});
interface Env {
  FIXTURE_ARTIFACTS: Fetcher;
  FIXTURE_RECIPE_STDERR?: boolean;
  FIXTURE_FAIL_HISTORY_CLEANUP?: boolean;
  FIXTURE_HISTORY_TRANSPORT_PATH?: string;
  FIXTURE_CHECKPOINT_EVIDENCE?: boolean;
  FIXTURE_CHECKPOINT: Fetcher;
  FIXTURE_PREPARATION: Fetcher;
  FIXTURE_REPOSITORY_SETUP_PATH?: string;
  DATABASE: Hyperdrive;
  RUNS: DurableObjectNamespace<RepairRun>;
  PREPARATIONS: DurableObjectNamespace<PreparationOwner>;
  COMPAT_PREPARATIONS: DurableObjectNamespace<CompatiblePreparationOwner>;
}
const Preparation = Schema.Struct({
  sessionId: Schema.String,
  turnId: Schema.String,
  preparationKey: Schema.optionalKey(Schema.String),
  fault: Schema.optionalKey(Schema.Literal("drop-publication-reply")),
  source: Schema.Struct({
    repositoryName: Schema.String,
    repositoryId: Schema.String,
    commitSha: Schema.String,
    artifactsRef: Schema.String,
  }),
});
const MetadataRead = Schema.Struct({
  kind: Schema.Literals(["pin", "head"]),
  source: Preparation.fields.source,
});
export class RepairRun extends NativeRun {
  constructor(state: DurableObjectState, env: Env) {
    super(state, { ...env, ARTIFACTS: fixtureArtifacts(env.FIXTURE_ARTIFACTS) });
  }
  prepare(input: unknown) {
    return this[DurableObject.RunSymbol](
      Schema.decodeUnknownEffect(Preparation)(input).pipe(
        Effect.flatMap(({ fault, ...request }) =>
          Effect.gen(function* () {
            if (fault === undefined) return yield* prepareWorkspaceBaseline(request);
            const baselines = yield* WorkspaceBaselineRepository;
            return yield* prepareWorkspaceBaseline(request).pipe(
              Effect.provideService(WorkspaceBaselineRepository, {
                ...baselines,
                publish: (baseline, previous) =>
                  baselines.publish(baseline, previous).pipe(
                    Effect.flatMap((published) =>
                      published
                        ? Effect.fail(
                            new WorkspaceBaselineError({
                              cause: new Error("Injected lost publication acknowledgement"),
                            }),
                          )
                        : Effect.succeed(false),
                    ),
                  ),
              }),
            );
          }),
        ),
        Effect.scoped,
        Effect.provide(OutboundAuthority.noneLayer),
        Effect.catchCause((cause) => {
          const setupReasons = cause.reasons.flatMap((reason) =>
            Cause.isFailReason(reason) &&
            reason.error._tag === "WorkspaceBaselinePreparationError" &&
            Cause.isCause(reason.error.cause)
              ? reason.error.cause.reasons
              : [reason],
          );
          return Effect.succeed({
            status: "failed" as const,
            failure: Cause.pretty(cause),
            failureKinds: cause.reasons.map((reason) => reason._tag),
            setupFailures: setupReasons.flatMap((reason) =>
              Cause.isFailReason(reason) && Schema.is(RepositorySetupError)(reason.error)
                ? [{ recovery: reason.error.recovery, report: reason.error.report }]
                : [],
            ),
            reason:
              cause.reasons.flatMap((reason) =>
                Cause.isFailReason(reason) &&
                reason.error._tag === "WorkspaceBaselinePreparationError"
                  ? [reason.error.reason]
                  : [],
              )[0] ?? null,
          });
        }),
      ),
    );
  }
  preparationState(sessionId: string, destroy: boolean) {
    return this[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const store = yield* SessionStateStore;
        const sandbox = yield* sandboxFor(
          "standard",
          sandboxNameFor(yield* resourceHash(sessionId)),
        );
        if (destroy) yield* sandbox.destroy;
        return {
          preparation: yield* store.workspaceBaselinePreparation.get,
          checkpoint: yield* store.workspaceCheckpoint.get,
          description: yield* sandbox.describe,
        };
      }),
    );
  }
  refreshBaselines() {
    return this[DurableObject.RunSymbol](refreshWorkspaceBaselines);
  }
  metadata(input: unknown) {
    return this[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const value = yield* Schema.decodeUnknownEffect(MetadataRead)(input);
        const checkout = yield* ArtifactsCheckout;
        return value.kind === "pin"
          ? yield* checkout.readSourcePin(value.source)
          : yield* checkout.readGenerationMainHead(value.source);
      }).pipe(
        Effect.exit,
        Effect.map((exit) =>
          exit._tag === "Success"
            ? { exit: exit._tag, value: exit.value }
            : { exit: exit._tag, failure: Cause.pretty(exit.cause) },
        ),
      ),
    );
  }
  eventsStorage(available: boolean) {
    return this[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* available
          ? sql`ALTER TABLE fixture_hidden_execution RENAME TO repair_sandbox_execution`
          : sql`ALTER TABLE repair_sandbox_execution RENAME TO fixture_hidden_execution`;
      }).pipe(Effect.provide(DurableObjectSqlite.layer())),
    );
  }
  evidence() {
    return this[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const state = yield* (yield* RepairSandboxTaskStore).read;
        if (state === undefined) return { state: null, checkpoint: null, pipeline: null, turns: 0 };
        const store = yield* SessionStateStore;
        const checkpoint = yield* store.workspaceCheckpoint.get;
        const pipeline = yield* store.pipelineTurn.get;
        let turns = 0;
        let restoredSnapshotId = null;
        if (state.status === "succeeded" && !state.cancelled) {
          const sandbox = yield* repairSandboxTaskSandbox(state.request.id, "turn");
          restoredSnapshotId = (yield* sandbox.describe).current?.restoredSnapshotId ?? null;
          const process = yield* sandbox.exec(
            ["sh", "-c", "wc -l < /workspace/fixture-turn-count"],
            { cwd: REPO_DIR },
          );
          turns = Number(
            (yield* process.outputText({ timeout: 30_000, maxBytes: 4096 })).stdout.trim(),
          );
        }
        return {
          state: yield* Schema.encodeEffect(RepairSandboxState)(state),
          checkpoint,
          pipeline,
          restoredSnapshotId,
          codexTurnId: (yield* store.codexTurn.get)?.id ?? null,
          setupReport: (yield* store.repositorySetupReceipt.get)?.report ?? null,
          turns,
        };
      }).pipe(
        Effect.provide(
          RepairSandboxTaskStore.layer.pipe(Layer.provide(DurableObjectSqlite.layer())),
        ),
      ),
    );
  }
}

// The SDK installs RPC methods on the prototype. Preserve this and every
// argument, including its tracing envelope, while recording the actual entry.
Object.defineProperty(RepairRun.prototype, "create", {
  value: function (this: RepairRun, ...args: Parameters<RepairRun["create"]>) {
    console.log("LAUNCH_RPC " + JSON.stringify({ operation: "create", id: args[0].id }));
    return NativeRun.prototype.create.apply(this, args);
  },
});

const Input = Schema.Struct({
  mode: Schema.Literals([
    "start",
    "reconcile",
    "inspect",
    "report",
    "retire",
    "retire-completed",
    "source",
    "evidence",
    "events",
    "detail",
    "snapshot",
    "break-events",
    "restore-events",
    "retain-launch",
  ]),
  launch: RunLaunch,
  application: Schema.String,
  fault: Schema.optionalKey(
    Schema.Literals([
      "interrupt-before-create",
      "drop-create-reply",
      "changed-create-fingerprint",
      "omit-inspect-fingerprint",
      "changed-retained-intent",
      "slow-authorization",
    ]),
  ),
});
const hostPorts = Layer.mergeAll(
  Layer.succeed(RepairModel, { prepare: Effect.succeed, usage: () => Effect.succeed(undefined) }),
  Layer.succeed(RepairPresentation, {
    retain: () => Effect.void,
    publicationContent: () => Effect.die("Publication is outside this fixture"),
  }),
  Layer.succeed(RepairErrorReporter, {
    report: (cause, operation) => Effect.sync(() => console.error(operation, Cause.pretty(cause))),
  }),
  Layer.succeed(RunConfig, {
    accountId: "fixture",
    artifactsNamespace: "fixture",
    githubInstallationId: "1",
  }),
  GithubAppAuth.layerUnavailable,
  FetchHttpClient.layer,
);

export default {
  async fetch(request: Request, env: Env) {
    const path = new URL(request.url).pathname;
    if (path === "/coordinator-request" || path === "/coordinator-state") {
      const { repository, compatible } = Schema.decodeUnknownSync(
        Schema.Struct({
          repository: Schema.NonEmptyString,
          compatible: Schema.optionalKey(Schema.Boolean),
        }),
      )(await request.json());
      const binding = compatible ? "COMPAT_PREPARATIONS" : "PREPARATIONS";
      if (path === "/coordinator-request") {
        await Effect.runPromise(
          requestRepositoryPreparation(repository).pipe(
            Effect.provide(RepositoryPreparationObject.layer({ binding })),
            Effect.provideService(WorkerEnvironment, env),
          ),
        );
        return Response.json({ queued: true });
      }
      const name = await Effect.runPromise(repositoryPreparationName(repository));
      if (compatible) {
        const owner = await env.COMPAT_PREPARATIONS.getByName(name).evidence();
        const retained = Schema.decodeUnknownSync(Schema.NullOr(RepositoryPreparationState))(
          owner.state,
        );
        return Response.json({
          ...owner,
          execution:
            retained === null
              ? null
              : await env.COMPAT_PREPARATIONS.getByName(
                  `${name}:${retained.generation}`,
                ).evidence(),
        });
      }
      return Response.json(await env.PREPARATIONS.getByName(name).evidence());
    }
    if (new URL(request.url).pathname === "/preparation-state") {
      const input = Schema.decodeUnknownSync(
        Schema.Struct({ sessionId: Schema.String, destroy: Schema.Boolean }),
      )(await request.json());
      return Response.json(
        await env.RUNS.getByName(input.sessionId).preparationState(input.sessionId, input.destroy),
      );
    }
    if (new URL(request.url).pathname === "/refresh-baselines") {
      await env.RUNS.getByName("fixture-baseline-refresh").refreshBaselines();
      return Response.json({ refreshed: true });
    }
    if (new URL(request.url).pathname === "/metadata") {
      const input = Schema.decodeUnknownSync(MetadataRead)(await request.json());
      return Response.json(
        await env.RUNS.getByName(`metadata:${input.source.repositoryName}`).metadata(input),
      );
    }
    if (new URL(request.url).pathname === "/prepare") {
      const input = Schema.decodeUnknownSync(Preparation)(await request.json());
      return Response.json(
        await env.RUNS.getByName(input.sessionId)
          .prepare(input)
          .catch((cause) => ({ status: "failed", failure: String(cause) })),
      );
    }
    if (new URL(request.url).pathname === "/native") {
      const input = Schema.decodeUnknownSync(SandboxTaskMachineCreate)(await request.json());
      return Response.json(
        await env.RUNS.getByName(repairSandboxTaskObjectName(input.id)).create(input),
      );
    }
    const input = Schema.decodeUnknownSync(Input)(await request.json());
    if (input.mode === "evidence")
      return Response.json(
        await env.RUNS.getByName(repairSandboxTaskObjectName(input.launch.runId)).evidence(),
      );
    if (["events", "detail", "snapshot", "break-events", "restore-events"].includes(input.mode)) {
      const object = env.RUNS.getByName(repairSandboxTaskObjectName(input.launch.runId));
      const result =
        input.mode === "events"
          ? await object.events(input.launch.runId, 0, 100)
          : input.mode === "detail"
            ? await object.inspect(input.launch.runId)
            : input.mode === "snapshot"
              ? await object.snapshot(input.launch.runId)
              : await object.eventsStorage(input.mode === "restore-events");
      console.log("LAUNCH_DONE " + input.application);
      return Response.json(result ?? null);
    }
    const database = PgClient.layer({
      url: Redacted.make(env.DATABASE.connectionString),
      applicationName: input.application,
      maxConnections: 2,
    });
    const authorization = Layer.effect(
      RepairAuthorization,
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        return {
          authorize: (state) =>
            sql<{
              allowed: boolean;
            }>`SELECT allowed, pg_sleep(${input.fault === "slow-authorization" ? 0.3 : 0})
              FROM fixture_authority WHERE run_id = ${state.runId}`.pipe(
              Effect.map((rows) => rows[0]?.allowed === true),
              Effect.mapError((cause) =>
                RepairHostError.make({ operation: "fixture.authorize", cause }),
              ),
            ),
        } satisfies RepairAuthorization["Service"];
      }),
    ).pipe(Layer.provide(database));
    const client = Layer.effect(
      RepairSandboxTaskClient,
      Effect.gen(function* () {
        const native = yield* RepairSandboxTaskClient;
        // A host may bind its principal and workspace as well as the raw task request.
        const fingerprint = (value: string) => contentHash(JSON.stringify(["fixture-host", value]));
        return {
          ...native,
          inspect: (id: Parameters<typeof native.inspect>[0]) =>
            native.inspect(id).pipe(
              Effect.map((detail): SandboxTaskDetail | null => {
                if (detail === null || input.fault !== "omit-inspect-fingerprint") return detail;
                const { requestFingerprint: _, ...legacy } = detail;
                return legacy;
              }),
              Effect.flatMap((detail) =>
                detail === null || detail.requestFingerprint === undefined
                  ? Effect.succeed(detail)
                  : Effect.map(fingerprint(detail.requestFingerprint), (requestFingerprint) => ({
                      ...detail,
                      requestFingerprint,
                    })),
              ),
            ),
          create: (value: SandboxTaskMachineCreate) =>
            Effect.gen(function* () {
              if (input.fault === "interrupt-before-create") return yield* Effect.interrupt;
              const receipt = yield* native.create(value);
              if (input.fault === "drop-create-reply")
                return yield* RepairSandboxTaskTransportError.make({
                  operation: "fixture.lost-create-reply",
                });
              return {
                ...receipt,
                requestFingerprint:
                  input.fault === "changed-create-fingerprint"
                    ? "changed-host-authority"
                    : yield* fingerprint(receipt.requestFingerprint),
              };
            }),
        };
      }),
    ).pipe(Layer.provide(RepairSandboxTaskClientLive({ binding: "RUNS" })));
    const dependencies = Layer.mergeAll(
      hostPorts,
      database,
      authorization,
      client,
      RepairExecutionRepository.layer.pipe(Layer.provide(database)),
      SandboxAgentArtifacts.layer({ binding: "ARTIFACTS" }),
      traceLayer,
    ).pipe(
      Layer.provide(
        Layer.succeed(WorkerEnvironment, {
          ...env,
          ARTIFACTS: fixtureArtifacts(env.FIXTURE_ARTIFACTS),
        }),
      ),
    );
    const operation = Effect.gen(function* () {
      if (input.mode === "start") return yield* startRepairExecution(input.launch);
      if (input.mode === "reconcile") return yield* reconcileRepairExecution;
      if (input.mode === "inspect") return yield* inspectRepairExecution;
      if (input.mode === "report") return yield* reportRepairExecution;
      if (input.mode === "retire" || input.mode === "retire-completed")
        return yield* retireRepairExecution(
          input.launch,
          input.mode === "retire-completed" ? null : "authority_revoked",
        );
      const records = yield* RepairExecutionRepository;
      if (input.mode === "retain-launch") {
        const sql = yield* SqlClient.SqlClient;
        const row = (yield* sql<{ intent: string }>`SELECT intent FROM repair_execution_receipts
          WHERE run_id = ${input.launch.runId} AND kind = 'sandbox-admission' AND key = 'launch'`)[0];
        if (row === undefined) return yield* Effect.die("Missing fixture launch receipt");
        return yield* records.retainLaunch(
          input.launch.runId,
          row.intent + (input.fault === "changed-retained-intent" ? "\n" : ""),
        );
      }
      yield* records.accept(input.launch);
      if (!(yield* authorizeRun)) return yield* Effect.die("Source fixture authority revoked");
      return yield* readRepairSource(input.launch);
    });
    const exit = await Effect.runPromiseExit(
      operation.pipe(
        Effect.provideService(RepairExecutionContext, input.launch.runId),
        Effect.provide(dependencies),
        Effect.scoped,
      ),
    );
    console.log("LAUNCH_DONE " + input.application);
    return Response.json(
      exit._tag === "Success"
        ? { exit: exit._tag, value: exit.value }
        : { exit: exit._tag, failure: Cause.pretty(exit.cause) },
    );
  },
};
