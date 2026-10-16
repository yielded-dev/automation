import { DurableObject as NativeDurableObject } from "cloudflare:workers";
import { Cause, DateTime, Effect, Layer, Schema } from "effect";
import { DurableObject, DurableObjectSqlite, WorkerEnvironment } from "effect-cf";
import { SqlClient } from "effect/sql";
import { makeRepairSandboxTaskDurableObject } from "../../src/cloudflare/RepairSandboxTaskDurableObject";
import { RepairSandboxAlarms } from "../../src/cloudflare/RepairSandboxAlarms";
import {
  RepairSandboxState,
  RepairSandboxTaskStore,
} from "../../src/cloudflare/RepairSandboxTaskStore";
import {
  RepairSandboxTaskObject,
  repairSandboxTaskObjectName,
} from "../../src/cloudflare/RepairSandboxTaskObject";
import { RepairSandboxTaskControl } from "../../src/cloudflare/RepairSandboxTaskControl";
import {
  cancelRepairSandboxTask,
  createRepairSandboxTask,
} from "../../src/cloudflare/RepairSandboxTaskExecution";
import { deliverRepairSandboxTaskSettlement } from "../../src/cloudflare/RepairSandboxTaskDelivery";
import {
  RepairSandboxTaskSettled,
  RepairSandboxTaskNotificationError,
  RepairSandboxTaskNotifications,
} from "../../src/cloudflare/RepairSandboxTaskNotifications";
import { SandboxTaskMachineCreate } from "@yielded/automation/server/sandbox-contracts/sandbox-task-machine";
import { SandboxTaskLaunchReceipt } from "@yielded/automation/server/sandbox-contracts/sandbox-task-admission";
import { SandboxTaskEvent } from "@yielded/automation/server/sandbox-contracts/sandbox-task-event";
import { SandboxAgentConfig } from "@yielded/sandbox/cloudflare/Config";
import {
  SessionStateStore,
  StoredCodexTurn,
  StoredPipelineTurn,
} from "@yielded/sandbox/cloudflare/SessionStateStore";
import { Harness } from "@yielded/sandbox/harness/Harness";
import { fixtureArtifacts } from "../../../../test/fixtures/artifacts.worker";

interface Env {
  RUNS: DurableObjectNamespace<CompletionRun>;
  QUIET_RUNS: DurableObjectNamespace<QuietRun>;
  FIXTURE_ARTIFACTS: Fetcher;
  NOTIFICATIONS: Fetcher;
}

const notificationsLayer = Layer.effect(
  RepairSandboxTaskNotifications,
  Effect.gen(function* () {
    const env = (yield* WorkerEnvironment) as Env;
    return RepairSandboxTaskNotifications.of({
      settled: Effect.fnUntraced(
        function* (event) {
          const body = yield* Schema.encodeEffect(Schema.fromJsonString(RepairSandboxTaskSettled))(
            event,
          );
          const response = yield* Effect.tryPromise({
            try: () =>
              env.NOTIFICATIONS.fetch("http://fixture.invalid/settled", { method: "POST", body }),
            catch: (cause) => RepairSandboxTaskNotificationError.make({ cause }),
          });
          if (!response.ok)
            return yield* RepairSandboxTaskNotificationError.make({
              cause: new Error("Injected host queue rejection"),
            });
        },
        Effect.mapError((cause) => RepairSandboxTaskNotificationError.make({ cause })),
      ),
    });
  }),
);

const Seed = Schema.Struct({
  request: SandboxTaskMachineCreate,
  pipeline: Schema.optionalKey(StoredPipelineTurn),
  codex: Schema.optionalKey(StoredCodexTurn),
  blockCompletion: Schema.optionalKey(Schema.Boolean),
  cancelBeforeLaunch: Schema.optionalKey(Schema.Boolean),
});

// Recover real persisted task receipts. Any model redispatch is a fixture failure.
const makeRun = (notify: boolean) => {
  const fixtureTaskServices = Layer.mergeAll(
    RepairSandboxTaskStore.layer,
    RepairSandboxTaskControl.layer,
    RepairSandboxTaskObject.layer({ binding: notify ? "RUNS" : "QUIET_RUNS" }),
  ).pipe(Layer.provideMerge(DurableObjectSqlite.layer()));
  const Base = makeRepairSandboxTaskDurableObject({
    bindings: { artifacts: "ARTIFACTS", sandbox: "SANDBOXES" },
    coordinatorBinding: notify ? "RUNS" : "QUIET_RUNS",
    configLayer: SandboxAgentConfig.layer({
      repositorySetupPath: ".fixture-setup.json",
      defaultModel: "unused",
      artifactsNamespace: "fixture",
      artifactsHostname: "fixture.invalid",
    }),
    diagnosticsLayer: Layer.empty,
    harnessLayer: Layer.succeed(Harness, {
      name: "codex",
      binary: "unused",
      version: "fixture-no-model",
      runTurn: () => Effect.die("COMPLETION_UNEXPECTED_MODEL_REPLAY"),
    }),
    ...(notify ? { taskNotificationsLayer: notificationsLayer } : {}),
    turnPolicy: { restrictModelRequests: true, sentryMcp: false, startRepositoryPreview: false },
  });

  return class extends Base {
    constructor(state: DurableObjectState, env: Env) {
      super(state, { ...env, ARTIFACTS: fixtureArtifacts(env.FIXTURE_ARTIFACTS) });
    }

    seed(input: unknown) {
      return this[DurableObject.RunSymbol](
        Effect.gen(function* () {
          const value = yield* Schema.decodeUnknownEffect(Seed)(input);
          if (value.cancelBeforeLaunch)
            return yield* cancelRepairSandboxTask(
              value.request.id,
              value.request.commandId,
              value.request,
            );
          const store = yield* SessionStateStore;
          if (value.pipeline !== undefined) yield* store.pipelineTurn.put(value.pipeline);
          if (value.codex !== undefined) yield* store.codexTurn.put(value.codex);
          if (value.blockCompletion) {
            yield* (yield* RepairSandboxAlarms).transaction(() => Effect.void);
            const sql = yield* SqlClient.SqlClient;
            yield* sql`CREATE TRIGGER fixture_reject_completion BEFORE INSERT ON effect_cf_scheduled_alarms
            WHEN NEW.tag = 'settled' BEGIN SELECT RAISE(ABORT, 'Injected completion commit failure'); END`;
          }
          return yield* Schema.encodeEffect(SandboxTaskLaunchReceipt)(
            yield* createRepairSandboxTask(value.request),
          );
        }).pipe(Effect.provide(fixtureTaskServices)),
      );
    }

    evidence() {
      return this[DurableObject.RunSymbol](
        Effect.gen(function* () {
          const records = yield* RepairSandboxTaskStore;
          const alarms = yield* RepairSandboxAlarms;
          const state = yield* records.required;
          const pending = yield* Effect.forEach(
            [
              { tag: "run", id: "turn" },
              { tag: "settled", id: "host" },
              { tag: "cleanup", id: "compute" },
            ] as const,
            (ref) =>
              Effect.gen(function* () {
                const alarm = yield* alarms.getAlarmStatus(ref);
                return {
                  ...ref,
                  alarm:
                    alarm === undefined
                      ? null
                      : {
                          attempts: alarm.attempts,
                          parked: alarm.parked,
                          runAt: DateTime.toEpochMillis(alarm.runAt),
                          retryAt:
                            alarm.retryAt === undefined
                              ? null
                              : DateTime.toEpochMillis(alarm.retryAt),
                        },
                };
              }),
          );
          return {
            state: yield* Schema.encodeEffect(RepairSandboxState)(state),
            pending,
            events: yield* Schema.encodeEffect(Schema.Array(SandboxTaskEvent))(
              yield* records.events(0, 100),
            ),
          };
        }).pipe(Effect.provide(fixtureTaskServices)),
      );
    }

    replay() {
      return this[DurableObject.RunSymbol](
        Effect.gen(function* () {
          return yield* Schema.encodeEffect(SandboxTaskLaunchReceipt)(
            yield* createRepairSandboxTask(
              (yield* (yield* RepairSandboxTaskStore).required).request,
            ),
          );
        }).pipe(Effect.provide(fixtureTaskServices)),
      );
    }

    allowCompletion() {
      return this[DurableObject.RunSymbol](
        Effect.gen(function* () {
          yield* (yield* SqlClient.SqlClient)`DROP TRIGGER fixture_reject_completion`;
          yield* (yield* RepairSandboxAlarms).scheduleAlarm({
            tag: "run",
            id: "turn",
            payload: null,
            runAt: yield* DateTime.now,
          });
        }).pipe(Effect.provide(fixtureTaskServices)),
      );
    }

    cancelTask() {
      return this[DurableObject.RunSymbol](
        Effect.gen(function* () {
          const state = yield* (yield* RepairSandboxTaskStore).required;
          return yield* cancelRepairSandboxTask(state.request.id, state.request.commandId, null);
        }).pipe(Effect.provide(fixtureTaskServices)),
      );
    }

    mixedCause() {
      return this[DurableObject.RunSymbol](
        Effect.gen(function* () {
          const state = yield* (yield* RepairSandboxTaskStore).required;
          const failure = RepairSandboxTaskNotificationError.make({
            cause: new Error("private failure"),
          });
          const defect = new Error("private defect");
          const cause = Cause.fromReasons([
            Cause.makeFailReason(failure),
            Cause.makeDieReason(defect),
            Cause.makeInterruptReason(71),
          ]);
          const exit = yield* deliverRepairSandboxTaskSettlement({ taskId: state.request.id }).pipe(
            Effect.provideService(RepairSandboxTaskNotifications, {
              settled: () => Effect.failCause(cause),
            }),
            Effect.exit,
          );
          return exit._tag === "Success"
            ? { exit: exit._tag }
            : {
                exit: exit._tag,
                reasons: exit.cause.reasons.map((reason) => reason._tag),
                originalFailure: exit.cause.reasons.some(
                  (reason) =>
                    Cause.isFailReason(reason) &&
                    "cause" in reason.error &&
                    reason.error.cause === failure,
                ),
                originalDefect: exit.cause.reasons.some(
                  (reason) => Cause.isDieReason(reason) && reason.defect === defect,
                ),
                originalInterrupt: exit.cause.reasons.some(
                  (reason) => Cause.isInterruptReason(reason) && reason.fiberId === 71,
                ),
              };
        }).pipe(Effect.provide(fixtureTaskServices)),
      );
    }
  };
};

export class CompletionRun extends makeRun(true) {}
export class QuietRun extends makeRun(false) {}

// Foreign compute cleanup endpoint only. No container or checkout implementation is substituted.
export class CleanupSandbox extends NativeDurableObject {
  locate() {
    return { _tag: "Success", value: undefined };
  }
  destroy() {
    console.log("COMPLETION_CLEANUP " + this.ctx.id.toString());
    return { _tag: "Success", value: undefined };
  }
}

const Input = Schema.Struct({
  id: Schema.String.check(Schema.isUUID()),
  quiet: Schema.optionalKey(Schema.Boolean),
  seed: Schema.optionalKey(Seed),
});

export default {
  async fetch(request: Request, env: Env) {
    const input = Schema.decodeUnknownSync(Input)(await request.json());
    const run = (input.quiet ? env.QUIET_RUNS : env.RUNS).getByName(
      repairSandboxTaskObjectName(input.id),
    );
    switch (new URL(request.url).pathname) {
      case "/completion/seed":
        return Response.json(await run.seed(input.seed));
      case "/completion/replay":
        return Response.json(await run.replay());
      case "/completion/allow":
        await run.allowCompletion();
        return Response.json({ allowed: true });
      case "/completion/cancel":
        return Response.json(await run.cancelTask());
      case "/completion/mixed":
        return Response.json(await run.mixedCause());
      default:
        return Response.json(await run.evidence());
    }
  },
};
