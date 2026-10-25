import { webCryptoLayer } from "@yielded/sandbox/cloudflare/Bindings";
import { Cause, Effect, Layer, Schema } from "effect";
import { DurableObjectSqlite, DurableObjectState, type WorkerEnvironment } from "effect-cf";
import { ConflictError, ServiceUnavailableError } from "../domain/errors";
import {
  makeSandboxAgentTurnLayer,
  makeRepositoryWorkspaceLayer,
  type SandboxAgentSessionOptions,
} from "@yielded/sandbox/cloudflare/TurnLayer";
import { OutboundAuthority } from "@yielded/sandbox/cloudflare/OutboundAuthority";
import {
  isRepositoryPreparationCoordinator,
  isRepositoryPreparationOwner,
  RepositoryPreparationGate,
  RepositoryPreparationObject,
  RepositoryPreparationSources,
} from "@yielded/sandbox/cloudflare/RepositoryPreparation";
import { repositoryPreparationHandlers } from "@yielded/sandbox/cloudflare/RepositoryPreparationRuntime";
import {
  reportSanitizedCause,
  withErrorReportingScope,
} from "@yielded/sandbox/observability/RuntimeDiagnostics";
import { RepairSandboxTaskObject } from "./RepairSandboxTaskObject";
import { RepairSandboxTaskStore } from "./RepairSandboxTaskStore";
import { RepairSandboxTaskControl } from "./RepairSandboxTaskControl";
import { RepairSandboxAlarms } from "./RepairSandboxAlarms";
import { RepairSandboxPreparation } from "./RepairSandboxPreparation";
import { RepairSandboxTaskNotifications } from "./RepairSandboxTaskNotifications";
import { deliverRepairSandboxTaskSettlement } from "./RepairSandboxTaskDelivery";
import {
  createRepairSandboxTask,
  inspectRepairSandboxTask,
  snapshotRepairSandboxTask,
  eventsRepairSandboxTask,
  cancelRepairSandboxTask,
  nativeRepairSandboxTask,
  forwardRepairSandboxTask,
  readRepairContinuation,
  runRepairSandboxTask,
  cleanupRepairSandboxTask,
  type RepairSandboxTurnPolicy,
} from "./RepairSandboxTaskExecution";

export interface RepairSandboxTaskOptions<
  ConfigFailure,
  RunSinkFailure = never,
  HarnessFailure = never,
  HostFailure = never,
> extends SandboxAgentSessionOptions<ConfigFailure, RunSinkFailure, HarnessFailure> {
  /** The same private coordinator namespace supplied to RepairSandboxTaskClientLive. */
  readonly coordinatorBinding: string;
  /** Host policy for model egress, optional Sentry tools and local repository preview. */
  readonly turnPolicy: RepairSandboxTurnPolicy;
  readonly preparationLayer?: Layer.Layer<RepairSandboxPreparation, HostFailure, WorkerEnvironment>;
  /** Authorize and select repository sources for event-driven checkpoint preparation. */
  readonly repositoryPreparationLayer?: Layer.Layer<
    RepositoryPreparationSources,
    HostFailure,
    WorkerEnvironment
  >;
  readonly outboundAuthorityLayer?: Layer.Layer<OutboundAuthority, HostFailure, WorkerEnvironment>;
  /** Optional durable terminal-task wake-up. Omission leaves task execution unchanged. */
  readonly taskNotificationsLayer?: Layer.Layer<
    RepairSandboxTaskNotifications,
    HostFailure,
    WorkerEnvironment
  >;
  readonly diagnosticsLayer: Layer.Layer<never, HostFailure, WorkerEnvironment>;
}

const outcome = <A, E, R>(operation: string, effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.map((value) => ({ _tag: "Success" as const, value })),
    Effect.catchCause(
      (cause): Effect.Effect<ConflictError | ServiceUnavailableError, ServiceUnavailableError> => {
        if (Cause.hasInterruptsOnly(cause))
          return Effect.failCause(
            Cause.map(cause, () =>
              ServiceUnavailableError.make({ message: "Repair sandbox execution is unavailable" }),
            ),
          );
        if (cause.reasons.length === 1) {
          const reason = cause.reasons[0]!;
          if (Cause.isFailReason(reason) && Schema.is(ConflictError)(reason.error))
            return Effect.succeed(reason.error);
        }
        const unavailable = ServiceUnavailableError.make({
          message: "Repair sandbox execution is unavailable",
        });
        return reportSanitizedCause(cause, { operation }).pipe(
          Effect.andThen(
            cause.reasons.every(Cause.isFailReason)
              ? Effect.succeed(unavailable)
              : Effect.failCause(Cause.map(cause, () => unavailable)),
          ),
        );
      },
    ),
    withErrorReportingScope,
  );

/** Accepted work and native recovery run locally on the host's own Cloudflare resources. */
export const makeRepairSandboxTaskDurableObject = <
  ConfigFailure,
  RunSinkFailure = never,
  HarnessFailure = never,
  HostFailure = never,
>(
  options: RepairSandboxTaskOptions<ConfigFailure, RunSinkFailure, HarnessFailure, HostFailure>,
) => {
  const taskRuntime = Layer.mergeAll(
    makeSandboxAgentTurnLayer(options),
    RepairSandboxTaskStore.layer,
    RepairSandboxTaskControl.layer,
    RepairSandboxTaskObject.layer({ binding: options.coordinatorBinding }),
    RepositoryPreparationObject.layer({ binding: options.coordinatorBinding }),
    RepositoryPreparationGate.layer,
    options.preparationLayer ?? RepairSandboxPreparation.noopLayer,
    options.outboundAuthorityLayer ?? OutboundAuthority.noneLayer,
    options.taskNotificationsLayer ?? Layer.empty,
  ).pipe(
    Layer.provideMerge(Layer.merge(DurableObjectSqlite.layer(), webCryptoLayer)),
    Layer.tapCause((cause) =>
      reportSanitizedCause(cause, { operation: "RepairSandboxTask.initialize" }).pipe(
        withErrorReportingScope,
      ),
    ),
    Layer.provideMerge(options.diagnosticsLayer),
  );
  const preparationRuntime = Layer.mergeAll(
    makeRepositoryWorkspaceLayer(options),
    RepositoryPreparationObject.layer({ binding: options.coordinatorBinding }),
    RepositoryPreparationGate.layer,
  ).pipe(Layer.provideMerge(options.diagnosticsLayer));
  // A task owner retains task services for its lifetime. Preparation owners never
  // initialize their tables, model provider, run sink, preparation or notifications.
  const runtime = Layer.unwrap(
    Effect.map(DurableObjectState.DurableObjectState, (state) =>
      isRepositoryPreparationOwner(state.id.name) ? preparationRuntime : taskRuntime,
    ),
  );
  const preparation = repositoryPreparationHandlers(
    options.repositoryPreparationLayer ?? RepositoryPreparationSources.unconfiguredLayer,
  );
  const taskAlarms = RepairSandboxAlarms.handlers({
    run: () =>
      runRepairSandboxTask(options.turnPolicy).pipe(
        Effect.provide(taskRuntime),
        withErrorReportingScope,
      ),
    // Delivery failure belongs to the alarm retry owner, never to model execution or alerts.
    settled: ({ payload }) =>
      deliverRepairSandboxTaskSettlement(payload).pipe(Effect.provide(taskRuntime)),
    cleanup: () =>
      cleanupRepairSandboxTask.pipe(
        Effect.provide(taskRuntime),
        Effect.tapCause((cause) =>
          reportSanitizedCause(cause, { operation: "RepairSandboxTask.cleanup" }),
        ),
        withErrorReportingScope,
      ),
  });
  return RepairSandboxTaskObject.make(runtime, {
    rpc: {
      ...preparation.rpc,
      // The selected task runtime is already memoized in this owner's parent layer.
      create: (request) =>
        outcome(
          "RepairSandboxTask.create",
          createRepairSandboxTask(request).pipe(Effect.provide(taskRuntime)),
        ),
      inspect: (id) =>
        outcome(
          "RepairSandboxTask.inspect",
          inspectRepairSandboxTask(id).pipe(Effect.provide(taskRuntime)),
        ),
      snapshot: (id) =>
        outcome(
          "RepairSandboxTask.snapshot",
          snapshotRepairSandboxTask(id).pipe(Effect.provide(taskRuntime)),
        ),
      events: (id, after, limit) =>
        outcome(
          "RepairSandboxTask.events",
          eventsRepairSandboxTask(id, after, limit).pipe(Effect.provide(taskRuntime)),
        ),
      cancel: (id, commandId, request) =>
        outcome(
          "RepairSandboxTask.cancel",
          cancelRepairSandboxTask(id, commandId, request).pipe(Effect.provide(taskRuntime)),
        ),
      native: (id, request) =>
        outcome(
          "RepairSandboxTask.native",
          nativeRepairSandboxTask(id, request).pipe(Effect.provide(taskRuntime)),
        ),
      continuation: (request) =>
        outcome(
          "RepairSandboxTask.continuation",
          readRepairContinuation(request).pipe(Effect.provide(taskRuntime)),
        ),
      forward: (id, purpose, operationId, request) =>
        forwardRepairSandboxTask(id, purpose, operationId, new Request(request)).pipe(
          Effect.provide(taskRuntime),
          Effect.catchCause((cause) => {
            if (Cause.hasInterruptsOnly(cause))
              return Effect.failCause(
                Cause.map(cause, () =>
                  ServiceUnavailableError.make({
                    message: "Repair sandbox execution is unavailable",
                  }),
                ),
              );
            const reason = cause.reasons[0];
            if (
              cause.reasons.length === 1 &&
              reason !== undefined &&
              Cause.isFailReason(reason) &&
              Schema.is(ConflictError)(reason.error)
            )
              return Effect.succeed(new Response(null, { status: 409 }));
            return reportSanitizedCause(cause, { operation: "RepairSandboxTask.forward" }).pipe(
              Effect.andThen(
                cause.reasons.every(Cause.isFailReason)
                  ? Effect.succeed(new Response(null, { status: 503 }))
                  : Effect.failCause(
                      Cause.map(cause, () =>
                        ServiceUnavailableError.make({
                          message: "Repair sandbox execution is unavailable",
                        }),
                      ),
                    ),
              ),
            );
          }),
          withErrorReportingScope,
        ),
    },
    alarms: {
      ...taskAlarms,
      run: Effect.gen(function* () {
        const state = yield* DurableObjectState.DurableObjectState;
        if (isRepositoryPreparationCoordinator(state.id.name)) return yield* preparation.alarms;
        return yield* taskAlarms.run;
      }),
    },
  });
};
