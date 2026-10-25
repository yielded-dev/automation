import { Cause, Clock, DateTime, Deferred, Effect, Fiber, Ref, Schema, Semaphore } from "effect";
import { v5 as uuidV5 } from "uuid";
import { ConflictError } from "../domain/errors";
import { contentHash } from "../server/hash";
import { SandboxTaskMachineCreate } from "../server/sandbox-contracts/sandbox-task-machine";
import { SandboxTaskLaunchReceipt } from "../server/sandbox-contracts/sandbox-task-admission";
import { SandboxTaskDetail } from "../server/sandbox-contracts/sandbox-task";
import { TurnId } from "../server/sandbox-contracts/ids";
import {
  SandboxTaskExecutionSnapshot,
  type SandboxTaskNativeRequest,
} from "../server/sandbox-contracts/sandbox-task-native";
import { AgentEvent } from "@yielded/sandbox/Contract";
import { withPhaseEvents } from "@yielded/sandbox/tracing/PhaseEvents";
import { AgentEventSink, AgentEventSinkError } from "@yielded/sandbox/cloudflare/AgentEventSink";
import { runTurn } from "@yielded/sandbox/cloudflare/MessagePipeline";
import { SessionStateStore } from "@yielded/sandbox/cloudflare/SessionStateStore";
import { SessionSandbox } from "@yielded/sandbox/SessionSandbox";
import { TurnWorkspaceServices } from "@yielded/sandbox/cloudflare/WorkspaceProcesses";
import {
  RepositoryPersistenceCheckpoint,
  RepositoryPersistenceCheckpointError,
} from "@yielded/sandbox/cloudflare/RepositoryPersistenceCheckpoint";
import { reportSanitizedCause } from "@yielded/sandbox/observability/RuntimeDiagnostics";
import { RepairSandboxAlarms } from "./RepairSandboxAlarms";
import { RepairSandboxPreparation } from "./RepairSandboxPreparation";
import { RepairSandboxTaskControl } from "./RepairSandboxTaskControl";
import { RepairSandboxTaskStore } from "./RepairSandboxTaskStore";
import { repairSandboxTaskSettlement } from "./RepairSandboxTaskDelivery";
import { preserveRepairSandboxFailure } from "./RepairSandboxTaskError";
import { repairSandboxTaskFailure } from "./RepairSandboxTaskFailure";
import { RepairSandboxTaskObject, repairSandboxTaskObjectName } from "./RepairSandboxTaskObject";
import { repairSandboxTaskSandbox } from "./RepairSandboxTaskSandbox";
import { retainRepairSandboxCheckpoint } from "./RepairSandboxTaskRepository";
import {
  runNativeOperation,
  readSandboxTaskExecutionSnapshot,
  forwardSandboxTaskNativeRequest,
} from "./SandboxTaskNativeTransport";

const conflict = (message: string) => ConflictError.make({ message });
const turnIdFor = (request: SandboxTaskMachineCreate) =>
  TurnId.make(uuidV5(`task-turn:${request.commandId}`, request.id));
const deadline = (millis: number) => DateTime.makeUnsafe(millis);
const owned = Effect.fnUntraced(function* (id: string) {
  const state = yield* (yield* RepairSandboxTaskStore).required;
  if (state.request.id !== id)
    return yield* conflict("The request identifies a different execution");
  return state;
});
const live = Effect.gen(function* () {
  const state = yield* (yield* RepairSandboxTaskStore).required;
  if (state.cancelled || state.request.deadlineAtMillis <= (yield* Clock.currentTimeMillis))
    return yield* conflict("The repair execution authority has expired");
});

export const readRepairContinuation = Effect.fn("RepairSandboxTask.continuation")(function* (
  request: SandboxTaskMachineCreate,
) {
  const state = yield* (yield* RepairSandboxTaskStore).required;
  const prior = state.request;
  const saved = state.savedCheckpoint;
  if (
    request.continuation?.previousTaskId !== prior.id ||
    request.id === prior.id ||
    request.execution === "commands" ||
    prior.execution === "commands" ||
    state.status !== "succeeded" ||
    request.source._tag !== "SeedRepository" ||
    prior.source._tag === "None" ||
    request.repair === undefined ||
    prior.repair === undefined ||
    request.repair.repositoryId !== prior.repair.repositoryId ||
    request.repair.repairId !== prior.repair.repairId ||
    saved === null ||
    request.source.commitSha !== saved.commitSha ||
    request.continuation.checkoutHead !== saved.commitSha ||
    request.source.repositoryName !== saved.repositoryName ||
    request.source.artifactsRepositoryId !== saved.artifactsRepositoryId ||
    saved.baseHead !== prior.source.commitSha
  )
    return yield* conflict("The continuation does not identify a completed owned candidate");
  if ((yield* Ref.get((yield* RepairSandboxTaskControl).active)) !== undefined)
    return yield* conflict("The continuation still owns active execution");
  const store = yield* SessionStateStore;
  const pipeline = yield* store.pipelineTurn.get;
  const codex = yield* store.codexTurn.get;
  const fork = yield* store.repositoryFork.get;
  if (
    saved.turnId !== turnIdFor(prior) ||
    pipeline?.id !== saved.turnId ||
    pipeline.persisted?.head !== saved.commitSha ||
    codex?.id !== saved.turnId ||
    codex.result?.terminalEventType !== "turn.completed" ||
    (yield* store.verifiedHead.get) !== saved.commitSha ||
    (prior.source._tag === "AttachedRepository"
      ? prior.source.repositoryName !== saved.repositoryName ||
        prior.source.artifactsRepositoryId !== saved.artifactsRepositoryId
      : fork?.initialized !== true ||
        fork.forkName !== saved.repositoryName ||
        fork.repositoryId !== saved.artifactsRepositoryId ||
        saved.repositoryName === prior.source.repositoryName)
  )
    return yield* conflict(
      "The continuation checkpoint does not match the retained execution receipts",
    );
  const checkpoint = yield* store.workspaceCheckpoint.get;
  const setup = yield* store.repositorySetupReceipt.get;
  return checkpoint === null ||
    setup?.prepared !== true ||
    setup.report.state === "failed" ||
    setup.fingerprint !== checkpoint.setupFingerprint
    ? null
    : { checkpoint, report: setup.report };
}, preserveRepairSandboxFailure);

const continuationFor = Effect.fnUntraced(function* (request: SandboxTaskMachineCreate) {
  if (request.continuation === undefined) return null;
  const objects = yield* RepairSandboxTaskObject;
  const outcome = yield* objects
    .byName(repairSandboxTaskObjectName(request.continuation.previousTaskId))
    .continuation(request);
  if (outcome._tag !== "Success") return yield* outcome;
  return outcome.value;
});

const admit = Effect.fnUntraced(function* (request: SandboxTaskMachineCreate) {
  yield* Schema.decodeEffect(Schema.String.check(Schema.isUUID()))(request.id);
  if (
    request.execution !== "commands" &&
    (request.source._tag === "None" || request.repair === undefined)
  )
    return yield* conflict("Model execution requires frozen source and repair lineage");
  const records = yield* RepairSandboxTaskStore;
  const fingerprint = yield* contentHash(
    yield* Schema.encodeEffect(Schema.fromJsonString(SandboxTaskMachineCreate))(request),
  );
  const previous = yield* records.read;
  if (previous === undefined) yield* continuationFor(request);
  const now = yield* Clock.currentTimeMillis;
  // Even an admission with no compute retains a deadline cleanup owner.
  return yield* (yield* RepairSandboxAlarms).transaction((alarms) =>
    Effect.gen(function* () {
      const state = yield* records.accept(request, fingerprint, now);
      if (previous === undefined)
        yield* alarms.scheduleAlarm({
          tag: "cleanup",
          id: "compute",
          payload: null,
          runAt: deadline(request.deadlineAtMillis),
        });
      return state;
    }),
  );
});

export const createRepairSandboxTask = Effect.fn("RepairSandboxTask.create")(function* (
  request: SandboxTaskMachineCreate,
) {
  const control = yield* RepairSandboxTaskControl;
  return yield* Semaphore.withPermit(
    control.lifecycle,
    Effect.gen(function* () {
      const accepted = yield* admit(request);
      if (accepted.launchReceipt !== null) return accepted.launchReceipt;
      const cancelled =
        accepted.cancelled || request.deadlineAtMillis <= (yield* Clock.currentTimeMillis);
      const receipt = SandboxTaskLaunchReceipt.make({
        commandId: request.commandId,
        requestFingerprint: accepted.fingerprint,
        result: {
          id: request.id,
          outcome: cancelled ? "cancelled" : "launched",
          ...(request.execution === "commands" ? {} : { turnId: turnIdFor(request) }),
        },
      });
      const records = yield* RepairSandboxTaskStore;
      return yield* (yield* RepairSandboxAlarms).transaction((alarms) =>
        Effect.gen(function* () {
          const previous = yield* records.required;
          const current = yield* records.update((current) => ({
            ...current,
            launchReceipt: receipt,
            cancelled,
            status: cancelled
              ? "cancelled"
              : request.execution === "commands"
                ? "succeeded"
                : "accepted",
          }));
          const notification = yield* repairSandboxTaskSettlement(previous, current);
          if (notification !== undefined) yield* alarms.scheduleAlarm(notification);
          if (cancelled)
            yield* alarms.scheduleAlarm({
              tag: "cleanup",
              id: "compute",
              payload: null,
              runAt: deadline(yield* Clock.currentTimeMillis),
            });
          else if (request.execution !== "commands")
            yield* alarms.scheduleAlarm({
              tag: "run",
              id: "turn",
              payload: null,
              runAt: deadline(yield* Clock.currentTimeMillis),
            });
          return receipt;
        }),
      );
    }),
  );
}, preserveRepairSandboxFailure);

export interface RepairSandboxTurnPolicy {
  readonly restrictModelRequests: boolean;
  readonly sentryMcp: boolean;
  readonly startRepositoryPreview: boolean;
}

const execute = (policy: RepairSandboxTurnPolicy) =>
  Effect.gen(function* () {
    const records = yield* RepairSandboxTaskStore;
    const state = yield* records.required;
    const request = state.request;
    const turnId = turnIdFor(request);
    const store = yield* SessionStateStore;
    const continuationCheckpoint = yield* continuationFor(request);
    const callbacks =
      yield* Effect.context<Effect.Services<ReturnType<typeof retainRepairSandboxCheckpoint>>>();
    yield* live;
    if ((yield* store.pipelineTurn.get) === null)
      yield* (yield* RepairSandboxPreparation).prepare(request);
    yield* runTurn({
      ...policy,
      turnId,
      recover: true,
      sessionId: request.id,
      ...(request.repair === undefined ? {} : { repositoryId: request.repair.repositoryId }),
      text: request.prompt,
      repository: request.source,
      requiredCommits: request.requiredCommits,
      initialCheckout: request.continuation?.checkoutHead,
      ...(request.continuation === undefined ? {} : { continuationCheckpoint }),
      deadlineAtMs: request.deadlineAtMillis,
      outputSchema: request.outputSchema,
      model: request.modelSettings.model,
      reasoningEffort: request.modelSettings.reasoningEffort,
      fast: request.modelSettings.fast,
      containerSize: request.containerSize,
      startedAtMillis: state.createdAtMillis,
    }).pipe(
      withPhaseEvents(state.createdAtMillis),
      Effect.provideService(TurnWorkspaceServices, []),
      Effect.provideService(RepositoryPersistenceCheckpoint, {
        prepared: () =>
          live.pipe(
            Effect.provideService(RepairSandboxTaskStore, records),
            Effect.catchCause((cause) =>
              Effect.failCause(
                Cause.map(cause, (error) =>
                  RepositoryPersistenceCheckpointError.make({ cause: error }),
                ),
              ),
            ),
          ),
      }),
      Effect.provideService(AgentEventSink, {
        emit: (event) =>
          Effect.gen(function* () {
            if (event.type === "agent.archived") return;
            if (event.type === "agent.persisted")
              yield* retainRepairSandboxCheckpoint(event.head, turnId);
            const payload =
              event.type === "agent.completed"
                ? { timings: event.timings }
                : yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(
                    yield* Schema.encodeEffect(Schema.fromJsonString(AgentEvent))(event),
                  );
            yield* records.append(
              turnId,
              event.type === "codex.event" ? "codex" : "runtime",
              event.type === "codex.event" ? event.event.type : event.type,
              event.type === "codex.event"
                ? yield* Schema.decodeUnknownEffect(Schema.Json)(event.event)
                : payload,
            );
          }).pipe(
            Effect.catchCause((cause) =>
              Effect.failCause(
                Cause.map(cause, (error) => new AgentEventSinkError({ cause: error })),
              ),
            ),
            Effect.provide(callbacks),
          ),
      }),
      Effect.timeout(Math.max(1, request.deadlineAtMillis - (yield* Clock.currentTimeMillis))),
    );
    const pipeline = yield* store.pipelineTurn.get;
    const codex = yield* store.codexTurn.get;
    if (
      pipeline?.id !== turnId ||
      pipeline.persisted === null ||
      codex?.id !== turnId ||
      codex.result?.terminalEventType !== "turn.completed"
    )
      return yield* conflict("The pipeline did not retain this turn's completed candidate");
    yield* (yield* RepairSandboxAlarms).transaction((alarms) =>
      Effect.gen(function* () {
        const previous = yield* records.required;
        const completed = yield* records.update((current) =>
          current.cancelled ? current : { ...current, status: "succeeded" },
        );
        yield* alarms.cancelAlarm({ tag: "run", id: "turn" });
        const notification = yield* repairSandboxTaskSettlement(previous, completed);
        if (notification !== undefined) yield* alarms.scheduleAlarm(notification);
      }),
    );
  });

/** The alarm owns the awaited turn; a prearmed successor recovers isolate loss with the same native receipt. */
export const runRepairSandboxTask = (policy: RepairSandboxTurnPolicy) =>
  Effect.gen(function* () {
    const control = yield* RepairSandboxTaskControl;
    const records = yield* RepairSandboxTaskStore;
    const alarms = yield* RepairSandboxAlarms;
    const fiber = yield* Semaphore.withPermit(
      control.lifecycle,
      Effect.gen(function* () {
        const existing = yield* Ref.get(control.active);
        if (existing !== undefined) return existing;
        const state = yield* records.required;
        if (state.cancelled || (state.status !== "accepted" && state.status !== "running"))
          return undefined;
        const now = yield* Clock.currentTimeMillis;
        if (state.request.deadlineAtMillis <= now) {
          yield* alarms.scheduleAlarm({
            tag: "cleanup",
            id: "compute",
            payload: null,
            runAt: deadline(now),
          });
          return undefined;
        }
        yield* alarms.transaction((mutations) =>
          Effect.gen(function* () {
            yield* records.update((current) => ({ ...current, status: "running" }));
            yield* mutations.scheduleAlarm({
              tag: "run",
              id: "turn",
              payload: null,
              runAt: deadline(Math.min(now + 60_000, state.request.deadlineAtMillis)),
              progress: yield* records.executionProgress,
            });
          }),
        );
        const ready = yield* Deferred.make<void>();
        const running = yield* Effect.forkDetach(
          Deferred.await(ready).pipe(
            Effect.andThen(execute(policy)),
            Effect.catchCause((cause) =>
              Effect.gen(function* () {
                const current = yield* records.required;
                if (current.cancelled) return;
                const codex = yield* (yield* SessionStateStore).codexTurn.get;
                const event = repairSandboxTaskFailure(cause, codex);
                if (event?.type === "agent.failed") {
                  yield* alarms.transaction((mutations) =>
                    Effect.gen(function* () {
                      const saved = yield* records.required;
                      if (saved.cancelled || saved.status === "failed") return;
                      const failed = yield* records.update((saved) => ({
                        ...saved,
                        status: "failed",
                      }));
                      yield* records.append(
                        turnIdFor(current.request),
                        "runtime",
                        event.type,
                        event,
                      );
                      yield* mutations.cancelAlarm({ tag: "run", id: "turn" });
                      const notification = yield* repairSandboxTaskSettlement(saved, failed);
                      if (notification !== undefined) yield* mutations.scheduleAlarm(notification);
                    }),
                  );
                  yield* reportSanitizedCause(cause, { operation: "RepairSandboxTask.run" });
                  return;
                }
                if (event !== undefined)
                  yield* records.append(turnIdFor(current.request), "runtime", event.type, event);
                return yield* Effect.failCause(cause);
              }).pipe(preserveRepairSandboxFailure),
            ),
            Effect.ensuring(Ref.set(control.active, undefined)),
          ),
        );
        yield* Ref.set(control.active, running);
        yield* Deferred.succeed(ready, undefined);
        return running;
      }),
    );
    if (fiber !== undefined)
      yield* Fiber.join(fiber).pipe(Effect.onInterrupt(() => Fiber.interrupt(fiber)));
  }).pipe(
    preserveRepairSandboxFailure,
    Effect.tapCause((cause) => reportSanitizedCause(cause, { operation: "RepairSandboxTask.run" })),
  );

export const cleanupRepairSandboxTask = Effect.gen(function* () {
  const control = yield* RepairSandboxTaskControl;
  const records = yield* RepairSandboxTaskStore;
  const state = yield* Semaphore.withPermit(
    control.lifecycle,
    Effect.gen(function* () {
      const current = yield* records.required;
      if (!current.cancelled && current.request.deadlineAtMillis > (yield* Clock.currentTimeMillis))
        return undefined;
      return yield* (yield* RepairSandboxAlarms).transaction((alarms) =>
        Effect.gen(function* () {
          const previous = yield* records.required;
          const cancelled = yield* records.update((saved) => ({
            ...saved,
            cancelled: true,
            status:
              saved.status === "succeeded" || saved.status === "failed"
                ? saved.status
                : "cancelled",
          }));
          yield* alarms.cancelAlarm({ tag: "run", id: "turn" });
          const notification = yield* repairSandboxTaskSettlement(previous, cancelled);
          if (notification !== undefined) yield* alarms.scheduleAlarm(notification);
          return cancelled;
        }),
      );
    }),
  );
  if (state === undefined) return;
  const active = yield* Ref.get(control.active);
  if (active !== undefined) yield* Fiber.interrupt(active);
  const results = yield* Effect.forEach(
    ["turn", "validation", "export", "source"] as const,
    (purpose) =>
      repairSandboxTaskSandbox(state.request.id, purpose).pipe(
        Effect.flatMap((sandbox) => sandbox.destroy),
        Effect.exit,
      ),
    { concurrency: 4 },
  );
  let failure: Cause.Cause<
    | Effect.Error<ReturnType<typeof repairSandboxTaskSandbox>>
    | import("@yielded/sandbox/cloudflare/NativeSandbox").SandboxOperationError
  > = Cause.empty;
  for (const result of results)
    if (result._tag === "Failure") failure = Cause.combine(failure, result.cause);
  if (failure.reasons.length > 0) return yield* Effect.failCause(failure);
}).pipe(preserveRepairSandboxFailure);

export const cancelRepairSandboxTask = Effect.fn("RepairSandboxTask.cancel")(function* (
  id: string,
  commandId: string,
  request: SandboxTaskMachineCreate | null,
) {
  const control = yield* RepairSandboxTaskControl;
  yield* Semaphore.withPermit(
    control.lifecycle,
    Effect.gen(function* () {
      if (request !== null) {
        if (request.id !== id) return yield* conflict("Cancellation admission identity changed");
        yield* admit(request);
      }
      yield* owned(id);
      const records = yield* RepairSandboxTaskStore;
      const fingerprint = yield* contentHash(`${id}\0${commandId}`);
      yield* (yield* RepairSandboxAlarms).transaction((alarms) =>
        Effect.gen(function* () {
          const previous = yield* records.required;
          const cancelled = yield* records.cancel(commandId, fingerprint);
          yield* alarms.cancelAlarm({ tag: "run", id: "turn" });
          yield* alarms.scheduleAlarm({
            tag: "cleanup",
            id: "compute",
            payload: null,
            runAt: deadline(yield* Clock.currentTimeMillis),
          });
          const notification = yield* repairSandboxTaskSettlement(previous, cancelled);
          if (notification !== undefined) yield* alarms.scheduleAlarm(notification);
        }),
      );
    }),
  );
  yield* cleanupRepairSandboxTask;
  return { id, commandId, cancelled: true as const };
}, preserveRepairSandboxFailure);

export const inspectRepairSandboxTask = Effect.fn("RepairSandboxTask.inspect")(function* (
  id: string,
) {
  const state = yield* (yield* RepairSandboxTaskStore).read;
  if (state === undefined) return null;
  if (state.request.id !== id)
    return yield* conflict("The request identifies a different execution");
  return SandboxTaskDetail.make({
    id: state.request.id,
    requestFingerprint: state.fingerprint,
    ...(state.request.repair === undefined
      ? {}
      : { launchAssociation: { _tag: "Repair", ...state.request.repair } }),
    // Cleanup retires compute, not the confirmed failure needed by host reconciliation.
    status:
      state.status === "failed"
        ? "failed"
        : state.cancelled
          ? "destroyed"
          : state.status === "succeeded"
            ? "idle"
            : state.status === "running"
              ? "running"
              : "ready",
    activeTurnId:
      !state.cancelled && (state.status === "accepted" || state.status === "running")
        ? turnIdFor(state.request)
        : null,
    ...(state.savedCheckpoint === null ? {} : { savedCheckpoint: state.savedCheckpoint }),
    latestSequence: state.latestSequence,
  });
}, preserveRepairSandboxFailure);
export const snapshotRepairSandboxTask = Effect.fn("RepairSandboxTask.snapshot")(function* (
  id: string,
) {
  const state = yield* (yield* RepairSandboxTaskStore).read;
  if (state === undefined)
    return SandboxTaskExecutionSnapshot.make({
      repositoryName: null,
      verifiedHead: null,
      materializationId: null,
      containerSize: null,
      pipeline: null,
      codexTurn: null,
    });
  if (state.request.id !== id)
    return yield* conflict("The request identifies a different execution");
  return yield* Schema.decodeUnknownEffect(SandboxTaskExecutionSnapshot)(
    yield* readSandboxTaskExecutionSnapshot(state.request),
  );
}, preserveRepairSandboxFailure);
export const eventsRepairSandboxTask = Effect.fn("RepairSandboxTask.events")(function* (
  id: string,
  after: number,
  limit: number,
) {
  const records = yield* RepairSandboxTaskStore;
  const state = yield* records.read;
  if (state === undefined) return [];
  if (state.request.id !== id)
    return yield* conflict("The request identifies a different execution");
  return yield* records.events(after, limit);
}, preserveRepairSandboxFailure);

const observation = (request: SandboxTaskNativeRequest) =>
  [
    "Describe",
    "Process",
    "WaitProcess",
    "Processes",
    "ReadFileString",
    "Exists",
    "Lstat",
    "ReadDirectory",
    "JournalGet",
    "WaitPort",
    "WaitHttp",
  ].includes(request.action._tag) ||
  (request.action._tag === "PrepareFile" && request.action.access === "read");
const cleanup = (request: SandboxTaskNativeRequest) =>
  [
    "Destroy",
    "Park",
    "AcknowledgeProcess",
    "SignalProcess",
    "ClearRepositoryAuthorization",
  ].includes(request.action._tag);
export const nativeRepairSandboxTask = Effect.fn("RepairSandboxTask.native")(function* (
  id: string,
  request: SandboxTaskNativeRequest,
) {
  const control = yield* RepairSandboxTaskControl;
  const operation = Effect.gen(function* () {
    const state = yield* owned(id);
    if (
      request.purpose === "turn" &&
      !observation(request) &&
      request.action._tag !== "PrepareForward" &&
      !cleanup(request)
    )
      return yield* conflict("The model runtime owns primary workspace mutations");
    if (
      request.purpose === "turn" &&
      !observation(request) &&
      (yield* Ref.get(control.active)) !== undefined
    )
      return yield* conflict("The active model turn owns this workspace");
    if (!observation(request) && !cleanup(request)) yield* live;
    const selected = yield* repairSandboxTaskSandbox(id, request.purpose);
    const described = yield* selected.describe;
    const sandbox =
      described.current === null
        ? selected
        : yield* repairSandboxTaskSandbox(id, request.purpose, {
            recovery: "exactWorkspaceRequired",
            expectedInstanceId: described.current.instanceId,
          });
    return yield* runNativeOperation(state.request, request).pipe(
      Effect.provideService(SessionSandbox, sandbox),
    );
  });
  return yield* observation(request)
    ? operation
    : Semaphore.withPermit(control.lifecycle, operation);
}, preserveRepairSandboxFailure);
export const forwardRepairSandboxTask = Effect.fn("RepairSandboxTask.forward")(function* (
  id: string,
  purpose: SandboxTaskNativeRequest["purpose"],
  operationId: string,
  request: Request,
) {
  const control = yield* RepairSandboxTaskControl;
  return yield* Semaphore.withPermit(
    control.lifecycle,
    Effect.gen(function* () {
      const state = yield* owned(id);
      if (state.cancelled && request.method !== "GET") return new Response(null, { status: 409 });
      const sandbox = yield* repairSandboxTaskSandbox(id, purpose);
      return yield* forwardSandboxTaskNativeRequest(
        state.request,
        purpose,
        operationId,
        request,
      ).pipe(Effect.provideService(SessionSandbox, sandbox));
    }),
  );
}, preserveRepairSandboxFailure);
