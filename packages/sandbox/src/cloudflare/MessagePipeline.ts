import * as Crypto from "effect/Crypto";
import {
  Cause,
  Clock,
  Context,
  Effect,
  Exit,
  Fiber,
  Option,
  Predicate,
  Ref,
  type JsonSchema,
} from "effect";

import type { RepositorySetupReport } from "../model/RepositorySetup";
import type { RepositorySourceAccess } from "../model/RepositorySource";
import { reportSanitizedCause } from "../observability/RuntimeDiagnostics";

import {
  agentPhases,
  unknownLifecycleObserved,
  type AgentTimings,
  type ContainerSize,
  type HarnessName,
  type LifecycleObserved,
  type LifecyclePostTurn,
  type LifecyclePreTurn,
  type ReasoningEffort,
  type RunLifecycle,
  type WorkspaceMaterialization,
} from "../Contract";
import {
  Harness,
  HarnessTurnError,
  type HarnessTurnResult,
  type HarnessTurnSteer,
} from "../harness/Harness";
import type { RecordedAgentEvent } from "../RunRecord";
import { SessionSandbox } from "../SessionSandbox";
import { inMeasure, inPhase } from "../tracing/RunSpanCapture";
import { MATERIALIZATION_MARKER, REPO_DIR } from "../workspace-paths";
import { AgentEventSink, type AgentEventSinkError } from "./AgentEventSink";
import type { RepositoryPersistenceCheckpointError } from "./RepositoryPersistenceCheckpoint";
import {
  ArtifactsCheckout,
  RepositoryForkIntentError,
  type ArtifactsRepositorySource,
} from "./ArtifactsCheckout";
import { sandboxFor } from "./Bindings";
import { SandboxAgentConfig, sandboxAgentDeadlinePolicy, requireRepositorySeed } from "./Config";
import { isSandboxFileMissing, SandboxOperationError } from "./NativeSandbox";
import { OutboundAuthority } from "./OutboundAuthority";
import {
  captureRepositoryCheckpoint,
  recoverRepositoryCheckpoint,
  sameRepositoryCheckpointInput,
} from "./RepositoryCheckpoint";
import {
  prepareRepositorySetup,
  readRepositorySetup,
  repositorySetupCheckpointMatches,
  repositorySetupFingerprint,
  type RepositorySetupInputs,
} from "./RepositorySetup";
import { forkNameFor, resourceHash, sandboxNameFor } from "./resource-names";
import { withSandboxLease } from "./SandboxLease";
import { SessionStateStore } from "./SessionStateStore";
import { RepositorySourceCheckout } from "./RepositorySourceCheckout";
import {
  WorkspaceBaselineRepository,
  WORKSPACE_PREPARATION_VERSION,
  WORKSPACE_SNAPSHOT_HEADROOM,
  type WorkspaceBaseline,
  type WorkspaceBaselineSelection,
} from "./WorkspaceBaselineRepository";
import {
  ensureWorkspaceServices,
  killStrayHarnessProcesses,
  TurnWorkspaceServices,
} from "./WorkspaceProcesses";

/**
 * One message turn: resolve durable state, prepare the sandbox workspace,
 * run the harness, persist the result, and emit the lifecycle event stream.
 */

const gitOperationSummary = (command: string): string => {
  switch (command) {
    case "clone":
      return "clone";
    case "rev-parse":
      return "revision inspection";
    case "remote":
      return "remote configuration";
    case "status":
      return "status inspection";
    case "add":
      return "change staging";
    case "-c":
      return "commit";
    case "diff":
      return "change inspection";
    case "push":
      return "push";
    default:
      return "operation";
  }
};

const harnessTurnSummary = (
  reason: string,
): { readonly reason: string; readonly message: string } | null => {
  switch (reason) {
    case "malformedEvent":
      return { reason, message: "The harness emitted a malformed event" };
    case "oversizedEvent":
      return { reason, message: "The harness emitted an oversized event" };
    case "missingTerminalEvent":
      return { reason, message: "The harness exited without a terminal event" };
    case "missingResumeToken":
      return { reason, message: "The harness did not report a resume token" };
    case "nonZeroExit":
      return { reason, message: "The harness process exited unsuccessfully" };
    case "deadlineExceeded":
      return { reason, message: "The harness turn exceeded its deadline" };
    case "turnFailed":
      return { reason, message: "The harness reported a failed turn" };
    case "freshSessionRequired":
      return { reason, message: "The harness session could not be resumed" };
    default:
      return null;
  }
};

/** Flattens a pipeline cause into bounded, content-free wire fields. The full
 * cause is logged at the host boundary before this summary is emitted. */
export const describeFailure = (
  cause: Cause.Cause<unknown>,
): { reason: string; message: string } => {
  const found = Cause.findErrorOption(cause);

  if (Option.isNone(found)) {
    return { reason: "Defect", message: "The message pipeline died unexpectedly" };
  }
  const failure = found.value;

  if (Predicate.isObject(failure) && Predicate.hasProperty(failure, "_tag")) {
    if (
      failure._tag === "GitCommandError" &&
      Predicate.hasProperty(failure, "command") &&
      Predicate.isString(failure.command) &&
      Predicate.hasProperty(failure, "exitCode") &&
      Predicate.isNumber(failure.exitCode)
    ) {
      return {
        reason: "GitCommandError",
        message: `Git ${gitOperationSummary(failure.command)} exited with ${failure.exitCode}`,
      };
    }
    if (
      failure._tag === "PersistedPathsLimitError" &&
      Predicate.hasProperty(failure, "pathCount") &&
      Predicate.isNumber(failure.pathCount) &&
      Predicate.hasProperty(failure, "observedBytes") &&
      Predicate.isNumber(failure.observedBytes) &&
      Predicate.hasProperty(failure, "maxBytes") &&
      Predicate.isNumber(failure.maxBytes)
    ) {
      return {
        reason: "PersistedPathsLimitError",
        message: `${failure.pathCount} changed paths encoded to ${failure.observedBytes} bytes (maximum ${failure.maxBytes})`,
      };
    }
    if (
      failure._tag === "HarnessTurnError" &&
      Predicate.hasProperty(failure, "reason") &&
      Predicate.isString(failure.reason)
    ) {
      const summary = harnessTurnSummary(failure.reason);

      if (summary !== null) return summary;
    }
    if (
      failure._tag === "ArtifactsOperationTimeoutError" &&
      Predicate.hasProperty(failure, "timeoutMillis") &&
      Predicate.isNumber(failure.timeoutMillis)
    ) {
      return {
        reason: "ArtifactsOperationTimeoutError",
        message: `An Artifacts operation exceeded ${failure.timeoutMillis} milliseconds`,
      };
    }
    switch (failure._tag) {
      case "ArtifactsOperationError":
        return { reason: "ArtifactsOperationError", message: "An Artifacts operation failed" };
      case "ForkNotReadyError":
        return { reason: "ForkNotReadyError", message: "The durable fork was not ready" };
      case "RepositoryForkIntentError":
        return {
          reason: "RepositoryForkIntentError",
          message: "The repository no longer matches its retained source or private fork",
        };
      case "PushVerificationError":
        return {
          reason: "PushVerificationError",
          message: "The durable repository did not match the persisted commit",
        };
      case "SandboxOperationError":
        return { reason: "SandboxOperationError", message: "A sandbox operation failed" };
    }
  }

  return { reason: "PipelineFailure", message: "The message pipeline failed" };
};

/** One-pass extraction of the run facts CI and analysis care about. */
export const summarizeRun = (recorded: ReadonlyArray<RecordedAgentEvent>) => {
  let outcome: "completed" | "failed" = "failed";
  let failureReason: string | null = null;
  let timings: AgentTimings | null = null;
  let harness: HarnessName | null = null;
  let harnessVersion: string | null = null;
  let model: string | null = null;
  let reasoningEffort: ReasoningEffort | null = null;
  let fast: boolean | null = null;
  let containerSize: ContainerSize | null = null;
  let materialization: WorkspaceMaterialization | null = null;
  let head: string | null = null;
  let changedPaths: ReadonlyArray<string> = [];
  let usage: unknown = null;
  let resumeToken: string | null = null;

  for (const { event } of recorded) {
    if (event.type === "agent.ready") {
      harness = event.harness;
      harnessVersion = event.harnessVersion;
      model = event.model;
      reasoningEffort = event.reasoningEffort;
      fast = event.fast;
      containerSize = event.containerSize;
      materialization = event.materialization;
    } else if (event.type === "agent.persisted") {
      head = event.head;
      changedPaths = event.changedPaths;
    } else if (event.type === "agent.completed") {
      outcome = "completed";
      timings = event.timings;
      usage = event.usage;
      resumeToken = event.resumeToken;
    } else if (event.type === "agent.failed") {
      failureReason = `${event.reason}: ${event.message}`;
    }
  }

  return {
    outcome,
    failureReason,
    timings,
    harness,
    harnessVersion,
    model,
    reasoningEffort,
    fast,
    containerSize,
    materialization,
    head,
    changedPaths,
    usage,
    resumeToken,
  };
};

/** Lifecycle observations accumulated as the pipeline reaches them; a part
 * still `null` at archive time means the pipeline died before observing it. */
export interface LifecycleCapture {
  readonly preTurn: LifecyclePreTurn | null;
  readonly observed: LifecycleObserved | null;
  readonly postTurn: LifecyclePostTurn | null;
}

export const finishLifecycle = (capture: LifecycleCapture): RunLifecycle | null =>
  capture.preTurn === null
    ? null
    : {
        preTurn: capture.preTurn,
        observed: capture.observed ?? unknownLifecycleObserved,
        postTurn: capture.postTurn,
      };

/** Accepted turn data. Host capabilities are supplied through the Effect context. */
export interface TurnRequest {
  readonly turnId: string;
  readonly recover?: boolean;
  readonly deadlineAtMs?: number;
  readonly restrictModelRequests?: boolean;
  readonly sentryMcp?: boolean;
  readonly outputSchema?: JsonSchema.JsonSchema;
  readonly sessionId: string;
  /** Original repository identity authenticated by the host, stable across private task forks. */
  readonly repositoryId?: string;
  readonly text: string;
  /** Host-selected immutable seed for this session; defaults to deployment policy. */
  readonly seedRepository?: string;
  /** Immutable admitted source generation and ref, independent of its current main. */
  readonly repositorySource?: ArtifactsRepositorySource;
  /** Host-derived authenticated repository scope, stable across syncs and task forks. */
  readonly preparationKey?: string;
  /** Prepared files from the host-authorized prior candidate, frozen with this turn's admission. */
  readonly continuationCheckpoint?: Pick<WorkspaceBaseline, "checkpoint" | "report"> | null;
  /** Explicit execution storage; None never falls back to a deployment seed. */
  readonly repository?:
    | { readonly _tag: "None" }
    | {
        readonly _tag: "AttachedRepository";
        readonly repositoryName: string;
        readonly artifactsRepositoryId: string;
        readonly commitSha: string;
      }
    | {
        readonly _tag: "SeedRepository";
        readonly repositoryName: string;
        readonly artifactsRepositoryId: string;
        readonly commitSha: string;
        readonly artifactsRef?: string;
      };
  /** Pinned revisions and their history, loaded before repository setup or the harness starts. */
  readonly requiredCommits?: ReadonlyArray<string>;
  /** Host-pinned initial working checkout; the fork's durable main remains the persistence base. */
  readonly initialCheckout?: string;
  /** Start the recipe's preview when the host has no separate preview controller. */
  readonly startRepositoryPreview?: boolean;
  /** Host-authorized Artifacts generation; mutually exclusive with seedRepository. */
  readonly generationSource?: RepositorySourceAccess;
  readonly model?: string;
  readonly reasoningEffort?: ReasoningEffort;
  readonly fast?: boolean;
  readonly serviceTier?: "fast" | "ultrafast" | null;
  readonly containerSize?: ContainerSize;
  /** Server monotonic zero: the request-acceptance timestamp. */
  readonly startedAtMillis: number;
}

/** Optional host receiver for the current native turn's steering capability. */
export const TurnSteering = Context.Reference<{
  readonly ready: (steer: HarnessTurnSteer) => Effect.Effect<void, AgentEventSinkError>;
} | null>("sandbox-agent/TurnSteering", { defaultValue: () => null });

/** Required for generation-bound turns; retain the exact commit before its remote write. */
export const GenerationSourceCheckpoint = Context.Reference<{
  readonly prepared: (head: string) => Effect.Effect<void, RepositoryPersistenceCheckpointError>;
} | null>("sandbox-agent/GenerationSourceCheckpoint", { defaultValue: () => null });

/** Optional per-turn archive capture; ordinary turns need no mutable capture. */
export const TurnLifecycleCapture = Context.Reference<Ref.Ref<LifecycleCapture> | null>(
  "sandbox-agent/TurnLifecycleCapture",
  { defaultValue: () => null },
);

/** @deprecated Use runTurn with TurnSteering, GenerationSourceCheckpoint and optional capture. */
export interface MessageContext<CallbackError = never> extends TurnRequest {
  readonly onSteeringReady?: (steer: HarnessTurnSteer) => Effect.Effect<void, CallbackError>;
  readonly onGenerationSourcePrepared?: (head: string) => Effect.Effect<void, CallbackError>;
  readonly lifecycle: Ref.Ref<LifecycleCapture>;
}

export const runTurn = Effect.fn("SandboxAgentSession.runMessagePipeline")(function* (
  context: TurnRequest,
) {
  yield* Effect.annotateCurrentSpan({
    "sandbox.task.id": context.sessionId,
    "sandbox.turn.id": context.turnId,
  });
  const output = yield* AgentEventSink;
  const store = yield* SessionStateStore;
  const checkout = yield* ArtifactsCheckout;
  const generationCheckouts = yield* RepositorySourceCheckout;
  const harness = yield* Harness;
  const config = yield* SandboxAgentConfig;
  const lifecycle = yield* TurnLifecycleCapture;
  const deadlinePolicy = sandboxAgentDeadlinePolicy(config);
  const saved = yield* store.pipelineTurn.get;
  const savedCodexTurn = context.recover === true ? yield* store.codexTurn.get : null;
  const recovery = context.recover === true && saved?.id === context.turnId ? saved : null;

  if (context.recover === true && recovery === null && savedCodexTurn?.id === context.turnId) {
    return yield* HarnessTurnError.make({
      reason: "turnFailed",
      message:
        "The active native turn has no matching workspace receipt; its input was not replayed",
    });
  }
  const startedAt = recovery?.startedAtMillis ?? context.startedAtMillis;
  const timings: Record<string, number> = {};
  const mark = (phase: string) =>
    Effect.map(Clock.currentTimeMillis, (now) => {
      timings[phase] = now - startedAt;
    });
  const complete = Effect.fnUntraced(function* (
    persisted: { readonly head: string | null; readonly changedPaths: ReadonlyArray<string> },
    turn: HarnessTurnResult,
  ) {
    yield* mark("gitPersisted");
    if (persisted.head !== null) yield* store.verifiedHead.put(persisted.head);
    yield* mark("verified");
    if (lifecycle !== null)
      yield* Ref.update(lifecycle, (capture) => ({
        ...capture,
        postTurn: { verifiedHead: persisted.head, resumeToken: turn.resumeToken },
      }));
    if (persisted.head !== null)
      yield* output.emit({
        type: "agent.persisted",
        head: persisted.head,
        changedPaths: persisted.changedPaths,
      });
    yield* mark("total");
    yield* store.lastTimings.put(timings);
    yield* output.emit({
      type: "agent.completed",
      resumeToken: turn.resumeToken,
      usage: turn.usage,
      timings,
    });
  });

  yield* mark("doActive");
  const hash = yield* resourceHash(context.sessionId);
  const storedFork = yield* store.forkName.get;
  const storedForkIntent = yield* store.repositoryFork.get;
  const forkName = storedFork ?? forkNameFor(hash);
  const repositoryFree = context.repository?._tag === "None";
  const attached =
    context.repository?._tag === "AttachedRepository" ? context.repository : undefined;
  const seed = context.repository?._tag === "SeedRepository" ? context.repository : undefined;
  const seeded =
    seed !== undefined ||
    (context.repository === undefined && context.generationSource === undefined);
  const seedRepository = seeded
    ? (seed?.repositoryName ?? context.seedRepository ?? config.seedRepository)
    : undefined;
  const hadFork = storedFork !== null;
  const storedSize = yield* store.containerSize.get;
  const containerSize = context.containerSize ?? "standard";

  if (storedSize !== containerSize) yield* store.containerSize.put(containerSize);
  const storedMaterialization = yield* store.materializationId.get;
  const storedResumeToken = yield* store.resumeToken.get;
  const storedHead = yield* store.verifiedHead.get;
  const storedInstance = yield* store.containerInstanceId.get;
  const storedCheckpoint = yield* store.workspaceCheckpoint.get;
  const storedSetup = yield* store.repositorySetupReceipt.get;

  const preTurn: LifecyclePreTurn = {
    forkRecorded: hadFork,
    materializationRecorded: storedMaterialization !== null,
    resumeTokenRecorded: storedResumeToken !== null,
    containerSize: storedSize,
    verifiedHead: storedHead,
  };
  if (lifecycle !== null) yield* Ref.update(lifecycle, (capture) => ({ ...capture, preTurn }));
  if (
    recovery !== null &&
    recovery.persisted !== null &&
    savedCodexTurn?.id === context.turnId &&
    savedCodexTurn.result !== null
  ) {
    // Both outcomes are durable; container replacement cannot undo completion.
    if (storedForkIntent !== null) {
      if (!storedForkIntent.initialized || storedForkIntent.repositoryId === null)
        return yield* RepositoryForkIntentError.make({ reason: "target-mismatch" });
      yield* checkout.ensurePlannedFork(storedForkIntent);
    }
    yield* store.resumeToken.put(savedCodexTurn.result.resumeToken);

    return yield* complete(recovery.persisted, savedCodexTurn.result);
  }
  if (recovery !== null && recovery.instanceId === undefined)
    return yield* HarnessTurnError.make({
      reason: "turnFailed",
      message: "The active turn has no native boot receipt; its input was not replayed",
    });
  const sandboxLookup = yield* Effect.gen(function* () {
    const unresolved = yield* sandboxFor(
      containerSize,
      sandboxNameFor(hash),
      recovery === null
        ? {}
        : { recovery: "exactWorkspaceRequired", expectedInstanceId: recovery.instanceId },
    );

    return { unresolved, description: yield* unresolved.describe };
  }).pipe(Effect.forkScoped);
  const baselines = yield* WorkspaceBaselineRepository;
  const admittedSource =
    seed === undefined
      ? context.repositorySource
      : {
          repositoryName: seed.repositoryName,
          repositoryId: seed.artifactsRepositoryId,
          commitSha: seed.commitSha,
          ...(seed.artifactsRef === undefined ? {} : { artifactsRef: seed.artifactsRef }),
        };
  const readSourceGeneration = Effect.gen(function* () {
    const generation = yield* checkout.sourceGeneration(
      yield* requireRepositorySeed(seedRepository),
    );
    return { ...generation, commitSha: yield* checkout.readGenerationMainHead(generation) };
  });
  const source =
    seeded && (storedForkIntent !== null || !hadFork)
      ? (admittedSource ?? storedForkIntent?.source ?? (yield* readSourceGeneration))
      : admittedSource;
  const plannedSource = seeded && (storedForkIntent !== null || !hadFork) ? source : undefined;
  // Retain source authority before dispatch. Start its remote work immediately
  // so workspace preparation does not consume an idle fork deadline.
  const forkIntent =
    plannedSource !== undefined
      ? yield* checkout.retainFork(forkName, plannedSource, context.deadlineAtMs)
      : null;
  const pinnedReadRetryDeadline = Effect.gen(function* () {
    const currentFork = yield* store.repositoryFork.get;
    return Math.min(
      context.deadlineAtMs ?? Infinity,
      startedAt + deadlinePolicy.forkReadyMillis,
      savedCodexTurn?.id === context.turnId ? savedCodexTurn.deadlineAt : Infinity,
      // Completed initialization no longer owns the budget of later turn reads.
      currentFork !== null && !currentFork.initialized ? currentFork.deadlineAtMillis : Infinity,
    );
  });
  const forkState = yield* Ref.make<
    | { readonly state: "pending" | "ready" }
    | {
        readonly state: "failed";
        readonly cause: Cause.Cause<Effect.Error<ReturnType<typeof checkout.ensurePlannedFork>>>;
      }
  >({ state: forkIntent?.initialized ? "ready" : "pending" });
  const forkFiber =
    forkIntent === null
      ? null
      : yield* checkout.ensurePlannedFork(forkIntent).pipe(
          Effect.tap(() => (forkIntent.initialized ? Effect.void : mark("forkReady"))),
          inPhase(agentPhases.fork),
          Effect.exit,
          Effect.tap((exit) =>
            Exit.isSuccess(exit)
              ? Ref.set(forkState, { state: "ready" })
              : Ref.set(forkState, { state: "failed", cause: exit.cause }),
          ),
          Effect.forkScoped,
        );
  const checkForkBeforeModel = Effect.gen(function* () {
    if (plannedSource === undefined) return;
    if (forkFiber === null || forkIntent === null)
      return yield* RepositoryForkIntentError.make({ reason: "intent-mismatch" });
    const accepted = yield* store.codexTurn.get;

    // Dispatch may already have reached the native owner. Recover its accepted
    // turn without interruption or replay; persistence still joins the fork.
    if (accepted?.id === context.turnId && accepted.startDispatched) return;
    const state = yield* Ref.get(forkState);

    // Pending remote work never gates model startup. A completed failure keeps
    // its whole Cause and prevents starting a new turn with failed authority.
    if (state.state === "failed") return yield* Effect.failCause(state.cause);
  });
  const joinFork = Effect.gen(function* () {
    if (plannedSource === undefined) return undefined;
    if (forkFiber === null || forkIntent === null)
      return yield* RepositoryForkIntentError.make({ reason: "intent-mismatch" });
    const exit = yield* Fiber.join(forkFiber);

    if (Exit.isFailure(exit)) return yield* Effect.failCause(exit.cause);
    // Recheck the retained initialized generation after any model time. No
    // lookup of current main can authorize a replacement or a second reset.
    const generation = yield* checkout.ensurePlannedFork(forkIntent);

    if (storedFork === null) yield* store.forkName.put(generation.repositoryName);

    return generation;
  }).pipe(inMeasure("fork-join"));
  const persistRepository = (baseHead: string) =>
    Effect.gen(function* () {
      const generation = yield* joinFork;

      return yield* checkout.persist(
        forkName,
        baseHead,
        yield* requireRepositorySeed(seedRepository),
        generation?.repositoryId,
      );
    });
  const ensuredFork =
    recovery === null && seeded && plannedSource === undefined
      ? yield* checkout
          .ensureFork(forkName, yield* requireRepositorySeed(seedRepository))
          .pipe(inPhase(agentPhases.fork))
      : null;
  const generation =
    source ??
    (recovery === null && seeded && !hadFork
      ? yield* checkout.sourceGeneration(yield* requireRepositorySeed(seedRepository))
      : null);
  const readGeneration =
    plannedSource === undefined
      ? undefined
      : forkIntent?.initialized && forkIntent.repositoryId !== null
        ? { repositoryName: forkIntent.forkName, repositoryId: forkIntent.repositoryId }
        : plannedSource;
  const repositoryHead = ensuredFork?.head ?? storedHead ?? plannedSource?.commitSha;
  const repository =
    context.preparationKey !== undefined
      ? `${config.artifactsHostname}/${config.artifactsNamespace}/prepared:${context.preparationKey}`
      : attached !== undefined
        ? `${config.artifactsHostname}/${config.artifactsNamespace}/${attached.repositoryName}@${attached.artifactsRepositoryId}`
        : context.generationSource === undefined
          ? `${config.artifactsHostname}/${config.artifactsNamespace}/${generation?.repositoryName ?? seedRepository}@${generation?.repositoryId ?? ""}`
          : `${config.artifactsHostname}/${config.artifactsNamespace}/${context.generationSource.repositoryName}@${context.generationSource.repositoryId}`;
  const desiredHead =
    (attached === undefined
      ? undefined
      : (storedHead ?? context.initialCheckout ?? attached.commitSha)) ??
    (context.generationSource === undefined
      ? (context.initialCheckout ?? repositoryHead)
      : (context.generationSource.draft?.sourceSha ?? context.generationSource.acceptedSourceSha));

  if (
    recovery === null &&
    storedForkIntent !== null &&
    plannedSource !== undefined &&
    readGeneration === plannedSource
  )
    yield* checkout
      .readSourcePin(plannedSource, yield* pinnedReadRetryDeadline)
      .pipe(inMeasure("repository-source-validate"));
  const { unresolved, description } = yield* Fiber.join(sandboxLookup);
  const now = yield* Clock.currentTimeMillis;
  let validSessionCheckpoint =
    !repositoryFree &&
    storedCheckpoint !== null &&
    storedCheckpoint.classIdentity === description.classIdentity &&
    storedCheckpoint.imageDigest === description.imageDigest &&
    storedCheckpoint.expiresAt > now + WORKSPACE_SNAPSHOT_HEADROOM
      ? storedCheckpoint
      : null;
  const ownerCheckpoint = yield* recoverRepositoryCheckpoint(
    description.checkpoint,
    yield* store.workspaceCheckpointInput.get,
  );

  if (
    validSessionCheckpoint !== null &&
    ownerCheckpoint !== null &&
    ownerCheckpoint.classIdentity === validSessionCheckpoint.classIdentity &&
    ownerCheckpoint.imageDigest === validSessionCheckpoint.imageDigest &&
    ownerCheckpoint.head === validSessionCheckpoint.head &&
    ownerCheckpoint.materializationId === validSessionCheckpoint.materializationId &&
    ownerCheckpoint.setupFingerprint === validSessionCheckpoint.setupFingerprint &&
    ownerCheckpoint.expiresAt > validSessionCheckpoint.expiresAt
  ) {
    validSessionCheckpoint = ownerCheckpoint;
    yield* store.workspaceCheckpoint.put(ownerCheckpoint);
  }
  let selected: WorkspaceBaseline | null =
    validSessionCheckpoint === null || storedSetup === null
      ? null
      : { repository, checkpoint: validSessionCheckpoint, report: storedSetup.report };
  const continuation = context.continuationCheckpoint;
  const fromContinuation =
    recovery === null &&
    (seeded || attached !== undefined) &&
    selected === null &&
    validSessionCheckpoint === null &&
    continuation !== undefined &&
    continuation !== null &&
    (description.current === null ||
      description.current.restoredSnapshotId === continuation.checkpoint.snapshotId) &&
    continuation.checkpoint.classIdentity === description.classIdentity &&
    continuation.checkpoint.imageDigest === description.imageDigest &&
    continuation.checkpoint.expiresAt > now + WORKSPACE_SNAPSHOT_HEADROOM;

  if (fromContinuation) selected = { repository, ...continuation };
  let shared = false;
  const findBaseline = (key: WorkspaceBaselineSelection) =>
    baselines.find(key).pipe(
      inMeasure(
        key.setupFingerprint === undefined
          ? "workspace-checkpoint-lookup"
          : "workspace-checkpoint-compatible-lookup",
      ),
      Effect.catchCauseIf(
        (cause) =>
          cause.reasons.every(
            (reason) =>
              Cause.isFailReason(reason) &&
              Predicate.isTagged(reason.error, "WorkspaceBaselineError"),
          ),
        (cause) =>
          reportSanitizedCause(cause, { operation: "MessagePipeline.baselineLookup" }).pipe(
            Effect.as(null),
          ),
      ),
    );

  if (
    recovery === null &&
    (seeded || attached !== undefined) &&
    description.current === null &&
    validSessionCheckpoint === null &&
    selected === null &&
    desiredHead !== null &&
    desiredHead !== undefined
  ) {
    selected = yield* findBaseline({
      repository,
      classIdentity: description.classIdentity,
      imageDigest: description.imageDigest,
      head: desiredHead,
    });
    shared = selected !== null;
  }
  let identity = yield* unresolved
    .ensure({
      recovery: recovery === null ? "freshAllowed" : "exactWorkspaceRequired",
      ...(recovery?.instanceId === undefined ? {} : { expectedInstanceId: recovery.instanceId }),
      ...(recovery !== null
        ? {}
        : { checkpoint: selected?.checkpoint ?? validSessionCheckpoint ?? undefined }),
    })
    .pipe(inPhase(agentPhases.containerBoot));

  if (
    recovery === null &&
    (seeded || attached !== undefined) &&
    storedInstance !== identity.instanceId &&
    (((storedMaterialization !== null || storedSetup !== null) && storedSetup?.prepared !== true) ||
      saved?.id === context.turnId)
  )
    return yield* HarnessTurnError.make({
      reason: "turnFailed",
      message: "The previous workspace has unfinished preparation; its commands were not replayed",
    });
  let selectedWorkspaceCheckpoint = selected?.checkpoint ?? validSessionCheckpoint;
  let checkpointRejected = false;
  let checkpointReselected = false;
  let restoredSetupInputs: RepositorySetupInputs | undefined;
  let restoredCommitWithHistory: string | undefined;
  let validatedRestore: { readonly instanceId: string; readonly marker: string } | undefined;

  while (
    recovery === null &&
    (seeded || attached !== undefined) &&
    identity.restoredSnapshotId !== null
  ) {
    const restoredSandbox = yield* sandboxFor(containerSize, sandboxNameFor(hash), {
      recovery: "exactWorkspaceRequired",
      expectedInstanceId: identity.instanceId,
    });
    let compatibleKey: WorkspaceBaselineSelection | undefined;
    const compatible = yield* withSandboxLease(
      `${context.turnId}:checkpoint-validation`,
      Effect.gen(function* () {
        const marker = yield* restoredSandbox.readFileString(MATERIALIZATION_MARKER).pipe(
          Effect.map((content) => content.trim()),
          Effect.catchCauseIf(
            (cause) =>
              cause.reasons.length > 0 &&
              cause.reasons.every(
                (reason) => Cause.isFailReason(reason) && isSandboxFileMissing(reason.error),
              ),
            () => Effect.succeed(null),
          ),
        );

        if (storedInstance === identity.instanceId) {
          if (storedMaterialization !== null && marker === storedMaterialization) return true;

          return yield* HarnessTurnError.make({
            reason: "turnFailed",
            message:
              "The restored workspace does not match its retained materialization; its commands were not replayed",
          });
        }
        // An interrupted validation can leave a running, unadopted restore.
        // Without its exact cache provenance, discard it before clone or setup.
        if (
          selected === null ||
          desiredHead === null ||
          desiredHead === undefined ||
          selected.checkpoint.snapshotId !== identity.restoredSnapshotId ||
          marker !== selected.checkpoint.materializationId
        )
          return false;
        const authority = yield* (yield* OutboundAuthority).mint({
          sessionId: context.sessionId,
          turnId: context.turnId,
          instanceId: identity.instanceId,
        });

        if (Option.isSome(authority))
          yield* restoredSandbox.configureOutboundAuthority(authority.value);
        // Read committed inputs without adopting the restored filesystem or
        // executing a recipe. An install may have written outside the checkout.
        const includeHistory =
          attached !== undefined ||
          (context.requiredCommits?.includes(desiredHead) === true &&
            (desiredHead === selected.checkpoint.head ||
              context.requiredCommits.includes(selected.checkpoint.head)));

        yield* checkout.ensureCommit(
          attached?.repositoryName ?? readGeneration?.repositoryName ?? forkName,
          desiredHead,
          {
            // Fetch required history once while loading the target's setup inputs.
            // Other checkpoint heads may be absent from the admitted generation.
            includeHistory,
            retryDeadlineAtMillis: yield* pinnedReadRetryDeadline,
            ...(attached === undefined
              ? readGeneration === undefined
                ? {}
                : { repositoryId: readGeneration.repositoryId }
              : { repositoryId: attached.artifactsRepositoryId }),
          },
        );
        const recipe = yield* readRepositorySetup(desiredHead);
        const fingerprint = yield* repositorySetupFingerprint(desiredHead, recipe);

        if (
          selected.checkpoint.setupFingerprint !== fingerprint &&
          (recipe?.requiredSecrets.length ?? 0) === 0
        )
          compatibleKey = {
            repository,
            classIdentity: description.classIdentity,
            imageDigest: description.imageDigest,
            head: desiredHead,
            setupFingerprint: fingerprint,
          };

        const matches = repositorySetupCheckpointMatches(selected, recipe, fingerprint);

        if (matches) {
          restoredSetupInputs = { sourceHead: desiredHead, recipe, fingerprint };
          // A running, unadopted restore may have work from an interrupted invocation.
          if (description.current?.instanceId !== identity.instanceId)
            validatedRestore = { instanceId: identity.instanceId, marker };
          // ensureCommit verified the admitted generation while loading its history.
          if (includeHistory && plannedSource !== undefined && storedForkIntent === null)
            restoredCommitWithHistory = desiredHead;
        }

        return matches;
      }),
    ).pipe(
      Effect.provideService(SessionSandbox, restoredSandbox),
      inMeasure("workspace-checkpoint-validate"),
    );

    if (compatible) break;
    // The newest checkpoint can hide an older compatible installation. Once
    // committed inputs are known, make one exact lookup instead of trying every
    // snapshot. Neither boot has an adopted workspace or accepted setup command.
    const replacement =
      shared && !checkpointReselected && compatibleKey !== undefined
        ? yield* findBaseline(compatibleKey)
        : null;

    // Keep incompatible shared checkpoints for revisions whose inputs still fit.
    yield* unresolved.destroy.pipe(inMeasure("workspace-checkpoint-discard"));
    checkpointRejected = replacement === null;
    if (replacement !== null) {
      selected = replacement;
      selectedWorkspaceCheckpoint = replacement.checkpoint;
      checkpointReselected = true;
    }
    identity = yield* unresolved
      .ensure({ recovery: "freshAllowed", checkpoint: replacement?.checkpoint })
      .pipe(inPhase(agentPhases.containerBoot));
  }
  const restoredCheckpoint =
    selected?.checkpoint.snapshotId === identity.restoredSnapshotId ? selected : null;
  const restoredWorkspaceCheckpoint =
    selectedWorkspaceCheckpoint?.snapshotId === identity.restoredSnapshotId
      ? selectedWorkspaceCheckpoint
      : null;
  const containerReused = description.current?.instanceId === identity.instanceId;
  const containerStartMillis =
    containerReused || identity.timings.ownerReadyAt === null
      ? 0
      : identity.timings.ownerReadyAt - identity.timings.nativeStartRequestedAt;
  const checkpointSource =
    selectedWorkspaceCheckpoint === null ? "none" : shared ? "shared" : "session";
  const checkpointRestore = checkpointRejected
    ? "rejected"
    : containerReused || selectedWorkspaceCheckpoint === null
      ? "not-requested"
      : restoredWorkspaceCheckpoint === null
        ? "miss"
        : "restored";

  yield* Effect.annotateCurrentSpan({
    "workspace.checkpoint.source": fromContinuation ? "continuation" : checkpointSource,
    "workspace.checkpoint.continuation":
      continuation === undefined
        ? "not-requested"
        : fromContinuation && restoredCheckpoint !== null && !checkpointRejected
          ? "hit"
          : "miss",
    "workspace.checkpoint.restore": checkpointRestore,
    "workspace.checkpoint.snapshotId": restoredWorkspaceCheckpoint?.snapshotId ?? null,
    "workspace.checkpoint.reselected": checkpointReselected,
    "workspace.container.instanceId": identity.instanceId,
    "workspace.container.reused": containerReused,
    "workspace.container.startMillis": containerStartMillis,
  });
  yield* mark("containerReady");

  if (
    shared &&
    selected !== null &&
    restoredCheckpoint === null &&
    !checkpointRejected &&
    description.current === null
  ) {
    yield* baselines
      .invalidate(
        {
          repository,
          classIdentity: selected.checkpoint.classIdentity,
          imageDigest: selected.checkpoint.imageDigest,
          head: selected.checkpoint.head,
        },
        selected.checkpoint.snapshotId,
      )
      .pipe(
        Effect.catchCauseIf(
          (cause) =>
            cause.reasons.every(
              (reason) =>
                Cause.isFailReason(reason) &&
                Predicate.isTagged(reason.error, "WorkspaceBaselineError"),
            ),
          (cause) =>
            reportSanitizedCause(cause, { operation: "MessagePipeline.baselineInvalidation" }),
        ),
      );
  }
  const sandbox = yield* sandboxFor(containerSize, sandboxNameFor(hash), {
    recovery: "exactWorkspaceRequired",
    expectedInstanceId: identity.instanceId,
  }).pipe(inMeasure("sandbox-resolve"));
  // Everything downstream acts through the turn's one resolved sandbox, so it
  // is provided once here and yielded as SessionSandbox by every turn-scoped
  // operation instead of threading the client through each signature.
  const turnBody = Effect.gen(function* () {
    const authority = yield* (yield* OutboundAuthority).mint({
      sessionId: context.sessionId,
      turnId: context.turnId,
      instanceId: identity.instanceId,
    });

    if (Option.isSome(authority)) yield* sandbox.configureOutboundAuthority(authority.value);
    const bootStep =
      validatedRestore?.instanceId === identity.instanceId
        ? Effect.succeed(validatedRestore.marker)
        : sandbox.readFileString(MATERIALIZATION_MARKER).pipe(
            Effect.map((content) => content.trim()),
            Effect.catchIf(isSandboxFileMissing, () => Effect.succeed(null)),
            inPhase(agentPhases.containerBoot),
          );
    // A native restore is only installation evidence while checkout retains its
    // files. A marker mismatch can force a clone that removes those dependencies.
    let installedCheckpoint: WorkspaceBaseline | undefined;
    const prepareRepositoryWorkspace = Effect.gen(function* () {
      const ensured = ensuredFork;
      const baseHead = repositoryHead;

      if (baseHead === undefined || baseHead === null)
        return yield* Effect.die("Missing admitted repository pin");
      const marker = yield* bootStep;

      yield* killStrayHarnessProcesses();
      const reuse =
        storedMaterialization !== null &&
        marker === storedMaterialization &&
        storedInstance === identity.instanceId;
      let head: string;
      let restored = false;
      let resumeToken: string | null = null;
      let harnessVersion: string | null = null;

      if (ensured !== null && !hadFork) yield* store.forkName.put(forkName);
      if (ensured !== null && ensured.disposition !== "existing") yield* mark("forkReady");
      yield* mark("sandboxResolved");
      if (reuse) {
        head = storedHead ?? baseHead;
        resumeToken = storedResumeToken;
        harnessVersion = yield* store.harnessVersion.get;
      } else {
        yield* ensureWorkspaceServices(true).pipe(Effect.provideService(TurnWorkspaceServices, []));
        yield* store.resumeToken.delete;
        const snapshot = restoredCheckpoint?.checkpoint;

        if (snapshot && marker === snapshot.materializationId) {
          head = context.initialCheckout ?? baseHead;
          yield* checkout
            .restoreSnapshotCheckout(forkName, head, snapshot.head)
            .pipe(inMeasure("workspace-checkpoint-checkout"));
          restored = true;
          installedCheckpoint = restoredCheckpoint ?? undefined;
        } else {
          const cloned = yield* (
            plannedSource !== undefined && readGeneration === plannedSource
              ? checkout.cloneSourceIntoSandbox(
                  plannedSource,
                  forkName,
                  yield* pinnedReadRetryDeadline,
                )
              : readGeneration !== undefined
                ? checkout.cloneGenerationIntoSandbox(readGeneration)
                : checkout.cloneIntoSandbox(
                    forkName,
                    "workspace",
                    recovery === null &&
                      generation !== null &&
                      storedMaterialization === null &&
                      storedHead === null &&
                      storedResumeToken === null &&
                      storedSetup === null &&
                      storedCheckpoint === null
                      ? { source: generation, forkHead: baseHead }
                      : undefined,
                  )
          ).pipe(inPhase(agentPhases.clone));

          if (plannedSource !== undefined && cloned.head !== baseHead)
            return yield* RepositoryForkIntentError.make({ reason: "target-mismatch" });

          head = cloned.head;
          restored =
            ensured?.disposition === "existing" || hadFork || storedMaterialization !== null;
        }
        yield* mark("cloneComplete");
        const materializationId = yield* (yield* Crypto.Crypto).randomUUIDv4().pipe(Effect.orDie);

        yield* sandbox
          .writeFile(MATERIALIZATION_MARKER, materializationId)
          .pipe(inMeasure("marker-write"));
        yield* store.materializationId.put(materializationId);
        yield* store.containerInstanceId.put(identity.instanceId);
        yield* store.verifiedHead.put(head);
      }
      for (const commit of context.requiredCommits ?? []) {
        if (installedCheckpoint !== undefined && commit === restoredCommitWithHistory) continue;
        yield* checkout.ensureCommit(readGeneration?.repositoryName ?? forkName, commit, {
          includeHistory: true,
          retryDeadlineAtMillis: yield* pinnedReadRetryDeadline,
          ...(readGeneration === undefined ? {} : { repositoryId: readGeneration.repositoryId }),
        });
      }
      if (context.initialCheckout !== undefined && head !== context.initialCheckout) {
        yield* checkout.checkoutCommit(context.initialCheckout);
        head = context.initialCheckout;
      }

      return {
        marker,
        reuse,
        head,
        restored,
        resumeToken,
        harnessVersion,
        forkDisposition: ensured?.disposition ?? "indeterminate",
        forkPollAttempts:
          ensured === null || ensured.disposition === "existing" ? null : ensured.pollAttempts,
        baseHead,
        persist: persistRepository(baseHead),
      } as const;
    });
    const prepareIndependentWorkspace = Effect.gen(function* () {
      const marker = yield* bootStep;

      yield* killStrayHarnessProcesses();
      const sourceHead =
        attached === undefined
          ? null
          : (storedHead ?? context.initialCheckout ?? attached.commitSha);
      const reuse =
        storedMaterialization !== null &&
        marker === storedMaterialization &&
        storedInstance === identity.instanceId &&
        (attached === undefined || storedHead === sourceHead);

      if (attached !== undefined) {
        const generation = yield* checkout.sourceGeneration(attached.repositoryName);

        if (generation.repositoryId !== attached.artifactsRepositoryId)
          return yield* HarnessTurnError.make({
            reason: "turnFailed",
            message: "The attached repository generation changed",
          });
      }
      if (!reuse) {
        yield* ensureWorkspaceServices(true).pipe(Effect.provideService(TurnWorkspaceServices, []));
        yield* store.resumeToken.delete;
        if (attached === undefined) {
          yield* sandbox.mkdir(REPO_DIR, { recursive: true });
          yield* store.verifiedHead.delete;
        } else {
          const snapshot = restoredCheckpoint?.checkpoint;

          if (snapshot !== undefined && marker === snapshot.materializationId) {
            yield* checkout
              .restoreSnapshotCheckout(attached.repositoryName, sourceHead!, snapshot.head)
              .pipe(inMeasure("workspace-checkpoint-checkout"));
            installedCheckpoint = restoredCheckpoint ?? undefined;
          } else {
            yield* checkout
              .cloneGenerationIntoSandbox({
                repositoryName: attached.repositoryName,
                repositoryId: attached.artifactsRepositoryId,
              })
              .pipe(inPhase(agentPhases.clone));
            yield* checkout.ensureCommit(attached.repositoryName, sourceHead!, {
              includeHistory: true,
              repositoryId: attached.artifactsRepositoryId,
              retryDeadlineAtMillis: yield* pinnedReadRetryDeadline,
            });
            yield* checkout.checkoutCommit(sourceHead!);
          }
          yield* store.verifiedHead.put(sourceHead!);
        }
        yield* mark("cloneComplete");
        const materializationId = yield* (yield* Crypto.Crypto).randomUUIDv4().pipe(Effect.orDie);

        yield* sandbox.writeFile(MATERIALIZATION_MARKER, materializationId);
        yield* store.materializationId.put(materializationId);
        yield* store.containerInstanceId.put(identity.instanceId);
      }
      if (attached !== undefined) {
        for (const commit of context.requiredCommits ?? [])
          yield* checkout.ensureCommit(attached.repositoryName, commit, {
            includeHistory: true,
            repositoryId: attached.artifactsRepositoryId,
            retryDeadlineAtMillis: yield* pinnedReadRetryDeadline,
          });
      }

      return {
        marker,
        reuse,
        head: sourceHead,
        baseHead: sourceHead,
        restored: !reuse && (storedMaterialization !== null || restoredCheckpoint !== null),
        resumeToken: reuse ? storedResumeToken : null,
        harnessVersion: reuse ? yield* store.harnessVersion.get : null,
        forkDisposition: "indeterminate" as const,
        forkPollAttempts: null,
        persist:
          attached === undefined
            ? Effect.succeed({ head: null, changedPaths: [] })
            : checkout.persistGeneration<never, never>(
                {
                  repositoryName: attached.repositoryName,
                  repositoryId: attached.artifactsRepositoryId,
                },
                sourceHead!,
              ),
      };
    });
    const prepareGenerationWorkspace = (generationSource: RepositorySourceAccess) =>
      Effect.gen(function* () {
        if (storedFork !== null) {
          return yield* Effect.die("A generation-bound run cannot own a task repository fork");
        }
        const sourceHead = generationSource.draft?.sourceSha ?? generationSource.acceptedSourceSha;
        const marker = yield* bootStep;
        const generationCheckout = generationCheckouts.forSource(generationSource);
        const checkpoint = yield* GenerationSourceCheckpoint;

        if (checkpoint === null) {
          return yield* Effect.die(
            "A generation-bound turn requires a durable GenerationSourceCheckpoint",
          );
        }

        yield* killStrayHarnessProcesses();
        const reuse =
          storedMaterialization !== null &&
          marker === storedMaterialization &&
          storedInstance === identity.instanceId &&
          storedHead === sourceHead;
        let harnessVersion: string | null;
        let resumeToken: string | null = null;

        if (reuse) {
          harnessVersion = yield* store.harnessVersion.get;
          resumeToken = storedResumeToken;
        } else {
          yield* store.resumeToken.delete;
          if (
            restoredWorkspaceCheckpoint?.head !== sourceHead ||
            marker !== restoredWorkspaceCheckpoint.materializationId
          )
            yield* generationCheckout.restore.pipe(inPhase(agentPhases.clone));
          else yield* checkout.checkoutCommit(sourceHead);

          yield* mark("cloneComplete");
          harnessVersion = null;
          yield* store.verifiedHead.put(sourceHead);
          const materializationId = yield* (yield* Crypto.Crypto).randomUUIDv4().pipe(Effect.orDie);

          yield* sandbox
            .writeFile(MATERIALIZATION_MARKER, materializationId)
            .pipe(inMeasure("marker-write"));
          yield* store.materializationId.put(materializationId);
          yield* store.containerInstanceId.put(identity.instanceId);
        }
        yield* mark("sandboxResolved");

        return {
          marker,
          reuse,
          head: sourceHead,
          restored: !reuse && storedMaterialization !== null,
          resumeToken,
          harnessVersion,
          // Generation-bound runs have no task fork; the legacy lifecycle vocabulary has
          // no "not applicable" member, so do not claim one existed.
          forkDisposition: "indeterminate" as const,
          forkPollAttempts: null,
          baseHead: sourceHead,
          persist: generationCheckout.persist(({ head: preparedHead }) =>
            checkpoint.prepared(preparedHead),
          ),
        } as const;
      });
    const recoverWorkspace = Effect.gen(function* () {
      if (recovery === null) return yield* Effect.die("Missing workspace recovery receipt");
      const marker = yield* bootStep;

      if (
        marker !== recovery.materializationId ||
        storedMaterialization !== recovery.materializationId ||
        recovery.instanceId !== identity.instanceId ||
        storedInstance !== identity.instanceId
      ) {
        return yield* HarnessTurnError.make({
          reason: "turnFailed",
          message:
            "The sandbox workspace was replaced during the active turn; its input was not replayed",
        });
      }
      const generationSource = context.generationSource;
      const persist = repositoryFree
        ? Effect.succeed({ head: null, changedPaths: [] })
        : attached !== undefined
          ? checkout.persistGeneration<never, never>(
              {
                repositoryName: attached.repositoryName,
                repositoryId: attached.artifactsRepositoryId,
              },
              recovery.baseHead!,
            )
          : generationSource === undefined
            ? persistRepository(recovery.baseHead!)
            : generationCheckouts.forSource(generationSource).persist(
                Effect.fnUntraced(function* ({ head }) {
                  const checkpoint = yield* GenerationSourceCheckpoint;
                  if (checkpoint === null)
                    return yield* Effect.die("Missing generation handoff owner");
                  yield* checkpoint.prepared(head);
                }),
              );

      return {
        marker,
        reuse: true,
        head: recovery.head,
        baseHead: recovery.baseHead,
        restored: false,
        resumeToken: recovery.resumeToken,
        harnessVersion: recovery.harnessVersion,
        forkDisposition: "indeterminate" as const,
        forkPollAttempts: null,
        persist,
      };
    });
    const prepared =
      recovery !== null
        ? yield* recoverWorkspace
        : repositoryFree || attached !== undefined
          ? yield* prepareIndependentWorkspace
          : context.generationSource === undefined
            ? yield* prepareRepositoryWorkspace
            : yield* prepareGenerationWorkspace(context.generationSource);
    const { marker, reuse, head, restored, resumeToken } = prepared;
    let harnessVersion = prepared.harnessVersion;
    let setupReport: RepositorySetupReport | undefined;

    yield* mark("repositoryReady");
    const prepareSetup = recovery === null && (seeded || attached !== undefined);
    let share = false;

    if (prepareSetup) {
      const pristine =
        storedMaterialization === null &&
        storedHead === null &&
        storedResumeToken === null &&
        storedSetup === null &&
        context.initialCheckout === undefined;

      // Share installation from an untouched checkout only while its head
      // matches the authorized source pin, before any agent has run.
      share =
        pristine &&
        (attached !== undefined ||
          (!hadFork &&
            generation !== null &&
            // Seed admissions keep the same installation-scoped sharing policy
            // as AttachedRepository. Loading their required history does not edit the checkout.
            (seed !== undefined ||
              (context.requiredCommits ?? []).every((commit) => commit === head)) &&
            head ===
              (plannedSource === undefined
                ? yield* checkout.readGenerationMainHead(generation)
                : plannedSource.commitSha)));

      yield* Effect.annotateCurrentSpan("workspace.checkpoint.shareEligible", share);
    }
    if (prepareSetup) {
      setupReport = yield* prepareRepositorySetup({
        execution: {
          sessionId: context.sessionId,
          turnId: context.turnId,
          instanceId: identity.instanceId,
          ...(context.repositoryId === undefined ? {} : { repositoryId: context.repositoryId }),
        },
        sourceKey: attached?.repositoryName ?? (yield* requireRepositorySeed(seedRepository)),
        sourceHead: head!,
        startPreview: context.startRepositoryPreview,
        installed: installedCheckpoint,
        inputs: installedCheckpoint === undefined ? undefined : restoredSetupInputs,
        snapshot: reuse
          ? undefined
          : {
              repository,
              identity,
              share,
            },
      }).pipe(inPhase(agentPhases.repositorySetup));
    }
    yield* mark("repositoryPrepared");
    yield* Effect.annotateCurrentSpan({
      "workspace.readiness.turnToPreparedMillis": timings.repositoryPrepared,
      "workspace.readiness.repositorySetup": setupReport?.state ?? "not-required",
    });
    if (recovery === null && context.generationSource !== undefined && !reuse) {
      const materializationId = yield* store.materializationId.get;

      if (materializationId === null)
        return yield* Effect.die("Missing prepared generation materialization");
      const setupFingerprint = yield* resourceHash(
        `${WORKSPACE_PREPARATION_VERSION}:site-source:${head}`,
      );

      const captureInput = {
        instanceId: identity.instanceId,
        setupFingerprint,
        head: head!,
        materializationId,
        snapshotAllowed: true,
      };
      yield* Effect.gen(function* () {
        const acceptedCapture = yield* store.workspaceCheckpointInput.get;

        if (
          acceptedCapture?.instanceId === identity.instanceId &&
          !sameRepositoryCheckpointInput(acceptedCapture, captureInput)
        )
          return yield* new SandboxOperationError({
            operation: "checkpoint",
            cause: new Error("This boot already owns different checkpoint inputs"),
          });
        yield* store.workspaceCheckpointInput.put(captureInput);
        return yield* captureRepositoryCheckpoint(captureInput);
      }).pipe(
        Effect.flatMap((checkpoint) =>
          checkpoint === null ? Effect.void : store.workspaceCheckpoint.put(checkpoint),
        ),
        Effect.catchCauseIf(
          (cause) =>
            cause.reasons.every(
              (reason) =>
                Cause.isFailReason(reason) &&
                Predicate.isTagged(reason.error, "SandboxOperationError"),
            ),
          (cause) => reportSanitizedCause(cause, { operation: "MessagePipeline.siteCapture" }),
        ),
      );
    }
    if (recovery === null && harnessVersion === null) {
      harnessVersion = harness.version;
      yield* store.harnessVersion.put(harnessVersion);
    }
    const model = context.model ?? config.defaultModel;
    const reasoningEffort = context.reasoningEffort ?? "medium";
    const fast = context.fast ?? true;
    const observed: LifecycleObserved = {
      fork: prepared.forkDisposition === "indeterminate" ? "unknown" : prepared.forkDisposition,
      forkPollAttempts: prepared.forkPollAttempts,
      marker:
        storedMaterialization === null
          ? "no-stored-materialization"
          : marker === null
            ? "absent"
            : marker === storedMaterialization
              ? "matched"
              : "mismatched",
      workspace: reuse ? "reused-in-place" : restored ? "restored-clone" : "fresh-clone",
      harnessSession: resumeToken === null ? "newly-started" : "resumed",
    };

    if (lifecycle !== null) yield* Ref.update(lifecycle, (capture) => ({ ...capture, observed }));

    const materializationId = yield* store.materializationId.get;

    if (materializationId === null)
      return yield* Effect.die("Prepared workspace has no materialization identity");
    let checkpoint = recovery ?? {
      id: context.turnId,
      instanceId: identity.instanceId,
      materializationId,
      head,
      baseHead: prepared.baseHead,
      resumeToken,
      harnessVersion,
      startedAtMillis: startedAt,
      persisted: null,
    };

    yield* store.pipelineTurn.put(checkpoint);
    yield* output.emit({
      type: "agent.ready",
      sessionId: context.sessionId,
      restored,
      materialization: reuse ? "reused" : restored ? "restored" : "fresh",
      head,
      harness: harness.name,
      harnessVersion: harnessVersion ?? "unknown",
      model,
      reasoningEffort,
      fast,
      containerSize,
      readiness: {
        startedAtMillis: startedAt,
        timings: { ...timings },
        containerReused,
        containerStartMillis,
        checkpointSource,
        checkpointRestore,
        ...(plannedSource === undefined ? {} : { fork: (yield* Ref.get(forkState)).state }),
        installation:
          recovery !== null && storedSetup?.prepared
            ? "in-place"
            : (setupReport?.installation ?? "not-required"),
        capture: setupReport?.checkpoint?.capture ?? "not-requested",
        publication: setupReport?.checkpoint?.publication ?? "not-requested",
      },
      lifecycle: {
        preTurn,
        observed,
      },
    });
    const firstEventSeen = yield* Ref.make(false);
    const steering = yield* TurnSteering;
    const harnessTurn = checkForkBeforeModel.pipe(
      Effect.andThen(mark("harnessStarted")),
      Effect.andThen(
        harness.runTurn<AgentEventSinkError>({
          turnId: context.turnId,
          recover: context.recover,
          prompt: context.text,
          resumeToken,
          model,
          reasoningEffort,
          fast,
          ...(context.serviceTier === undefined ? {} : { serviceTier: context.serviceTier }),
          deadlinePolicy,
          ...(context.restrictModelRequests === undefined
            ? {}
            : { restrictModelRequests: context.restrictModelRequests }),
          ...(context.sentryMcp !== true || config.sentryMcp === undefined
            ? {}
            : { sentryMcp: config.sentryMcp }),
          ...(config.commitCoauthorTrailer === undefined
            ? {}
            : { commitCoauthorTrailer: config.commitCoauthorTrailer }),
          ...(context.outputSchema === undefined ? {} : { outputSchema: context.outputSchema }),
          ...(context.deadlineAtMs === undefined ? {} : { deadlineAtMs: context.deadlineAtMs }),
          ...(steering === null ? {} : { onSteeringReady: steering.ready }),
          onEvent: (event) =>
            Effect.gen(function* () {
              const seen = yield* Ref.getAndSet(firstEventSeen, true);

              if (!seen) yield* mark("firstHarnessEvent");
              yield* output.emit(event);
            }),
        }),
      ),
      Effect.tapError((error) =>
        Predicate.isObject(error) &&
        Predicate.hasProperty(error, "_tag") &&
        error._tag === "HarnessTurnError" &&
        Predicate.hasProperty(error, "reason") &&
        (error.reason === "freshSessionRequired" || error.reason === "deadlineExceeded")
          ? store.resumeToken.delete.pipe(Effect.asVoid)
          : Effect.void,
      ),
      inPhase(agentPhases.harness),
    );
    // Services boot alongside the harness turn, never gating or failing it; a
    // failed service start is logged data and shows up in the archived spans.
    const servicesStep = (
      context.generationSource === undefined ? Effect.void : ensureWorkspaceServices(!reuse)
    ).pipe(
      Effect.catchCause((cause) =>
        Effect.logError("sandbox-agent workspace service start failed", cause),
      ),
    );
    const [turn] = yield* Effect.all([harnessTurn, servicesStep] as const, {
      concurrency: "unbounded",
    });

    yield* mark("harnessExit");
    yield* store.resumeToken.put(turn.resumeToken);
    const persisted = checkpoint.persisted ?? (yield* prepared.persist);

    checkpoint = { ...checkpoint, persisted };
    yield* store.pipelineTurn.put(checkpoint);

    yield* complete(persisted, turn);
  });

  yield* withSandboxLease(context.turnId, turnBody).pipe(
    Effect.provideService(SessionSandbox, sandbox),
  );
}, Effect.scoped);

/** @deprecated Compatibility adapter for hosts migrating to runTurn and its inward ports. */
export const runMessagePipeline = <CallbackError = never>(
  context: MessageContext<CallbackError>,
): Effect.Effect<
  void,
  Effect.Error<ReturnType<typeof runTurn>> | CallbackError,
  Effect.Services<ReturnType<typeof runTurn>>
> => {
  const { lifecycle, onSteeringReady, onGenerationSourcePrepared, ...request } = context;

  // The old API admits arbitrary callback failures. Erase only their types at
  // this adapter, preserving the original causes and native recovery predicates;
  // CallbackError remains explicit in the returned error channel.
  return runTurn(request).pipe(
    Effect.provideService(TurnLifecycleCapture, lifecycle),
    Effect.provideService(
      TurnSteering,
      onSteeringReady === undefined
        ? null
        : {
            ready: onSteeringReady as (
              steer: HarnessTurnSteer,
            ) => Effect.Effect<void, AgentEventSinkError>,
          },
    ),
    Effect.provideService(
      GenerationSourceCheckpoint,
      onGenerationSourcePrepared === undefined
        ? null
        : {
            prepared: onGenerationSourcePrepared as (
              head: string,
            ) => Effect.Effect<void, RepositoryPersistenceCheckpointError>,
          },
    ),
  );
};
