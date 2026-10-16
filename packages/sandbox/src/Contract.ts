import { Schema } from "effect";

import { RepositorySetupCheckpoint } from "./model/RepositorySetup";

/**
 * Wire schemas for the sandbox-agent HTTP protocol: the message request body,
 * the NDJSON event stream, and the session-state response. Route parsing and
 * JSON encoding helpers live in `./Http`.
 */

/** Version emitted in every `agent.run` event for wire-compatibility checks. */
export const PROTOCOL_VERSION = 2;

/** Session-id charset source, shared with route parsing in `./Http` so the
 * schema and the route regex cannot drift apart. */
export const SESSION_ID_PATTERN_SOURCE = "[A-Za-z0-9][A-Za-z0-9_-]{0,63}";

const sessionIdPattern = new RegExp(`^${SESSION_ID_PATTERN_SOURCE}$`, "u");

/** Conservative URL-safe session id; hashed before becoming resource names. */
export const SessionId = Schema.String.pipe(
  Schema.check(
    Schema.makeFilter((value) => sessionIdPattern.test(value), {
      expected: "a URL-safe session id of at most 64 characters",
    }),
  ),
);
export type SessionId = typeof SessionId.Type;

/** Harnesses this protocol can carry; each one tags its own dialect events. */
export const HarnessName = Schema.Literals(["codex"]);
export type HarnessName = typeof HarnessName.Type;

/** Reasoning-effort ladder; each harness maps it onto its own model knobs. */
export const ReasoningEffort = Schema.NonEmptyString.check(Schema.isMaxLength(64));
export type ReasoningEffort = typeof ReasoningEffort.Type;

/** Container size tiers available to a session. */
export const ContainerSize = Schema.Literals(["standard", "big"]);
export type ContainerSize = typeof ContainerSize.Type;

/** Durable coordinator state as found before this turn ran. */
export const LifecyclePreTurn = Schema.Struct({
  forkRecorded: Schema.Boolean,
  materializationRecorded: Schema.Boolean,
  resumeTokenRecorded: Schema.Boolean,
  containerSize: Schema.NullOr(ContainerSize),
  verifiedHead: Schema.NullOr(Schema.String),
});
export type LifecyclePreTurn = typeof LifecyclePreTurn.Type;

/**
 * Runtime facts discovered while preparing a turn. These values describe
 * what actually happened, rather than what durable coordinator state implied.
 */
export const LifecycleObserved = Schema.Struct({
  fork: Schema.Literals(["existing", "created", "unknown"]),
  forkPollAttempts: Schema.NullOr(Schema.Int),
  marker: Schema.Literals([
    "matched",
    "mismatched",
    "absent",
    "no-stored-materialization",
    "unknown",
  ]),
  workspace: Schema.Literals(["fresh-clone", "reused-in-place", "restored-clone", "unknown"]),
  harnessSession: Schema.Literals(["newly-started", "resumed", "unknown"]),
});
export type LifecycleObserved = typeof LifecycleObserved.Type;

/** Durable session pointers after a turn has persisted successfully. */
export const LifecyclePostTurn = Schema.Struct({
  verifiedHead: Schema.NullOr(Schema.String),
  resumeToken: Schema.NullOr(Schema.String),
});
export type LifecyclePostTurn = typeof LifecyclePostTurn.Type;

/** `postTurn: null` means the pipeline failed before durable completion. */
export const RunLifecycle = Schema.Struct({
  preTurn: LifecyclePreTurn,
  observed: LifecycleObserved,
  postTurn: Schema.NullOr(LifecyclePostTurn),
});
export type RunLifecycle = typeof RunLifecycle.Type;

/** Sentinel used when a run fails before a lifecycle fact can be observed. */
export const unknownLifecycleObserved: LifecycleObserved = {
  fork: "unknown",
  forkPollAttempts: null,
  marker: "unknown",
  workspace: "unknown",
  harnessSession: "unknown",
};

/**
 * One long-lived workspace process (dev server, database) the kernel keeps
 * running alongside harness turns. The argv runs from the repository root by
 * default; the caller owns what it does. Services never block or fail a turn.
 */
export const WorkspaceService = Schema.Struct({
  name: Schema.String.pipe(
    Schema.check(
      Schema.makeFilter((value) => /^[a-z][a-z0-9-]{0,31}$/u.test(value), {
        expected: "a kebab-case service name",
      }),
    ),
  ),
  argv: Schema.Array(
    Schema.String.pipe(
      Schema.check(
        Schema.makeFilter((value) => value.length >= 1 && value.length <= 16_384, {
          expected: "an argv token between 1 and 16384 characters",
        }),
      ),
    ),
  ).pipe(
    Schema.check(
      Schema.makeFilter((value) => value.length >= 1 && value.length <= 16, {
        expected: "an argv of 1 to 16 tokens",
      }),
    ),
  ),
});
export type WorkspaceService = typeof WorkspaceService.Type;

/** Body of `POST /sessions/:sessionId/messages`, bounded to 16 KiB. */
export const MessageRequest = Schema.Struct({
  text: Schema.String.pipe(
    Schema.check(
      Schema.makeFilter((value) => value.length >= 1 && value.length <= 16_384, {
        expected: "a message between 1 and 16384 characters",
      }),
    ),
  ),
  /** Per-turn model override; the deployment default applies when absent. */
  model: Schema.optionalKey(
    Schema.String.pipe(
      Schema.check(
        Schema.makeFilter((value) => /^[A-Za-z0-9._-]{1,64}$/u.test(value), {
          expected: "a model slug",
        }),
      ),
    ),
  ),
  reasoningEffort: Schema.optionalKey(ReasoningEffort),
  fast: Schema.optionalKey(Schema.Boolean),
  /** Requested size for this turn; defaults to standard. */
  containerSize: Schema.optionalKey(ContainerSize),
  /** Optional versioned prompt classification copied to the host's run sink. */
  promptProfile: Schema.optionalKey(
    Schema.Struct({
      name: Schema.String.pipe(
        Schema.check(
          Schema.makeFilter((value) => /^[a-z][a-z0-9-]{0,63}$/u.test(value), {
            expected: "a kebab-case profile name",
          }),
        ),
      ),
      version: Schema.Int,
    }),
  ),
  /** Workspace processes to keep running this turn, concurrent with the
   * harness. Distinct names; a re-materialized workspace restarts them. */
  services: Schema.optionalKey(
    Schema.Array(WorkspaceService).pipe(
      Schema.check(
        Schema.makeFilter(
          (value) => value.length <= 4 && new Set(value.map((s) => s.name)).size === value.length,
          { expected: "at most 4 uniquely named services" },
        ),
      ),
    ),
  ),
  /** Caller idempotency key. Any key already accepted during this session's
   * lifetime replays its original receipt instead of executing a new turn. */
  idempotencyKey: Schema.optionalKey(
    Schema.String.pipe(
      Schema.check(
        Schema.makeFilter((value) => /^[A-Za-z0-9._-]{1,64}$/u.test(value), {
          expected: "an idempotency key of at most 64 URL-safe characters",
        }),
      ),
    ),
  ),
});
export type MessageRequest = typeof MessageRequest.Type;

/** JSON-string codec for the message endpoint's bounded request body. */
export const MessageRequestFromJson = Schema.fromJsonString(MessageRequest);

/** JSON body returned when an idempotency key matches any receipt accepted
 * during this session's lifetime. Execution and archive disposition are
 * separate because archival deliberately runs after the session busy lock is
 * released. */
export const RunReceiptState = Schema.Literals(["accepted", "running", "terminal"]);
export type RunReceiptState = typeof RunReceiptState.Type;

export const RunArchiveState = Schema.Literals([
  "pending",
  "stored",
  "unavailable",
  "failed",
  "unknown",
]);
export type RunArchiveState = typeof RunArchiveState.Type;

export const DuplicateRunReceipt = Schema.Struct({
  duplicate: Schema.Literal(true),
  runId: Schema.String,
  archiveKey: Schema.String,
  state: RunReceiptState,
  archiveState: RunArchiveState,
});
export type DuplicateRunReceipt = typeof DuplicateRunReceipt.Type;

/*
 * Control-surface responses. The coordinator builds these literals and
 * clients decode through the same schemas, so the wire contract has exactly
 * one owner instead of hand-written probes on both sides.
 */

/** One timed cleanup operation; a failure is data, never an abort. */
export const CleanupOp = Schema.Struct({
  name: Schema.String,
  durationMs: Schema.Finite,
  outcome: Schema.Literals(["ok", "failed", "skipped"]),
});
export type CleanupOp = typeof CleanupOp.Type;

/** Body of `POST /sessions/:sessionId/restart` (`?force=true` preempts). */
export const RestartResponse = Schema.Struct({
  restarted: Schema.Boolean,
  /** True when the restart hard-stopped a turn that was still running. */
  preempted: Schema.Boolean,
  retained: Schema.Struct({
    forkName: Schema.NullOr(Schema.String),
    verifiedHead: Schema.NullOr(Schema.String),
  }),
  cleanup: Schema.Array(CleanupOp),
});
export type RestartResponse = typeof RestartResponse.Type;

/** Body of the lifecycle-test `POST /sessions/:sessionId/simulate-replacement`. */
export const SimulateReplacementResponse = Schema.Struct({
  replaced: Schema.Boolean,
  doStateRetained: Schema.Literal(true),
  cleanup: Schema.Array(CleanupOp),
});
export type SimulateReplacementResponse = typeof SimulateReplacementResponse.Type;

/** Body of `DELETE /sessions/:sessionId`; `null` names report nothing deleted. */
export const DeleteSessionResponse = Schema.Struct({
  deleted: Schema.Struct({
    sandbox: Schema.NullOr(Schema.String),
    fork: Schema.NullOr(Schema.String),
  }),
  cleanup: Schema.Array(CleanupOp),
});
export type DeleteSessionResponse = typeof DeleteSessionResponse.Type;

/*
 * Codex dialect. Harness events cross the wire untranslated inside their
 * harness's own envelope; adding a harness adds an envelope variant instead
 * of normalizing every dialect into one shared vocabulary.
 */

/**
 * The persisted Codex item-event dialect. The harness adapts app-server
 * notifications to the exec-shaped records already stored in task histories;
 * the kernel requires only a `type` discriminant and passes the rest through.
 */
export const CodexEvent = Schema.StructWithRest(Schema.Struct({ type: Schema.String }), [
  Schema.Record(Schema.String, Schema.Unknown),
]);
export type CodexEvent = typeof CodexEvent.Type;

/** Terminal turn events; exactly one must arrive before the connection closes. */
export const codexTerminalEventTypes: ReadonlySet<string> = new Set([
  "turn.completed",
  "turn.failed",
  "error",
]);

/** Monotonic per-phase durations reported with every message. */
export const AgentTimings = Schema.Record(Schema.String, Schema.Finite);
export type AgentTimings = typeof AgentTimings.Type;

/** Preparation facts observed before harness startup, retained independently of trace sampling. */
export const AgentReadiness = Schema.Struct({
  startedAtMillis: Schema.Finite,
  timings: AgentTimings,
  containerReused: Schema.Boolean,
  /** Boot intent through authenticated supervisor readiness, including capacity; not app readiness. */
  containerStartMillis: Schema.Finite,
  checkpointSource: Schema.Literals(["none", "session", "shared"]),
  checkpointRestore: Schema.Literals(["not-requested", "miss", "restored", "rejected"]),
  /** A retained target is pending until its generation and pinned main are verified. */
  fork: Schema.optionalKey(Schema.Literals(["pending", "ready", "failed"])),
  installation: Schema.Literals(["executed", "checkpoint", "in-place", "absent", "not-required"]),
  ...RepositorySetupCheckpoint.fields,
});
export type AgentReadiness = typeof AgentReadiness.Type;

/** How this turn's workspace came to be. */
export const WorkspaceMaterialization = Schema.Literals(["fresh", "reused", "restored"]);
export type WorkspaceMaterialization = typeof WorkspaceMaterialization.Type;

/** Kernel-emitted phase names: the one vocabulary shared by kernel call
 * sites, run-record spans, and timeline clients. Renaming a value here is a
 * wire change. */
export const agentPhases = {
  fork: "fork",
  containerBoot: "container-boot+marker",
  clone: "clone",
  repositorySetup: "repository-setup",
  harnessWarmup: "harness-warmup",
  harness: "harness",
  gitStatus: "git-status",
  gitCommit: "git-commit",
  gitPush: "git-push",
  artifactsVerify: "artifacts-verify",
} as const;
export type AgentPhase = (typeof agentPhases)[keyof typeof agentPhases];

/** Live phase boundary for timeline rendering; `at` is ms since the request
 * was accepted. Every started phase also emits its end, on failure too. */
export const AgentPhaseEvent = Schema.Struct({
  type: Schema.Literal("agent.phase"),
  phase: Schema.String,
  status: Schema.Literals(["start", "end"]),
  at: Schema.Finite,
  outcome: Schema.optionalKey(Schema.Literals(["ok", "failed"])),
});
export type AgentPhaseEvent = typeof AgentPhaseEvent.Type;

/** First NDJSON record of every message run: the run id is assigned before
 * expensive work and doubles as the response header `x-agent-run-id`, so a
 * disconnected client can retrieve the same archive deterministically at
 * `archiveKey`. */
export const AgentRunEvent = Schema.Struct({
  type: Schema.Literal("agent.run"),
  runId: Schema.String,
  archiveKey: Schema.String,
  protocolVersion: Schema.Int,
});
export type AgentRunEvent = typeof AgentRunEvent.Type;

export const AgentReadyEvent = Schema.Struct({
  type: Schema.Literal("agent.ready"),
  sessionId: Schema.String,
  restored: Schema.Boolean,
  /** fresh = first message ever; reused = warm container; restored =
   * container was replaced, workspace re-cloned from the durable fork. */
  materialization: WorkspaceMaterialization,
  head: Schema.NullOr(Schema.String),
  /** Which harness runs this turn, and the version it reported. */
  harness: HarnessName,
  harnessVersion: Schema.String,
  /** The model this turn actually runs on. */
  model: Schema.String,
  reasoningEffort: ReasoningEffort,
  fast: Schema.Boolean,
  containerSize: ContainerSize,
  /** Observed per-layer lifecycle state; post-turn state lands in the
   * archived run document. */
  lifecycle: Schema.Struct({
    preTurn: LifecyclePreTurn,
    observed: LifecycleObserved,
  }),
  readiness: Schema.optionalKey(AgentReadiness),
});
export type AgentReadyEvent = typeof AgentReadyEvent.Type;

/** Codex's dialect envelope: its events cross the wire untranslated. */
export const CodexDialectEvent = Schema.Struct({
  type: Schema.Literal("codex.event"),
  event: CodexEvent,
});
export type CodexDialectEvent = typeof CodexDialectEvent.Type;

/** Per-harness dialect envelopes; grows one member per supported harness. */
export const HarnessDialectEvent = CodexDialectEvent;
export type HarnessDialectEvent = typeof HarnessDialectEvent.Type;

export const AgentPersistedEvent = Schema.Struct({
  type: Schema.Literal("agent.persisted"),
  head: Schema.String,
  changedPaths: Schema.Array(Schema.String),
});
export type AgentPersistedEvent = typeof AgentPersistedEvent.Type;

export const AgentCompletedEvent = Schema.Struct({
  type: Schema.Literal("agent.completed"),
  /** Opaque harness continuity token; the next turn resumes this session. */
  resumeToken: Schema.String,
  /** Harness-reported token usage for the turn, `null` when unreported. */
  usage: Schema.Unknown,
  timings: AgentTimings,
});
export type AgentCompletedEvent = typeof AgentCompletedEvent.Type;

export const AgentFailedEvent = Schema.Struct({
  type: Schema.Literal("agent.failed"),
  reason: Schema.String,
  message: Schema.String,
});
export type AgentFailedEvent = typeof AgentFailedEvent.Type;

/** The accepted execution is still recoverable; this is not a terminal result. */
export const AgentRecoveringEvent = Schema.Struct({
  type: Schema.Literal("agent.recovering"),
  reason: Schema.String,
  message: Schema.String,
});
export type AgentRecoveringEvent = typeof AgentRecoveringEvent.Type;

export const AgentArchivedEvent = Schema.Struct({
  type: Schema.Literal("agent.archived"),
  key: Schema.String,
  /** Opaque host-issued capability for retrieving the archived run. */
  token: Schema.NullOr(Schema.String),
  runId: Schema.String,
  /** Archive serialization and sink write cost, measured server-side; they
   * cannot live inside the document they produce, so they travel here and
   * the client records them with its own receive offsets. */
  serializeMs: Schema.Finite,
  putMs: Schema.Finite,
  bytes: Schema.Int,
});
export type AgentArchivedEvent = typeof AgentArchivedEvent.Type;

/**
 * Complete event vocabulary streamed by the session endpoint. Kernel-owned
 * `agent.*` records frame the untouched harness dialect stream and make
 * setup, persistence, completion, failure, and optional archival observable.
 */
export const AgentEvent = Schema.Union([
  AgentPhaseEvent,
  AgentRunEvent,
  AgentReadyEvent,
  CodexDialectEvent,
  AgentPersistedEvent,
  AgentCompletedEvent,
  AgentFailedEvent,
  AgentRecoveringEvent,
  AgentArchivedEvent,
]);
export type AgentEvent = typeof AgentEvent.Type;

/** Inspectable session state returned by `GET /sessions/:sessionId`. */
export const SessionStateResponse = Schema.Struct({
  sessionId: Schema.String,
  forkName: Schema.NullOr(Schema.String),
  sandboxId: Schema.String,
  materializationId: Schema.NullOr(Schema.String),
  resumeToken: Schema.NullOr(Schema.String),
  verifiedHead: Schema.NullOr(Schema.String),
  containerSize: Schema.NullOr(ContainerSize),
  busy: Schema.Boolean,
  timings: Schema.NullOr(AgentTimings),
});
export type SessionStateResponse = typeof SessionStateResponse.Type;
