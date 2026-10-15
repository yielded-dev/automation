import { RepositorySetupCheckpoint } from "./model/RepositorySetup.mjs";
import { Schema } from "effect";
//#region src/Contract.ts
/**
* Wire schemas for the sandbox-agent HTTP protocol: the message request body,
* the NDJSON event stream, and the session-state response. Route parsing and
* JSON encoding helpers live in `./Http`.
*/
/** Version emitted in every `agent.run` event for wire-compatibility checks. */
const PROTOCOL_VERSION = 2;
/** Session-id charset source, shared with route parsing in `./Http` so the
* schema and the route regex cannot drift apart. */
const SESSION_ID_PATTERN_SOURCE = "[A-Za-z0-9][A-Za-z0-9_-]{0,63}";
const sessionIdPattern = new RegExp(`^${SESSION_ID_PATTERN_SOURCE}$`, "u");
/** Conservative URL-safe session id; hashed before becoming resource names. */
const SessionId = Schema.String.pipe(Schema.check(Schema.makeFilter((value) => sessionIdPattern.test(value), { expected: "a URL-safe session id of at most 64 characters" })));
/** Harnesses this protocol can carry; each one tags its own dialect events. */
const HarnessName = Schema.Literals(["codex"]);
/** Reasoning-effort ladder; each harness maps it onto its own model knobs. */
const ReasoningEffort = Schema.NonEmptyString.check(Schema.isMaxLength(64));
/** Container size tiers available to a session. */
const ContainerSize = Schema.Literals(["standard", "big"]);
/** Durable coordinator state as found before this turn ran. */
const LifecyclePreTurn = Schema.Struct({
	forkRecorded: Schema.Boolean,
	materializationRecorded: Schema.Boolean,
	resumeTokenRecorded: Schema.Boolean,
	containerSize: Schema.NullOr(ContainerSize),
	verifiedHead: Schema.NullOr(Schema.String)
});
/**
* Runtime facts discovered while preparing a turn. These values describe
* what actually happened, rather than what durable coordinator state implied.
*/
const LifecycleObserved = Schema.Struct({
	fork: Schema.Literals([
		"existing",
		"created",
		"unknown"
	]),
	forkPollAttempts: Schema.NullOr(Schema.Int),
	marker: Schema.Literals([
		"matched",
		"mismatched",
		"absent",
		"no-stored-materialization",
		"unknown"
	]),
	workspace: Schema.Literals([
		"fresh-clone",
		"reused-in-place",
		"restored-clone",
		"unknown"
	]),
	harnessSession: Schema.Literals([
		"newly-started",
		"resumed",
		"unknown"
	])
});
/** Durable session pointers after a turn has persisted successfully. */
const LifecyclePostTurn = Schema.Struct({
	verifiedHead: Schema.NullOr(Schema.String),
	resumeToken: Schema.NullOr(Schema.String)
});
/** `postTurn: null` means the pipeline failed before durable completion. */
const RunLifecycle = Schema.Struct({
	preTurn: LifecyclePreTurn,
	observed: LifecycleObserved,
	postTurn: Schema.NullOr(LifecyclePostTurn)
});
/** Sentinel used when a run fails before a lifecycle fact can be observed. */
const unknownLifecycleObserved = {
	fork: "unknown",
	forkPollAttempts: null,
	marker: "unknown",
	workspace: "unknown",
	harnessSession: "unknown"
};
/**
* One long-lived workspace process (dev server, database) the kernel keeps
* running alongside harness turns. The argv runs from the repository root by
* default; the caller owns what it does. Services never block or fail a turn.
*/
const WorkspaceService = Schema.Struct({
	name: Schema.String.pipe(Schema.check(Schema.makeFilter((value) => /^[a-z][a-z0-9-]{0,31}$/u.test(value), { expected: "a kebab-case service name" }))),
	argv: Schema.Array(Schema.String.pipe(Schema.check(Schema.makeFilter((value) => value.length >= 1 && value.length <= 16384, { expected: "an argv token between 1 and 16384 characters" })))).pipe(Schema.check(Schema.makeFilter((value) => value.length >= 1 && value.length <= 16, { expected: "an argv of 1 to 16 tokens" })))
});
/** Body of `POST /sessions/:sessionId/messages`, bounded to 16 KiB. */
const MessageRequest = Schema.Struct({
	text: Schema.String.pipe(Schema.check(Schema.makeFilter((value) => value.length >= 1 && value.length <= 16384, { expected: "a message between 1 and 16384 characters" }))),
	/** Per-turn model override; the deployment default applies when absent. */
	model: Schema.optionalKey(Schema.String.pipe(Schema.check(Schema.makeFilter((value) => /^[A-Za-z0-9._-]{1,64}$/u.test(value), { expected: "a model slug" })))),
	reasoningEffort: Schema.optionalKey(ReasoningEffort),
	fast: Schema.optionalKey(Schema.Boolean),
	/** Requested size for this turn; defaults to standard. */
	containerSize: Schema.optionalKey(ContainerSize),
	/** Optional versioned prompt classification copied to the host's run sink. */
	promptProfile: Schema.optionalKey(Schema.Struct({
		name: Schema.String.pipe(Schema.check(Schema.makeFilter((value) => /^[a-z][a-z0-9-]{0,63}$/u.test(value), { expected: "a kebab-case profile name" }))),
		version: Schema.Int
	})),
	/** Workspace processes to keep running this turn, concurrent with the
	* harness. Distinct names; a re-materialized workspace restarts them. */
	services: Schema.optionalKey(Schema.Array(WorkspaceService).pipe(Schema.check(Schema.makeFilter((value) => value.length <= 4 && new Set(value.map((s) => s.name)).size === value.length, { expected: "at most 4 uniquely named services" })))),
	/** Caller idempotency key. Any key already accepted during this session's
	* lifetime replays its original receipt instead of executing a new turn. */
	idempotencyKey: Schema.optionalKey(Schema.String.pipe(Schema.check(Schema.makeFilter((value) => /^[A-Za-z0-9._-]{1,64}$/u.test(value), { expected: "an idempotency key of at most 64 URL-safe characters" }))))
});
/** JSON-string codec for the message endpoint's bounded request body. */
const MessageRequestFromJson = Schema.fromJsonString(MessageRequest);
/** JSON body returned when an idempotency key matches any receipt accepted
* during this session's lifetime. Execution and archive disposition are
* separate because archival deliberately runs after the session busy lock is
* released. */
const RunReceiptState = Schema.Literals([
	"accepted",
	"running",
	"terminal"
]);
const RunArchiveState = Schema.Literals([
	"pending",
	"stored",
	"unavailable",
	"failed",
	"unknown"
]);
const DuplicateRunReceipt = Schema.Struct({
	duplicate: Schema.Literal(true),
	runId: Schema.String,
	archiveKey: Schema.String,
	state: RunReceiptState,
	archiveState: RunArchiveState
});
/** One timed cleanup operation; a failure is data, never an abort. */
const CleanupOp = Schema.Struct({
	name: Schema.String,
	durationMs: Schema.Finite,
	outcome: Schema.Literals([
		"ok",
		"failed",
		"skipped"
	])
});
/** Body of `POST /sessions/:sessionId/restart` (`?force=true` preempts). */
const RestartResponse = Schema.Struct({
	restarted: Schema.Boolean,
	/** True when the restart hard-stopped a turn that was still running. */
	preempted: Schema.Boolean,
	retained: Schema.Struct({
		forkName: Schema.NullOr(Schema.String),
		verifiedHead: Schema.NullOr(Schema.String)
	}),
	cleanup: Schema.Array(CleanupOp)
});
/** Body of the lifecycle-test `POST /sessions/:sessionId/simulate-replacement`. */
const SimulateReplacementResponse = Schema.Struct({
	replaced: Schema.Boolean,
	doStateRetained: Schema.Literal(true),
	cleanup: Schema.Array(CleanupOp)
});
/** Body of `DELETE /sessions/:sessionId`; `null` names report nothing deleted. */
const DeleteSessionResponse = Schema.Struct({
	deleted: Schema.Struct({
		sandbox: Schema.NullOr(Schema.String),
		fork: Schema.NullOr(Schema.String)
	}),
	cleanup: Schema.Array(CleanupOp)
});
/**
* The persisted Codex item-event dialect. The harness adapts app-server
* notifications to the exec-shaped records already stored in task histories;
* the kernel requires only a `type` discriminant and passes the rest through.
*/
const CodexEvent = Schema.StructWithRest(Schema.Struct({ type: Schema.String }), [Schema.Record(Schema.String, Schema.Unknown)]);
/** Terminal turn events; exactly one must arrive before the connection closes. */
const codexTerminalEventTypes = /* @__PURE__ */ new Set([
	"turn.completed",
	"turn.failed",
	"error"
]);
/** Monotonic per-phase durations reported with every message. */
const AgentTimings = Schema.Record(Schema.String, Schema.Finite);
/** Preparation facts observed before harness startup, retained independently of trace sampling. */
const AgentReadiness = Schema.Struct({
	startedAtMillis: Schema.Finite,
	timings: AgentTimings,
	containerReused: Schema.Boolean,
	/** Boot intent through authenticated supervisor readiness, including capacity; not app readiness. */
	containerStartMillis: Schema.Finite,
	checkpointSource: Schema.Literals([
		"none",
		"session",
		"shared"
	]),
	checkpointRestore: Schema.Literals([
		"not-requested",
		"miss",
		"restored",
		"rejected"
	]),
	/** A retained target is pending until its generation and pinned main are verified. */
	fork: Schema.optionalKey(Schema.Literals([
		"pending",
		"ready",
		"failed"
	])),
	installation: Schema.Literals([
		"executed",
		"checkpoint",
		"in-place",
		"absent",
		"not-required"
	]),
	...RepositorySetupCheckpoint.fields
});
/** How this turn's workspace came to be. */
const WorkspaceMaterialization = Schema.Literals([
	"fresh",
	"reused",
	"restored"
]);
/** Kernel-emitted phase names: the one vocabulary shared by kernel call
* sites, run-record spans, and timeline clients. Renaming a value here is a
* wire change. */
const agentPhases = {
	fork: "fork",
	containerBoot: "container-boot+marker",
	clone: "clone",
	repositorySetup: "repository-setup",
	harnessWarmup: "harness-warmup",
	harness: "harness",
	gitStatus: "git-status",
	gitCommit: "git-commit",
	gitPush: "git-push",
	artifactsVerify: "artifacts-verify"
};
/** Live phase boundary for timeline rendering; `at` is ms since the request
* was accepted. Every started phase also emits its end, on failure too. */
const AgentPhaseEvent = Schema.Struct({
	type: Schema.Literal("agent.phase"),
	phase: Schema.String,
	status: Schema.Literals(["start", "end"]),
	at: Schema.Finite,
	outcome: Schema.optionalKey(Schema.Literals(["ok", "failed"]))
});
/** First NDJSON record of every message run: the run id is assigned before
* expensive work and doubles as the response header `x-agent-run-id`, so a
* disconnected client can retrieve the same archive deterministically at
* `archiveKey`. */
const AgentRunEvent = Schema.Struct({
	type: Schema.Literal("agent.run"),
	runId: Schema.String,
	archiveKey: Schema.String,
	protocolVersion: Schema.Int
});
const AgentReadyEvent = Schema.Struct({
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
		observed: LifecycleObserved
	}),
	readiness: Schema.optionalKey(AgentReadiness)
});
/** Codex's dialect envelope: its events cross the wire untranslated. */
const CodexDialectEvent = Schema.Struct({
	type: Schema.Literal("codex.event"),
	event: CodexEvent
});
/** Per-harness dialect envelopes; grows one member per supported harness. */
const HarnessDialectEvent = CodexDialectEvent;
const AgentPersistedEvent = Schema.Struct({
	type: Schema.Literal("agent.persisted"),
	head: Schema.String,
	changedPaths: Schema.Array(Schema.String)
});
const AgentCompletedEvent = Schema.Struct({
	type: Schema.Literal("agent.completed"),
	/** Opaque harness continuity token; the next turn resumes this session. */
	resumeToken: Schema.String,
	/** Harness-reported token usage for the turn, `null` when unreported. */
	usage: Schema.Unknown,
	timings: AgentTimings
});
const AgentFailedEvent = Schema.Struct({
	type: Schema.Literal("agent.failed"),
	reason: Schema.String,
	message: Schema.String
});
/** The accepted execution is still recoverable; this is not a terminal result. */
const AgentRecoveringEvent = Schema.Struct({
	type: Schema.Literal("agent.recovering"),
	reason: Schema.String,
	message: Schema.String
});
const AgentArchivedEvent = Schema.Struct({
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
	bytes: Schema.Int
});
/**
* Complete event vocabulary streamed by the session endpoint. Kernel-owned
* `agent.*` records frame the untouched harness dialect stream and make
* setup, persistence, completion, failure, and optional archival observable.
*/
const AgentEvent = Schema.Union([
	AgentPhaseEvent,
	AgentRunEvent,
	AgentReadyEvent,
	CodexDialectEvent,
	AgentPersistedEvent,
	AgentCompletedEvent,
	AgentFailedEvent,
	AgentRecoveringEvent,
	AgentArchivedEvent
]);
/** Inspectable session state returned by `GET /sessions/:sessionId`. */
const SessionStateResponse = Schema.Struct({
	sessionId: Schema.String,
	forkName: Schema.NullOr(Schema.String),
	sandboxId: Schema.String,
	materializationId: Schema.NullOr(Schema.String),
	resumeToken: Schema.NullOr(Schema.String),
	verifiedHead: Schema.NullOr(Schema.String),
	containerSize: Schema.NullOr(ContainerSize),
	busy: Schema.Boolean,
	timings: Schema.NullOr(AgentTimings)
});
//#endregion
export { AgentArchivedEvent, AgentCompletedEvent, AgentEvent, AgentFailedEvent, AgentPersistedEvent, AgentPhaseEvent, AgentReadiness, AgentReadyEvent, AgentRecoveringEvent, AgentRunEvent, AgentTimings, CleanupOp, CodexDialectEvent, CodexEvent, ContainerSize, DeleteSessionResponse, DuplicateRunReceipt, HarnessDialectEvent, HarnessName, LifecycleObserved, LifecyclePostTurn, LifecyclePreTurn, MessageRequest, MessageRequestFromJson, PROTOCOL_VERSION, ReasoningEffort, RestartResponse, RunArchiveState, RunLifecycle, RunReceiptState, SESSION_ID_PATTERN_SOURCE, SessionId, SessionStateResponse, SimulateReplacementResponse, WorkspaceMaterialization, WorkspaceService, agentPhases, codexTerminalEventTypes, unknownLifecycleObserved };
