import { RepositorySetupReceipt, RepositorySetupReport } from "../model/RepositorySetup.mjs";
import { AgentTimings, ContainerSize, RunArchiveState, RunReceiptState, WorkspaceService } from "../Contract.mjs";
import { RepositoryAuthorizationGrant, WorkspaceCheckpoint } from "./NativeSandbox.mjs";
import { NativeWorkspaceIdentityData } from "./NativeWorkspaceIdentity.mjs";
import { StoredWorkspaceBaselinePreparation } from "./WorkspaceBaselinePreparationState.mjs";
import { Cause, Context, Effect, Layer, Schema } from "effect";
import { DurableObjectState } from "effect-cf";
//#region src/cloudflare/SessionStateStore.ts
const MAX_RECENT_RUN_RECEIPTS = 32;
/** Durable caller receipt retained for the lifetime of its session. */
const StoredRunReceipt = Schema.Struct({
	idempotencyKey: Schema.String,
	runId: Schema.String,
	archiveKey: Schema.String,
	state: RunReceiptState,
	archiveState: RunArchiveState
});
/** A process the kernel started or adopted for a declared workspace service. */
const ManagedWorkspaceService = Schema.Struct({
	service: WorkspaceService,
	processId: Schema.String
});
const StoredRunReceipts = Schema.Array(StoredRunReceipt).pipe(Schema.check(Schema.makeFilter((receipts) => receipts.length <= 32, { expected: `at most 32 stored run receipts` })));
const SessionStateSnapshot = Schema.Struct({
	forkName: Schema.NullOr(Schema.String),
	verifiedHead: Schema.NullOr(Schema.String),
	materializationId: Schema.NullOr(Schema.String),
	resumeToken: Schema.NullOr(Schema.String),
	harnessVersion: Schema.NullOr(Schema.String),
	containerSize: Schema.NullOr(ContainerSize),
	lastTimings: Schema.NullOr(AgentTimings),
	recentRunReceipts: StoredRunReceipts,
	managedWorkspaceServices: Schema.Array(ManagedWorkspaceService)
});
const PersistedTurnResult = Schema.Struct({
	resumeToken: Schema.String,
	terminalEventType: Schema.String,
	lastAgentMessage: Schema.NullOr(Schema.String),
	usage: Schema.Unknown,
	stderrTail: Schema.String
});
/** Private recovery capability. Never include this record in runtime inspection or archives. */
const StoredCodexTurn = Schema.Struct({
	id: Schema.String,
	processId: Schema.String,
	token: Schema.String,
	configuration: Schema.Struct({
		command: Schema.NonEmptyArray(Schema.String),
		cwd: Schema.String,
		env: Schema.Record(Schema.String, Schema.String),
		workspaceIdentity: Schema.optionalKey(NativeWorkspaceIdentityData),
		mode: Schema.Literal("codex"),
		port: Schema.Int,
		nativePort: Schema.Int
	}),
	threadParams: Schema.Record(Schema.String, Schema.Unknown),
	turnParams: Schema.Record(Schema.String, Schema.Unknown),
	resumeToken: Schema.NullOr(Schema.String),
	deadlineAt: Schema.Number,
	threadId: Schema.NullOr(Schema.String),
	turnId: Schema.NullOr(Schema.String),
	previousTurnId: Schema.NullOr(Schema.String),
	startDispatched: Schema.Boolean,
	result: Schema.NullOr(PersistedTurnResult)
});
/** Workspace and publication checkpoint for one accepted host turn. */
const StoredPipelineTurn = Schema.Struct({
	id: Schema.String,
	instanceId: Schema.optionalKey(Schema.String),
	materializationId: Schema.String,
	head: Schema.NullOr(Schema.String),
	baseHead: Schema.NullOr(Schema.String),
	resumeToken: Schema.NullOr(Schema.String),
	harnessVersion: Schema.NullOr(Schema.String),
	startedAtMillis: Schema.Number,
	persisted: Schema.NullOr(Schema.Struct({
		head: Schema.NullOr(Schema.String),
		changedPaths: Schema.Array(Schema.String)
	}))
});
/** Private fork intent, retained before the provider mutation and never inferred from main. */
const StoredRepositoryFork = Schema.Struct({
	forkName: Schema.String,
	source: Schema.Struct({
		repositoryName: Schema.String,
		repositoryId: Schema.String,
		commitSha: Schema.String,
		artifactsRef: Schema.String
	}),
	originalMainHead: Schema.NullOr(Schema.String),
	description: Schema.String,
	requestedAtMillis: Schema.Finite,
	deadlineAtMillis: Schema.Finite,
	repositoryId: Schema.NullOr(Schema.String),
	initialized: Schema.Boolean
});
/** Private native preparation authority, including any short-lived Git capability. */
const StoredWorkspacePreparation = Schema.Struct({
	operation: Schema.Struct({
		id: Schema.String,
		token: Schema.String,
		port: Schema.Int,
		deadlineAt: Schema.Finite,
		command: Schema.NonEmptyArray(Schema.String),
		cwd: Schema.String,
		env: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
		workspaceIdentity: Schema.optionalKey(NativeWorkspaceIdentityData),
		mode: Schema.Literal("command"),
		maxOutputBytes: Schema.Int
	}),
	repositoryToken: Schema.optionalKey(Schema.Struct({
		repositoryName: Schema.String,
		repositoryId: Schema.optionalKey(Schema.String),
		tokenId: Schema.String,
		authorization: RepositoryAuthorizationGrant
	})),
	result: Schema.NullOr(Schema.Struct({
		state: Schema.Literal("completed"),
		stdout: Schema.String,
		stderr: Schema.String,
		exitCode: Schema.NullOr(Schema.Int),
		timedOut: Schema.Boolean,
		truncated: Schema.Boolean
	}))
});
const PreparationListener = Schema.Struct({
	id: Schema.String,
	token: Schema.String,
	port: Schema.Int,
	deadlineAt: Schema.Finite
});
const SNAPSHOT_KEY = "session-state";
const RUN_RECEIPT_KEY_PREFIX = "run-receipt:";
var SessionStateSnapshotCorruptedError = class extends Schema.TaggedError()("SessionStateSnapshotCorruptedError", {
	key: Schema.String,
	cause: Schema.Defect()
}) {};
var SessionRunReceiptCorruptedError = class extends Schema.TaggedError()("SessionRunReceiptCorruptedError", {
	key: Schema.String,
	cause: Schema.Defect()
}) {};
var SessionRunReceiptMissingError = class extends Schema.TaggedError()("SessionRunReceiptMissingError", { idempotencyKey: Schema.String }) {};
var RunReceiptCapacityExceededError = class extends Schema.TaggedError()("RunReceiptCapacityExceededError", { maximum: Schema.Int }) {};
var RunReceiptAlreadyAcceptedError = class extends Schema.TaggedError()("RunReceiptAlreadyAcceptedError", { idempotencyKey: Schema.String }) {};
var SessionStateStore = class SessionStateStore extends Context.Service()("sandbox-agent/SessionStateStore") {
	static layer = Layer.effect(SessionStateStore)(Effect.gen(function* () {
		const state = yield* DurableObjectState.DurableObjectState;
		const receiptKeyFor = (idempotencyKey) => `${RUN_RECEIPT_KEY_PREFIX}${encodeURIComponent(idempotencyKey)}`;
		const decodeStoredRunReceipt = Schema.decodeUnknownEffect(StoredRunReceipt);
		const readRunReceipt = (idempotencyKey) => {
			const key = receiptKeyFor(idempotencyKey);
			return state.storage.kv.get(key).pipe(Effect.flatMap((stored) => stored === void 0 ? Effect.succeed(null) : decodeStoredRunReceipt(stored).pipe(Effect.mapError((cause) => SessionRunReceiptCorruptedError.make({
				key,
				cause
			})))));
		};
		const emptySnapshot = {
			forkName: null,
			verifiedHead: null,
			materializationId: null,
			resumeToken: null,
			harnessVersion: null,
			containerSize: null,
			lastTimings: null,
			recentRunReceipts: [],
			managedWorkspaceServices: []
		};
		const decodeSnapshot = Schema.decodeUnknownEffect(SessionStateSnapshot);
		const snapshot = state.storage.kv.get(SNAPSHOT_KEY).pipe(Effect.flatMap((stored) => stored === void 0 ? Effect.succeed(emptySnapshot) : decodeSnapshot(stored).pipe(Effect.mapError((cause) => SessionStateSnapshotCorruptedError.make({
			key: SNAPSHOT_KEY,
			cause
		})))));
		const modify = (update) => Effect.gen(function* () {
			const current = yield* snapshot;
			const [value, next] = update(current);
			if (next !== current) yield* state.storage.kv.put(SNAPSHOT_KEY, next);
			return value;
		});
		const nullableKey = (read, write) => ({
			get: snapshot.pipe(Effect.map(read)),
			put: (value) => modify((current) => [void 0, Object.is(read(current), value) ? current : write(current, value)]),
			delete: modify((current) => [read(current) !== null, read(current) === null ? current : write(current, null)])
		});
		const recoveryKey = (key, schema) => ({
			get: state.storage.kv.get(key).pipe(Effect.flatMap((value) => value === void 0 ? Effect.succeed(null) : Schema.decodeUnknownEffect(schema)(value).pipe(Effect.catchCause((cause) => Effect.failCause(Cause.map(cause, (cause) => SessionStateSnapshotCorruptedError.make({
				key,
				cause
			}))))))),
			put: (value) => state.storage.kv.put(key, value),
			delete: state.storage.kv.delete(key)
		});
		const isSettledReceipt = (receipt) => receipt.state === "terminal" && receipt.archiveState !== "pending";
		const sameReceipt = (left, right) => left.idempotencyKey === right.idempotencyKey && left.runId === right.runId && left.archiveKey === right.archiveKey && left.state === right.state && left.archiveState === right.archiveState;
		const findRunReceipt = Effect.fn("SessionStateStore.findRunReceipt")(function* (idempotencyKey) {
			const current = yield* snapshot;
			const authoritative = yield* readRunReceipt(idempotencyKey);
			if (authoritative === null && current.recentRunReceipts.some((receipt) => receipt.idempotencyKey === idempotencyKey)) return yield* SessionRunReceiptMissingError.make({ idempotencyKey });
			return authoritative;
		});
		const acceptRunReceipt = Effect.fn("SessionStateStore.acceptRunReceipt")(function* (receipt) {
			const current = yield* snapshot;
			yield* state.storage.transactionSync(() => Effect.gen(function* () {
				if ((yield* readRunReceipt(receipt.idempotencyKey)) !== null || current.recentRunReceipts.some((indexed) => indexed.idempotencyKey === receipt.idempotencyKey)) return yield* RunReceiptAlreadyAcceptedError.make({ idempotencyKey: receipt.idempotencyKey });
				let recentRunReceipts = current.recentRunReceipts;
				if (recentRunReceipts.length >= 32) {
					const evictableIndex = recentRunReceipts.findIndex(isSettledReceipt);
					if (evictableIndex < 0) return yield* RunReceiptCapacityExceededError.make({ maximum: 32 });
					recentRunReceipts = recentRunReceipts.filter((_, index) => index !== evictableIndex);
				}
				yield* state.storage.kv.put(receiptKeyFor(receipt.idempotencyKey), receipt);
				yield* state.storage.kv.put(SNAPSHOT_KEY, {
					...current,
					recentRunReceipts: [...recentRunReceipts, receipt]
				});
			}));
		});
		const updateRunReceipt = Effect.fn("SessionStateStore.updateRunReceipt")(function* (runId, update) {
			const current = yield* snapshot;
			const indexed = current.recentRunReceipts.find((receipt) => receipt.runId === runId);
			if (indexed === void 0) return false;
			return yield* state.storage.transactionSync(() => Effect.gen(function* () {
				const authoritative = yield* readRunReceipt(indexed.idempotencyKey);
				if (authoritative === null || authoritative.runId !== runId) return yield* SessionRunReceiptMissingError.make({ idempotencyKey: indexed.idempotencyKey });
				const next = update(authoritative);
				if (sameReceipt(authoritative, next)) return true;
				const recentRunReceipts = current.recentRunReceipts.map((receipt) => receipt.runId === runId ? next : receipt);
				yield* state.storage.kv.put(receiptKeyFor(next.idempotencyKey), next);
				yield* state.storage.kv.put(SNAPSHOT_KEY, {
					...current,
					recentRunReceipts
				});
				return true;
			}));
		});
		const reconcileStaleRunReceipts = Effect.fn("SessionStateStore.reconcileStaleRunReceipts")(function* (activeArchiveRunIds) {
			const current = yield* snapshot;
			const reconcile = (receipt) => ({
				...receipt,
				state: "terminal",
				archiveState: receipt.archiveState === "pending" && !activeArchiveRunIds.includes(receipt.runId) ? "unknown" : receipt.archiveState
			});
			const indexedUpdates = current.recentRunReceipts.filter((receipt) => sameReceipt(receipt, reconcile(receipt)) ? false : true);
			if (indexedUpdates.length === 0) return 0;
			return yield* state.storage.transactionSync(() => Effect.gen(function* () {
				const updates = /* @__PURE__ */ new Map();
				for (const indexed of indexedUpdates) {
					const authoritative = yield* readRunReceipt(indexed.idempotencyKey);
					if (authoritative === null || authoritative.runId !== indexed.runId) return yield* SessionRunReceiptMissingError.make({ idempotencyKey: indexed.idempotencyKey });
					const next = reconcile(authoritative);
					if (!sameReceipt(authoritative, next)) {
						updates.set(indexed.runId, next);
						yield* state.storage.kv.put(receiptKeyFor(next.idempotencyKey), next);
					}
				}
				if (updates.size === 0) return 0;
				const recentRunReceipts = current.recentRunReceipts.map((receipt) => updates.get(receipt.runId) ?? receipt);
				yield* state.storage.kv.put(SNAPSHOT_KEY, {
					...current,
					recentRunReceipts
				});
				return updates.size;
			}));
		});
		const lastRunReceipt = {
			get: snapshot.pipe(Effect.map((current) => current.recentRunReceipts.at(-1) ?? null)),
			put: acceptRunReceipt,
			delete: modify((current) => {
				const settledIndex = current.recentRunReceipts.findIndex(isSettledReceipt);
				return [settledIndex >= 0, settledIndex < 0 ? current : {
					...current,
					recentRunReceipts: current.recentRunReceipts.filter((_, index) => index !== settledIndex)
				}];
			})
		};
		const managedWorkspaceServices = {
			get: snapshot.pipe(Effect.map((current) => current.managedWorkspaceServices)),
			put: (services) => modify((current) => {
				return [void 0, services.length === current.managedWorkspaceServices.length && services.every((service, index) => {
					const existing = current.managedWorkspaceServices[index];
					return existing !== void 0 && existing.processId === service.processId && existing.service.name === service.service.name && existing.service.argv.length === service.service.argv.length && existing.service.argv.every((token, tokenIndex) => token === service.service.argv[tokenIndex]);
				}) ? current : {
					...current,
					managedWorkspaceServices: [...services]
				}];
			}),
			delete: modify((current) => [current.managedWorkspaceServices.length > 0, current.managedWorkspaceServices.length === 0 ? current : {
				...current,
				managedWorkspaceServices: []
			}])
		};
		return SessionStateStore.of({
			snapshot,
			codexTurn: recoveryKey("codex-turn", StoredCodexTurn),
			preparationListener: recoveryKey("preparation-listener", PreparationListener),
			preparationListenerFor: (scope) => recoveryKey(`preparation-listener:${scope}`, PreparationListener),
			pipelineTurn: recoveryKey("pipeline-turn", StoredPipelineTurn),
			repositoryFork: recoveryKey("repository-fork", StoredRepositoryFork),
			containerInstanceId: recoveryKey("container-instance", Schema.String),
			workspaceCheckpoint: recoveryKey("workspace-checkpoint", WorkspaceCheckpoint),
			workspaceBaselinePreparation: recoveryKey("workspace-baseline-preparation", StoredWorkspaceBaselinePreparation),
			workspacePreparation: (scope) => recoveryKey(`workspace-preparation:${scope}`, StoredWorkspacePreparation),
			repositorySetupReceipt: {
				get: state.storage.kv.get("repository-setup").pipe(Effect.flatMap((value) => value === void 0 ? Effect.succeed(null) : Schema.decodeUnknownEffect(RepositorySetupReceipt)(value).pipe(Effect.mapError((cause) => SessionStateSnapshotCorruptedError.make({
					key: "repository-setup",
					cause
				}))))),
				put: (receipt) => state.storage.kv.put("repository-setup", receipt),
				delete: state.storage.kv.delete("repository-setup")
			},
			repositorySetupValidationReport: {
				get: state.storage.kv.get("repository-setup-validation").pipe(Effect.flatMap((value) => value === void 0 ? Effect.succeed(null) : Schema.decodeUnknownEffect(RepositorySetupReport)(value).pipe(Effect.mapError((cause) => SessionStateSnapshotCorruptedError.make({
					key: "repository-setup-validation",
					cause
				}))))),
				put: (report) => state.storage.kv.put("repository-setup-validation", report),
				delete: state.storage.kv.delete("repository-setup-validation")
			},
			forkName: nullableKey((current) => current.forkName, (current, forkName) => ({
				...current,
				forkName
			})),
			verifiedHead: nullableKey((current) => current.verifiedHead, (current, verifiedHead) => ({
				...current,
				verifiedHead
			})),
			materializationId: nullableKey((current) => current.materializationId, (current, materializationId) => ({
				...current,
				materializationId
			})),
			resumeToken: nullableKey((current) => current.resumeToken, (current, resumeToken) => ({
				...current,
				resumeToken
			})),
			harnessVersion: nullableKey((current) => current.harnessVersion, (current, harnessVersion) => ({
				...current,
				harnessVersion
			})),
			containerSize: nullableKey((current) => current.containerSize, (current, containerSize) => ({
				...current,
				containerSize
			})),
			lastTimings: nullableKey((current) => current.lastTimings, (current, lastTimings) => ({
				...current,
				lastTimings
			})),
			lastRunReceipt,
			findRunReceipt,
			acceptRunReceipt,
			updateRunReceipt,
			reconcileStaleRunReceipts,
			managedWorkspaceServices,
			clear: state.storage.deleteAll()
		});
	}));
};
//#endregion
export { MAX_RECENT_RUN_RECEIPTS, ManagedWorkspaceService, RunReceiptAlreadyAcceptedError, RunReceiptCapacityExceededError, SessionRunReceiptCorruptedError, SessionRunReceiptMissingError, SessionStateSnapshot, SessionStateSnapshotCorruptedError, SessionStateStore, StoredCodexTurn, StoredPipelineTurn, StoredRepositoryFork, StoredRunReceipt, StoredWorkspacePreparation };
