import { Cause, Context, Effect, Layer, Schema } from "effect";
import { DurableObjectState, type DurableObjectStorage } from "effect-cf";

import { RepositorySetupReceipt, RepositorySetupReport } from "../model/RepositorySetup";

import {
  AgentTimings,
  ContainerSize,
  RunArchiveState,
  RunReceiptState,
  WorkspaceService,
} from "../Contract";
import { RepositoryAuthorizationGrant, WorkspaceCheckpoint } from "./NativeSandbox";
import { RepositoryCheckpointInput } from "./RepositoryCheckpoint";
import { NativeWorkspaceIdentityData } from "./NativeWorkspaceIdentity";
import { StoredWorkspaceBaselinePreparation } from "./WorkspaceBaselinePreparationState";

export const MAX_RECENT_RUN_RECEIPTS = 32;

/** Durable caller receipt retained for the lifetime of its session. */
export const StoredRunReceipt = Schema.Struct({
  idempotencyKey: Schema.String,
  runId: Schema.String,
  archiveKey: Schema.String,
  state: RunReceiptState,
  archiveState: RunArchiveState,
});
export type StoredRunReceipt = typeof StoredRunReceipt.Type;

/** A process the kernel started or adopted for a declared workspace service. */
export const ManagedWorkspaceService = Schema.Struct({
  service: WorkspaceService,
  processId: Schema.String,
});
export type ManagedWorkspaceService = typeof ManagedWorkspaceService.Type;

const StoredRunReceipts = Schema.Array(StoredRunReceipt).pipe(
  Schema.check(
    Schema.makeFilter((receipts) => receipts.length <= MAX_RECENT_RUN_RECEIPTS, {
      expected: `at most ${MAX_RECENT_RUN_RECEIPTS} stored run receipts`,
    }),
  ),
);

export const SessionStateSnapshot = Schema.Struct({
  forkName: Schema.NullOr(Schema.String),
  verifiedHead: Schema.NullOr(Schema.String),
  materializationId: Schema.NullOr(Schema.String),
  resumeToken: Schema.NullOr(Schema.String),
  harnessVersion: Schema.NullOr(Schema.String),
  containerSize: Schema.NullOr(ContainerSize),
  lastTimings: Schema.NullOr(AgentTimings),
  recentRunReceipts: StoredRunReceipts,
  managedWorkspaceServices: Schema.Array(ManagedWorkspaceService),
});
export type SessionStateSnapshot = typeof SessionStateSnapshot.Type;

const PersistedTurnResult = Schema.Struct({
  resumeToken: Schema.String,
  terminalEventType: Schema.String,
  lastAgentMessage: Schema.NullOr(Schema.String),
  usage: Schema.Unknown,
  stderrTail: Schema.String,
});

/** Private recovery capability. Never include this record in runtime inspection or archives. */
export const StoredCodexTurn = Schema.Struct({
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
    nativePort: Schema.Int,
  }),
  threadParams: Schema.Record(Schema.String, Schema.Unknown),
  turnParams: Schema.Record(Schema.String, Schema.Unknown),
  resumeToken: Schema.NullOr(Schema.String),
  deadlineAt: Schema.Finite,
  threadId: Schema.NullOr(Schema.String),
  turnId: Schema.NullOr(Schema.String),
  previousTurnId: Schema.NullOr(Schema.String),
  startDispatched: Schema.Boolean,
  result: Schema.NullOr(PersistedTurnResult),
});
export type StoredCodexTurn = typeof StoredCodexTurn.Type;

/** Workspace and publication checkpoint for one accepted host turn. */
export const StoredPipelineTurn = Schema.Struct({
  id: Schema.String,
  instanceId: Schema.optionalKey(Schema.String),
  materializationId: Schema.String,
  head: Schema.NullOr(Schema.String),
  baseHead: Schema.NullOr(Schema.String),
  resumeToken: Schema.NullOr(Schema.String),
  harnessVersion: Schema.NullOr(Schema.String),
  startedAtMillis: Schema.Finite,
  persisted: Schema.NullOr(
    Schema.Struct({
      head: Schema.NullOr(Schema.String),
      changedPaths: Schema.Array(Schema.String),
    }),
  ),
});
export type StoredPipelineTurn = typeof StoredPipelineTurn.Type;

/** Private fork intent, retained before the provider mutation and never inferred from main. */
export const StoredRepositoryFork = Schema.Struct({
  forkName: Schema.String,
  source: Schema.Struct({
    repositoryName: Schema.String,
    repositoryId: Schema.String,
    commitSha: Schema.String,
    artifactsRef: Schema.String,
  }),
  originalMainHead: Schema.NullOr(Schema.String),
  description: Schema.String,
  requestedAtMillis: Schema.Finite,
  deadlineAtMillis: Schema.Finite,
  repositoryId: Schema.NullOr(Schema.String),
  initialized: Schema.Boolean,
});
export type StoredRepositoryFork = typeof StoredRepositoryFork.Type;

/** Private native preparation authority, including any short-lived Git capability. */
export const StoredWorkspacePreparation = Schema.Struct({
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
    maxOutputBytes: Schema.Int,
  }),
  repositoryToken: Schema.optionalKey(
    Schema.Struct({
      repositoryName: Schema.String,
      repositoryId: Schema.optionalKey(Schema.String),
      tokenId: Schema.String,
      authorization: RepositoryAuthorizationGrant,
    }),
  ),
  result: Schema.NullOr(
    Schema.Struct({
      state: Schema.Literal("completed"),
      stdout: Schema.String,
      stderr: Schema.String,
      exitCode: Schema.NullOr(Schema.Int),
      timedOut: Schema.Boolean,
      truncated: Schema.Boolean,
    }),
  ),
});
export type StoredWorkspacePreparation = typeof StoredWorkspacePreparation.Type;
const PreparationListener = Schema.Struct({
  id: Schema.String,
  token: Schema.String,
  port: Schema.Int,
  deadlineAt: Schema.Finite,
});

const SNAPSHOT_KEY = "session-state";
const RUN_RECEIPT_KEY_PREFIX = "run-receipt:";

export class SessionStateSnapshotCorruptedError extends Schema.TaggedError<SessionStateSnapshotCorruptedError>()(
  "SessionStateSnapshotCorruptedError",
  { key: Schema.String, cause: Schema.Defect() },
) {}

export class SessionRunReceiptCorruptedError extends Schema.TaggedError<SessionRunReceiptCorruptedError>()(
  "SessionRunReceiptCorruptedError",
  { key: Schema.String, cause: Schema.Defect() },
) {}

export class SessionRunReceiptMissingError extends Schema.TaggedError<SessionRunReceiptMissingError>()(
  "SessionRunReceiptMissingError",
  { idempotencyKey: Schema.String },
) {}

export class RunReceiptCapacityExceededError extends Schema.TaggedError<RunReceiptCapacityExceededError>()(
  "RunReceiptCapacityExceededError",
  { maximum: Schema.Int },
) {}

export class RunReceiptAlreadyAcceptedError extends Schema.TaggedError<RunReceiptAlreadyAcceptedError>()(
  "RunReceiptAlreadyAcceptedError",
  { idempotencyKey: Schema.String },
) {}

export type SessionStateStoreError =
  | DurableObjectStorage.StorageOperationError
  | SessionStateSnapshotCorruptedError
  | SessionRunReceiptCorruptedError
  | SessionRunReceiptMissingError;

export type AcceptRunReceiptError =
  | SessionStateStoreError
  | RunReceiptCapacityExceededError
  | RunReceiptAlreadyAcceptedError;

/**
 * Schema-checked accessors over the coordinator's Durable Object storage.
 * Every durable session key is declared exactly once here, so key names and
 * stored shapes have a single owner instead of ad-hoc string keys and
 * unchecked `get<T>` casts at call sites.
 */

/** One durable session key: schema-checked read, typed write, delete. */
export interface StoredKey<A, PutError = SessionStateStoreError> {
  /** `null` only when the key is absent. An undecodable snapshot fails closed
   * so a later write can never overwrite corrupted durable recovery state. */
  readonly get: Effect.Effect<A | null, SessionStateStoreError>;
  readonly put: (value: A) => Effect.Effect<void, PutError>;
  readonly delete: Effect.Effect<boolean, SessionStateStoreError>;
}

export class SessionStateStore extends Context.Service<
  SessionStateStore,
  {
    readonly snapshot: Effect.Effect<SessionStateSnapshot, SessionStateStoreError>;
    readonly codexTurn: StoredKey<StoredCodexTurn>;
    readonly pipelineTurn: StoredKey<StoredPipelineTurn>;
    readonly repositoryFork: StoredKey<StoredRepositoryFork>;
    readonly workspacePreparation: (scope: string) => StoredKey<StoredWorkspacePreparation>;
    readonly preparationListener: StoredKey<typeof PreparationListener.Type>;
    readonly preparationListenerFor: (scope: string) => StoredKey<typeof PreparationListener.Type>;
    readonly forkName: StoredKey<string>;
    readonly verifiedHead: StoredKey<string>;
    readonly materializationId: StoredKey<string>;
    readonly containerInstanceId: StoredKey<string>;
    readonly workspaceCheckpoint: StoredKey<WorkspaceCheckpoint>;
    readonly workspaceCheckpointInput: StoredKey<RepositoryCheckpointInput>;
    readonly workspaceBaselinePreparation: StoredKey<StoredWorkspaceBaselinePreparation>;
    readonly repositorySetupReceipt: StoredKey<RepositorySetupReceipt>;
    readonly repositorySetupValidationReport: StoredKey<RepositorySetupReport>;
    readonly resumeToken: StoredKey<string>;
    readonly harnessVersion: StoredKey<string>;
    readonly containerSize: StoredKey<ContainerSize>;
    readonly lastTimings: StoredKey<AgentTimings>;
    readonly lastRunReceipt: StoredKey<StoredRunReceipt, AcceptRunReceiptError>;
    readonly findRunReceipt: (
      idempotencyKey: string,
    ) => Effect.Effect<StoredRunReceipt | null, SessionStateStoreError>;
    readonly acceptRunReceipt: (
      receipt: StoredRunReceipt,
    ) => Effect.Effect<void, AcceptRunReceiptError>;
    readonly updateRunReceipt: (
      runId: string,
      update: (receipt: StoredRunReceipt) => StoredRunReceipt,
    ) => Effect.Effect<boolean, SessionStateStoreError>;
    readonly reconcileStaleRunReceipts: (
      activeArchiveRunIds: ReadonlyArray<string>,
    ) => Effect.Effect<number, SessionStateStoreError>;
    readonly managedWorkspaceServices: StoredKey<ReadonlyArray<ManagedWorkspaceService>>;
    /** Deletes every durable key for this session. */
    readonly clear: Effect.Effect<void, DurableObjectStorage.StorageOperationError>;
  }
>()("sandbox-agent/SessionStateStore") {
  static readonly layer = Layer.effect(SessionStateStore)(
    Effect.gen(function* () {
      const state = yield* DurableObjectState.DurableObjectState;

      const receiptKeyFor = (idempotencyKey: string) =>
        `${RUN_RECEIPT_KEY_PREFIX}${encodeURIComponent(idempotencyKey)}`;
      // oxlint-disable-next-line no-restricted-properties -- sync KV receipt values are an untyped boundary.
      const decodeStoredRunReceipt = Schema.decodeUnknownEffect(StoredRunReceipt);
      const readRunReceipt = (idempotencyKey: string) => {
        const key = receiptKeyFor(idempotencyKey);

        return state.storage.kv
          .get(key)
          .pipe(
            Effect.flatMap((stored) =>
              stored === undefined
                ? Effect.succeed(null)
                : decodeStoredRunReceipt(stored).pipe(
                    Effect.mapError((cause) =>
                      SessionRunReceiptCorruptedError.make({ key, cause }),
                    ),
                  ),
            ),
          );
      };
      const emptySnapshot: SessionStateSnapshot = {
        forkName: null,
        verifiedHead: null,
        materializationId: null,
        resumeToken: null,
        harnessVersion: null,
        containerSize: null,
        lastTimings: null,
        recentRunReceipts: [],
        managedWorkspaceServices: [],
      };
      // oxlint-disable-next-line no-restricted-properties -- sync KV is an untyped boundary.
      const decodeSnapshot = Schema.decodeUnknownEffect(SessionStateSnapshot);
      const snapshot = state.storage.kv
        .get(SNAPSHOT_KEY)
        .pipe(
          Effect.flatMap((stored) =>
            stored === undefined
              ? Effect.succeed(emptySnapshot)
              : decodeSnapshot(stored).pipe(
                  Effect.mapError((cause) =>
                    SessionStateSnapshotCorruptedError.make({ key: SNAPSHOT_KEY, cause }),
                  ),
                ),
          ),
        );
      const modify = <A>(
        update: (current: SessionStateSnapshot) => readonly [A, SessionStateSnapshot],
      ): Effect.Effect<A, SessionStateStoreError> =>
        Effect.gen(function* () {
          const current = yield* snapshot;
          const [value, next] = update(current);

          if (next !== current) yield* state.storage.kv.put(SNAPSHOT_KEY, next);

          return value;
        });
      const nullableKey = <A>(
        read: (current: SessionStateSnapshot) => A | null,
        write: (current: SessionStateSnapshot, value: A | null) => SessionStateSnapshot,
      ): StoredKey<A> => ({
        get: snapshot.pipe(Effect.map(read)),
        put: (value) =>
          modify(
            (current) =>
              [
                undefined,
                Object.is(read(current), value) ? current : write(current, value),
              ] as const,
          ),
        delete: modify(
          (current) =>
            [
              read(current) !== null,
              read(current) === null ? current : write(current, null),
            ] as const,
        ),
      });
      const recoveryKey = <S extends Schema.Top & { readonly DecodingServices: never }>(
        key: string,
        schema: S,
      ): StoredKey<S["Type"]> => ({
        get: state.storage.kv.get(key).pipe(
          Effect.flatMap((value) =>
            value === undefined
              ? Effect.succeed(null)
              : // oxlint-disable-next-line no-restricted-properties -- durable KV is an untyped boundary.
                Schema.decodeEffect(schema)(value).pipe(
                  Effect.catchCause((cause) =>
                    Effect.failCause(
                      Cause.map(cause, (cause) =>
                        SessionStateSnapshotCorruptedError.make({ key, cause }),
                      ),
                    ),
                  ),
                ),
          ),
        ),
        put: (value) => state.storage.kv.put(key, value),
        delete: state.storage.kv.delete(key),
      });
      const isSettledReceipt = (receipt: StoredRunReceipt) =>
        receipt.state === "terminal" && receipt.archiveState !== "pending";
      const sameReceipt = (left: StoredRunReceipt, right: StoredRunReceipt) =>
        left.idempotencyKey === right.idempotencyKey &&
        left.runId === right.runId &&
        left.archiveKey === right.archiveKey &&
        left.state === right.state &&
        left.archiveState === right.archiveState;
      const findRunReceipt = Effect.fn("SessionStateStore.findRunReceipt")(function* (
        idempotencyKey: string,
      ) {
        const current = yield* snapshot;
        const authoritative = yield* readRunReceipt(idempotencyKey);

        if (
          authoritative === null &&
          current.recentRunReceipts.some((receipt) => receipt.idempotencyKey === idempotencyKey)
        ) {
          return yield* SessionRunReceiptMissingError.make({ idempotencyKey });
        }

        return authoritative;
      });
      const acceptRunReceipt = Effect.fn("SessionStateStore.acceptRunReceipt")(function* (
        receipt: StoredRunReceipt,
      ) {
        const current = yield* snapshot;

        yield* state.storage.transactionSync(() =>
          Effect.gen(function* () {
            if (
              (yield* readRunReceipt(receipt.idempotencyKey)) !== null ||
              current.recentRunReceipts.some(
                (indexed) => indexed.idempotencyKey === receipt.idempotencyKey,
              )
            ) {
              return yield* RunReceiptAlreadyAcceptedError.make({
                idempotencyKey: receipt.idempotencyKey,
              });
            }
            let recentRunReceipts = current.recentRunReceipts;

            if (recentRunReceipts.length >= MAX_RECENT_RUN_RECEIPTS) {
              const evictableIndex = recentRunReceipts.findIndex(isSettledReceipt);

              if (evictableIndex < 0) {
                return yield* RunReceiptCapacityExceededError.make({
                  maximum: MAX_RECENT_RUN_RECEIPTS,
                });
              }
              recentRunReceipts = recentRunReceipts.filter((_, index) => index !== evictableIndex);
            }

            yield* state.storage.kv.put(receiptKeyFor(receipt.idempotencyKey), receipt);
            yield* state.storage.kv.put(SNAPSHOT_KEY, {
              ...current,
              recentRunReceipts: [...recentRunReceipts, receipt],
            });
          }),
        );
      });
      const updateRunReceipt = Effect.fn("SessionStateStore.updateRunReceipt")(function* (
        runId: string,
        update: (receipt: StoredRunReceipt) => StoredRunReceipt,
      ) {
        const current = yield* snapshot;
        const indexed = current.recentRunReceipts.find((receipt) => receipt.runId === runId);

        if (indexed === undefined) return false;

        return yield* state.storage.transactionSync(() =>
          Effect.gen(function* () {
            const authoritative = yield* readRunReceipt(indexed.idempotencyKey);

            if (authoritative === null || authoritative.runId !== runId) {
              return yield* SessionRunReceiptMissingError.make({
                idempotencyKey: indexed.idempotencyKey,
              });
            }
            const next = update(authoritative);

            if (sameReceipt(authoritative, next)) return true;
            const recentRunReceipts = current.recentRunReceipts.map((receipt) =>
              receipt.runId === runId ? next : receipt,
            );

            yield* state.storage.kv.put(receiptKeyFor(next.idempotencyKey), next);
            yield* state.storage.kv.put(SNAPSHOT_KEY, { ...current, recentRunReceipts });

            return true;
          }),
        );
      });
      const reconcileStaleRunReceipts = Effect.fn("SessionStateStore.reconcileStaleRunReceipts")(
        function* (activeArchiveRunIds: ReadonlyArray<string>) {
          const current = yield* snapshot;
          const reconcile = (receipt: StoredRunReceipt): StoredRunReceipt => ({
            ...receipt,
            state: "terminal",
            // `pending` is truthful while this isolate still owns the R2
            // attempt. Once ownership is lost, the put may or may not have
            // committed, so only `unknown` is a defensible disposition.
            archiveState:
              receipt.archiveState === "pending" && !activeArchiveRunIds.includes(receipt.runId)
                ? "unknown"
                : receipt.archiveState,
          });
          const indexedUpdates = current.recentRunReceipts.filter((receipt) =>
            sameReceipt(receipt, reconcile(receipt)) ? false : true,
          );

          if (indexedUpdates.length === 0) return 0;

          return yield* state.storage.transactionSync(() =>
            Effect.gen(function* () {
              const updates = new Map<string, StoredRunReceipt>();

              for (const indexed of indexedUpdates) {
                const authoritative = yield* readRunReceipt(indexed.idempotencyKey);

                if (authoritative === null || authoritative.runId !== indexed.runId) {
                  return yield* SessionRunReceiptMissingError.make({
                    idempotencyKey: indexed.idempotencyKey,
                  });
                }
                const next = reconcile(authoritative);

                if (!sameReceipt(authoritative, next)) {
                  updates.set(indexed.runId, next);
                  yield* state.storage.kv.put(receiptKeyFor(next.idempotencyKey), next);
                }
              }
              if (updates.size === 0) return 0;
              const recentRunReceipts = current.recentRunReceipts.map(
                (receipt) => updates.get(receipt.runId) ?? receipt,
              );

              yield* state.storage.kv.put(SNAPSHOT_KEY, { ...current, recentRunReceipts });

              return updates.size;
            }),
          );
        },
      );
      const lastRunReceipt: StoredKey<StoredRunReceipt, AcceptRunReceiptError> = {
        get: snapshot.pipe(Effect.map((current) => current.recentRunReceipts.at(-1) ?? null)),
        put: acceptRunReceipt,
        delete: modify((current) => {
          const settledIndex = current.recentRunReceipts.findIndex(isSettledReceipt);

          return [
            settledIndex >= 0,
            settledIndex < 0
              ? current
              : {
                  ...current,
                  recentRunReceipts: current.recentRunReceipts.filter(
                    (_, index) => index !== settledIndex,
                  ),
                },
          ] as const;
        }),
      };
      const managedWorkspaceServices: StoredKey<ReadonlyArray<ManagedWorkspaceService>> = {
        get: snapshot.pipe(Effect.map((current) => current.managedWorkspaceServices)),
        put: (services) =>
          modify((current) => {
            const unchanged =
              services.length === current.managedWorkspaceServices.length &&
              services.every((service, index) => {
                const existing = current.managedWorkspaceServices[index];

                return (
                  existing !== undefined &&
                  existing.processId === service.processId &&
                  existing.service.name === service.service.name &&
                  existing.service.argv.length === service.service.argv.length &&
                  existing.service.argv.every(
                    (token, tokenIndex) => token === service.service.argv[tokenIndex],
                  )
                );
              });

            return [
              undefined,
              unchanged ? current : { ...current, managedWorkspaceServices: [...services] },
            ] as const;
          }),
        delete: modify(
          (current) =>
            [
              current.managedWorkspaceServices.length > 0,
              current.managedWorkspaceServices.length === 0
                ? current
                : { ...current, managedWorkspaceServices: [] },
            ] as const,
        ),
      };

      return SessionStateStore.of({
        snapshot,
        codexTurn: recoveryKey("codex-turn", StoredCodexTurn),
        preparationListener: recoveryKey("preparation-listener", PreparationListener),
        preparationListenerFor: (scope) =>
          recoveryKey(`preparation-listener:${scope}`, PreparationListener),
        pipelineTurn: recoveryKey("pipeline-turn", StoredPipelineTurn),
        repositoryFork: recoveryKey("repository-fork", StoredRepositoryFork),
        containerInstanceId: recoveryKey("container-instance", Schema.String),
        workspaceCheckpoint: recoveryKey("workspace-checkpoint", WorkspaceCheckpoint),
        workspaceCheckpointInput: recoveryKey(
          "workspace-checkpoint-input",
          RepositoryCheckpointInput,
        ),
        workspaceBaselinePreparation: recoveryKey(
          "workspace-baseline-preparation",
          StoredWorkspaceBaselinePreparation,
        ),
        workspacePreparation: (scope) =>
          recoveryKey(`workspace-preparation:${scope}`, StoredWorkspacePreparation),
        repositorySetupReceipt: {
          get: state.storage.kv.get("repository-setup").pipe(
            Effect.flatMap((value) =>
              value === undefined
                ? Effect.succeed(null)
                : // oxlint-disable-next-line no-restricted-properties -- durable KV is an untyped boundary.
                  Schema.decodeUnknownEffect(RepositorySetupReceipt)(value).pipe(
                    Effect.mapError((cause) =>
                      SessionStateSnapshotCorruptedError.make({ key: "repository-setup", cause }),
                    ),
                  ),
            ),
          ),
          put: (receipt) => state.storage.kv.put("repository-setup", receipt),
          delete: state.storage.kv.delete("repository-setup"),
        },
        repositorySetupValidationReport: {
          get: state.storage.kv.get("repository-setup-validation").pipe(
            Effect.flatMap((value) =>
              value === undefined
                ? Effect.succeed(null)
                : // oxlint-disable-next-line no-restricted-properties -- durable KV is an untyped boundary.
                  Schema.decodeUnknownEffect(RepositorySetupReport)(value).pipe(
                    Effect.mapError((cause) =>
                      SessionStateSnapshotCorruptedError.make({
                        key: "repository-setup-validation",
                        cause,
                      }),
                    ),
                  ),
            ),
          ),
          put: (report) => state.storage.kv.put("repository-setup-validation", report),
          delete: state.storage.kv.delete("repository-setup-validation"),
        },
        forkName: nullableKey(
          (current) => current.forkName,
          (current, forkName) => ({ ...current, forkName }),
        ),
        verifiedHead: nullableKey(
          (current) => current.verifiedHead,
          (current, verifiedHead) => ({ ...current, verifiedHead }),
        ),
        materializationId: nullableKey(
          (current) => current.materializationId,
          (current, materializationId) => ({ ...current, materializationId }),
        ),
        resumeToken: nullableKey(
          (current) => current.resumeToken,
          (current, resumeToken) => ({ ...current, resumeToken }),
        ),
        harnessVersion: nullableKey(
          (current) => current.harnessVersion,
          (current, harnessVersion) => ({ ...current, harnessVersion }),
        ),
        containerSize: nullableKey(
          (current) => current.containerSize,
          (current, containerSize) => ({ ...current, containerSize }),
        ),
        lastTimings: nullableKey(
          (current) => current.lastTimings,
          (current, lastTimings) => ({ ...current, lastTimings }),
        ),
        lastRunReceipt,
        findRunReceipt,
        acceptRunReceipt,
        updateRunReceipt,
        reconcileStaleRunReceipts,
        managedWorkspaceServices,
        clear: state.storage.deleteAll(),
      });
    }),
  );
}
