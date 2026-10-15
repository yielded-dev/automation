import { Cause, Clock, Context, Data, Effect, Layer, Schema } from "effect";
import { SqlClient } from "effect/sql/SqlClient";

import { RepositorySetupReport } from "../model/RepositorySetup";

import { WorkspaceCheckpoint } from "./NativeSandbox";
import {
  WORKSPACE_BASELINE_REFRESH_HEADROOM,
  WORKSPACE_SNAPSHOT_HEADROOM,
} from "./WorkspaceSnapshotPolicy";

export {
  WORKSPACE_SNAPSHOT_HEADROOM,
  WORKSPACE_BASELINE_REFRESH_HEADROOM,
} from "./WorkspaceSnapshotPolicy";
export const WORKSPACE_PREPARATION_VERSION = "native-container-1";

export const WorkspaceBaseline = Schema.Struct({
  repository: Schema.String,
  checkpoint: WorkspaceCheckpoint,
  report: RepositorySetupReport,
});
export type WorkspaceBaseline = typeof WorkspaceBaseline.Type;
export interface WorkspaceBaselineSelection {
  readonly repository: string;
  readonly classIdentity: string;
  readonly imageDigest: string;
  readonly head: string;
  /** Available after inspecting the pinned commit's installation inputs. */
  readonly setupFingerprint?: string;
  /** Preparation also needs expired predecessors for compare-and-swap publication. */
  readonly includeExpired?: boolean;
}
export class WorkspaceBaselineError extends Data.TaggedError("WorkspaceBaselineError")<{
  readonly cause: unknown;
}> {}

/** Immutable prepared filesystem cache. Source and execution receipts have other owners. */
export class WorkspaceBaselineRepository extends Context.Service<
  WorkspaceBaselineRepository,
  {
    readonly find: (
      key: WorkspaceBaselineSelection,
    ) => Effect.Effect<WorkspaceBaseline | null, WorkspaceBaselineError>;
    readonly publish: (
      baseline: WorkspaceBaseline,
      /** Replace only this predecessor; null requires an absent key. Omit to replace expired entries. */
      previousSnapshotId?: string | null,
    ) => Effect.Effect<boolean, WorkspaceBaselineError>;
    readonly invalidate: (
      key: WorkspaceBaselineSelection,
      snapshotId: string,
    ) => Effect.Effect<void, WorkspaceBaselineError>;
    readonly due: (owner: {
      readonly classIdentity: string;
      readonly imageDigest: string;
    }) => Effect.Effect<ReadonlyArray<WorkspaceBaseline>, WorkspaceBaselineError>;
  }
>()("sandbox-agent/WorkspaceBaselineRepository") {
  /** Fixture hosts can omit a shared cache while retaining per-session recovery. */
  static readonly noopLayer = Layer.succeed(WorkspaceBaselineRepository, {
    find: () => Effect.succeed(null),
    publish: () => Effect.succeed(false),
    invalidate: () => Effect.void,
    due: () => Effect.succeed([]),
  });

  /** Captures the host's PG layer, acquiring a connection only around cache operations. */
  static readonly postgresLayer = <E, R>(clientLayer: Layer.Layer<SqlClient, E, R>) =>
    Layer.effect(
      WorkspaceBaselineRepository,
      Effect.gen(function* () {
        const context = yield* Effect.context<R>();
        const withClient = <A, Failure>(operation: Effect.Effect<A, Failure, SqlClient>) =>
          operation.pipe(
            Effect.provide(clientLayer),
            Effect.provide(context),
            Effect.catchCause((cause) =>
              Effect.failCause(Cause.map(cause, (cause) => new WorkspaceBaselineError({ cause }))),
            ),
          );
        const decodeRows = (rows: ReadonlyArray<{ readonly payload: unknown }>) =>
          Effect.forEach(rows, ({ payload }) =>
            // oxlint-disable-next-line no-restricted-properties -- Postgres JSON is an untyped persistence boundary.
            Schema.decodeUnknownEffect(WorkspaceBaseline)(payload),
          );
        const find = Effect.fn("WorkspaceBaselineRepository.find")(function* (
          key: WorkspaceBaselineSelection,
        ) {
          const sql = yield* SqlClient;
          const now = yield* Clock.currentTimeMillis;
          const rows = yield* sql<{ payload: unknown }>`
            SELECT payload FROM sandbox_workspace_baselines
            WHERE class_identity=${key.classIdentity} AND repository=${key.repository}
              AND image_digest=${key.imageDigest}
              AND ${key.includeExpired === true ? sql`true` : sql`expires_at > to_timestamp(${(now + WORKSPACE_SNAPSHOT_HEADROOM) / 1000})`}
              AND ${key.setupFingerprint === undefined ? sql`true` : sql`setup_fingerprint=${key.setupFingerprint}`}
            ORDER BY (head=${key.head}) DESC, created_at DESC LIMIT 1`;

          const baseline = (yield* decodeRows(rows))[0] ?? null;

          yield* Effect.annotateCurrentSpan(
            "workspace.checkpoint.lookup",
            baseline === null ? "miss" : "hit",
          );

          return baseline;
        });
        const publish = Effect.fn("WorkspaceBaselineRepository.publish")(function* (
          baseline: WorkspaceBaseline,
          previousSnapshotId?: string | null,
        ) {
          const sql = yield* SqlClient;
          const checkpoint = baseline.checkpoint;
          const now = yield* Clock.currentTimeMillis;
          const payload = yield* Schema.encodeEffect(Schema.fromJsonString(WorkspaceBaseline))(
            baseline,
          );
          const rows =
            previousSnapshotId == null
              ? yield* sql<{ snapshot_id: string }>`
                INSERT INTO sandbox_workspace_baselines
                  (class_identity,repository,setup_fingerprint,image_digest,head,snapshot_id,payload,expires_at,created_at)
                VALUES (${checkpoint.classIdentity},${baseline.repository},${checkpoint.setupFingerprint},
                  ${checkpoint.imageDigest},${checkpoint.head},${checkpoint.snapshotId},${payload}::jsonb,
                  to_timestamp(${checkpoint.expiresAt / 1000}),to_timestamp(${checkpoint.capturedAt / 1000}))
                ON CONFLICT (class_identity,repository,setup_fingerprint,image_digest) DO UPDATE SET
                  head=excluded.head,snapshot_id=excluded.snapshot_id,payload=excluded.payload,
                  expires_at=excluded.expires_at,created_at=excluded.created_at
                WHERE ${previousSnapshotId === null ? sql`false` : sql`sandbox_workspace_baselines.expires_at <= to_timestamp(${(now + WORKSPACE_SNAPSHOT_HEADROOM) / 1000})`}
                RETURNING snapshot_id`
              : yield* sql<{ snapshot_id: string }>`
                UPDATE sandbox_workspace_baselines SET head=${checkpoint.head},snapshot_id=${checkpoint.snapshotId},payload=${payload}::jsonb,
                  expires_at=to_timestamp(${checkpoint.expiresAt / 1000}),created_at=to_timestamp(${checkpoint.capturedAt / 1000})
                WHERE class_identity=${checkpoint.classIdentity} AND repository=${baseline.repository}
                  AND setup_fingerprint=${checkpoint.setupFingerprint} AND image_digest=${checkpoint.imageDigest}
                  AND snapshot_id=${previousSnapshotId}
                RETURNING snapshot_id`;

          return rows.length === 1;
        });
        const invalidate = Effect.fn("WorkspaceBaselineRepository.invalidate")(function* (
          key: WorkspaceBaselineSelection,
          snapshotId: string,
        ) {
          const sql = yield* SqlClient;

          yield* sql`DELETE FROM sandbox_workspace_baselines
            WHERE class_identity=${key.classIdentity} AND repository=${key.repository}
              AND image_digest=${key.imageDigest} AND head=${key.head} AND snapshot_id=${snapshotId}`;
        });
        const due = Effect.fnUntraced(function* (owner: {
          readonly classIdentity: string;
          readonly imageDigest: string;
        }) {
          const sql = yield* SqlClient;
          const now = yield* Clock.currentTimeMillis;
          const rows = yield* sql<{
            payload: unknown;
          }>`SELECT payload FROM sandbox_workspace_baselines
            WHERE class_identity=${owner.classIdentity} AND image_digest=${owner.imageDigest}
              AND expires_at > to_timestamp(${(now + WORKSPACE_SNAPSHOT_HEADROOM) / 1000})
              AND expires_at < to_timestamp(${(now + WORKSPACE_BASELINE_REFRESH_HEADROOM) / 1000})
            ORDER BY expires_at LIMIT 5`;

          return yield* decodeRows(rows);
        });

        return WorkspaceBaselineRepository.of({
          find: (key) => withClient(find(key)),
          publish: (baseline, previous) => withClient(publish(baseline, previous)),
          invalidate: (key, snapshotId) => withClient(invalidate(key, snapshotId)),
          due: (owner) => withClient(due(owner)),
        });
      }),
    );
}
