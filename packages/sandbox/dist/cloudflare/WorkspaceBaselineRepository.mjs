import { RepositorySetupReport } from "../model/RepositorySetup.mjs";
import { WorkspaceCheckpoint } from "./NativeSandbox.mjs";
import { WORKSPACE_BASELINE_REFRESH_HEADROOM, WORKSPACE_SNAPSHOT_HEADROOM } from "./WorkspaceSnapshotPolicy.mjs";
import { Cause, Clock, Context, Data, Effect, Layer, Schema } from "effect";
import { SqlClient } from "effect/sql/SqlClient";
//#region src/cloudflare/WorkspaceBaselineRepository.ts
const WORKSPACE_PREPARATION_VERSION = "native-container-1";
const WorkspaceBaseline = Schema.Struct({
	repository: Schema.String,
	checkpoint: WorkspaceCheckpoint,
	report: RepositorySetupReport
});
var WorkspaceBaselineError = class extends Data.TaggedError("WorkspaceBaselineError") {};
/** Immutable prepared filesystem cache. Source and execution receipts have other owners. */
var WorkspaceBaselineRepository = class WorkspaceBaselineRepository extends Context.Service()("sandbox-agent/WorkspaceBaselineRepository") {
	/** Fixture hosts can omit a shared cache while retaining per-session recovery. */
	static noopLayer = Layer.succeed(WorkspaceBaselineRepository, {
		find: () => Effect.succeed(null),
		publish: () => Effect.succeed(false),
		invalidate: () => Effect.void,
		due: () => Effect.succeed([])
	});
	/** Captures the host's PG layer, acquiring a connection only around cache operations. */
	static postgresLayer = (clientLayer) => Layer.effect(WorkspaceBaselineRepository, Effect.gen(function* () {
		const context = yield* Effect.context();
		const withClient = (operation) => operation.pipe(Effect.provide(clientLayer), Effect.provide(context), Effect.catchCause((cause) => Effect.failCause(Cause.map(cause, (cause) => new WorkspaceBaselineError({ cause })))));
		const decodeRows = (rows) => Effect.forEach(rows, ({ payload }) => Schema.decodeUnknownEffect(WorkspaceBaseline)(payload));
		const find = Effect.fn("WorkspaceBaselineRepository.find")(function* (key) {
			const sql = yield* SqlClient;
			const now = yield* Clock.currentTimeMillis;
			const rows = yield* sql`
            SELECT payload FROM sandbox_workspace_baselines
            WHERE class_identity=${key.classIdentity} AND repository=${key.repository}
              AND image_digest=${key.imageDigest}
              AND ${key.includeExpired === true ? sql`true` : sql`expires_at > to_timestamp(${(now + WORKSPACE_SNAPSHOT_HEADROOM) / 1e3})`}
              AND ${key.setupFingerprint === void 0 ? sql`true` : sql`setup_fingerprint=${key.setupFingerprint}`}
            ORDER BY (head=${key.head}) DESC, created_at DESC LIMIT 1`;
			const baseline = (yield* decodeRows(rows))[0] ?? null;
			yield* Effect.annotateCurrentSpan("workspace.checkpoint.lookup", baseline === null ? "miss" : "hit");
			return baseline;
		});
		const publish = Effect.fn("WorkspaceBaselineRepository.publish")(function* (baseline, previousSnapshotId) {
			const sql = yield* SqlClient;
			const checkpoint = baseline.checkpoint;
			const now = yield* Clock.currentTimeMillis;
			const payload = yield* Schema.encodeEffect(Schema.fromJsonString(WorkspaceBaseline))(baseline);
			return (previousSnapshotId == null ? yield* sql`
                INSERT INTO sandbox_workspace_baselines
                  (class_identity,repository,setup_fingerprint,image_digest,head,snapshot_id,payload,expires_at,created_at)
                VALUES (${checkpoint.classIdentity},${baseline.repository},${checkpoint.setupFingerprint},
                  ${checkpoint.imageDigest},${checkpoint.head},${checkpoint.snapshotId},${payload}::jsonb,
                  to_timestamp(${checkpoint.expiresAt / 1e3}),to_timestamp(${checkpoint.capturedAt / 1e3}))
                ON CONFLICT (class_identity,repository,setup_fingerprint,image_digest) DO UPDATE SET
                  head=excluded.head,snapshot_id=excluded.snapshot_id,payload=excluded.payload,
                  expires_at=excluded.expires_at,created_at=excluded.created_at
                WHERE ${previousSnapshotId === null ? sql`false` : sql`sandbox_workspace_baselines.expires_at <= to_timestamp(${(now + WORKSPACE_SNAPSHOT_HEADROOM) / 1e3})`}
                RETURNING snapshot_id` : yield* sql`
                UPDATE sandbox_workspace_baselines SET head=${checkpoint.head},snapshot_id=${checkpoint.snapshotId},payload=${payload}::jsonb,
                  expires_at=to_timestamp(${checkpoint.expiresAt / 1e3}),created_at=to_timestamp(${checkpoint.capturedAt / 1e3})
                WHERE class_identity=${checkpoint.classIdentity} AND repository=${baseline.repository}
                  AND setup_fingerprint=${checkpoint.setupFingerprint} AND image_digest=${checkpoint.imageDigest}
                  AND snapshot_id=${previousSnapshotId}
                RETURNING snapshot_id`).length === 1;
		});
		const invalidate = Effect.fn("WorkspaceBaselineRepository.invalidate")(function* (key, snapshotId) {
			yield* (yield* SqlClient)`DELETE FROM sandbox_workspace_baselines
            WHERE class_identity=${key.classIdentity} AND repository=${key.repository}
              AND image_digest=${key.imageDigest} AND head=${key.head} AND snapshot_id=${snapshotId}`;
		});
		const due = Effect.fnUntraced(function* (owner) {
			const sql = yield* SqlClient;
			const now = yield* Clock.currentTimeMillis;
			const rows = yield* sql`SELECT payload FROM sandbox_workspace_baselines
            WHERE class_identity=${owner.classIdentity} AND image_digest=${owner.imageDigest}
              AND expires_at > to_timestamp(${(now + WORKSPACE_SNAPSHOT_HEADROOM) / 1e3})
              AND expires_at < to_timestamp(${(now + WORKSPACE_BASELINE_REFRESH_HEADROOM) / 1e3})
            ORDER BY expires_at LIMIT 5`;
			return yield* decodeRows(rows);
		});
		return WorkspaceBaselineRepository.of({
			find: (key) => withClient(find(key)),
			publish: (baseline, previous) => withClient(publish(baseline, previous)),
			invalidate: (key, snapshotId) => withClient(invalidate(key, snapshotId)),
			due: (owner) => withClient(due(owner))
		});
	}));
};
//#endregion
export { WORKSPACE_BASELINE_REFRESH_HEADROOM, WORKSPACE_PREPARATION_VERSION, WORKSPACE_SNAPSHOT_HEADROOM, WorkspaceBaseline, WorkspaceBaselineError, WorkspaceBaselineRepository };
