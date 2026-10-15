import { reportSanitizedCause } from "../observability/RuntimeDiagnostics.mjs";
import { resourceHash } from "./resource-names.mjs";
import { Sandboxes } from "./Bindings.mjs";
import { WorkspaceBaselineRepository } from "./WorkspaceBaselineRepository.mjs";
import { Cause, Effect, Predicate } from "effect";
//#region src/cloudflare/WorkspaceBaselineRefresh.ts
/** Refresh a bounded batch from pristine snapshots, without a transaction across container I/O. */
const refreshWorkspaceBaselines = Effect.gen(function* () {
	const baselines = yield* WorkspaceBaselineRepository;
	const description = yield* (yield* Sandboxes.get("baseline-refresh-policy")).describe;
	const due = yield* baselines.due(description);
	yield* Effect.forEach(due, (baseline) => Effect.gen(function* () {
		const saved = baseline.checkpoint;
		const name = `baseline-refresh-${yield* resourceHash(`${saved.classIdentity}:${saved.snapshotId}`)}`;
		const sandbox = yield* Sandboxes.get(name);
		const description = yield* sandbox.describe;
		if (description.classIdentity !== saved.classIdentity || description.imageDigest !== saved.imageDigest) return;
		yield* Effect.acquireUseRelease(Effect.succeed(sandbox), () => Effect.gen(function* () {
			const identity = yield* sandbox.ensure({ checkpoint: saved });
			if (identity.restoredSnapshotId !== saved.snapshotId) {
				yield* baselines.invalidate({
					repository: baseline.repository,
					classIdentity: saved.classIdentity,
					imageDigest: saved.imageDigest,
					head: saved.head
				}, saved.snapshotId);
				return;
			}
			const checkpoint = yield* sandbox.checkpoint({
				captureId: `refresh:${saved.snapshotId}:${identity.instanceId}`,
				instanceId: identity.instanceId,
				setupFingerprint: saved.setupFingerprint,
				head: saved.head,
				materializationId: saved.materializationId,
				snapshotAllowed: true
			});
			if (checkpoint !== null) yield* baselines.publish({
				...baseline,
				checkpoint
			}, saved.snapshotId);
		}), () => sandbox.destroy);
	}).pipe(Effect.catchCause((cause) => cause.reasons.every((reason) => Cause.isFailReason(reason) && (Predicate.isTagged(reason.error, "SandboxOperationError") || Predicate.isTagged(reason.error, "WorkspaceBaselineError"))) ? reportSanitizedCause(cause, { operation: "WorkspaceBaseline.refresh" }) : Effect.failCause(cause))), {
		concurrency: 1,
		discard: true
	});
});
//#endregion
export { refreshWorkspaceBaselines };
