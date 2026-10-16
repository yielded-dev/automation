import { Cause, Effect, Predicate } from "effect";

import { reportSanitizedCause } from "../observability/RuntimeDiagnostics";

import { Sandboxes } from "./Bindings";
import { resourceHash } from "./resource-names";
import { WorkspaceBaselineRepository } from "./WorkspaceBaselineRepository";

/** Refresh a bounded batch from pristine snapshots, without a transaction across container I/O. */
export const refreshWorkspaceBaselines = Effect.gen(function* () {
  const baselines = yield* WorkspaceBaselineRepository;
  const owner = yield* Sandboxes.get("baseline-refresh-policy");
  const description = yield* owner.describe;
  const due = yield* baselines.due(description);

  yield* Effect.forEach(
    due,
    (baseline) =>
      Effect.gen(function* () {
        const saved = baseline.checkpoint;
        const name = `baseline-refresh-${yield* resourceHash(`${saved.classIdentity}:${saved.snapshotId}`)}`;
        const sandbox = yield* Sandboxes.get(name);
        const description = yield* sandbox.describe;

        // The same database can hold baselines from several independently deployed classes.
        if (
          description.classIdentity !== saved.classIdentity ||
          description.imageDigest !== saved.imageDigest
        )
          return;
        yield* Effect.acquireUseRelease(
          Effect.succeed(sandbox),
          () =>
            Effect.gen(function* () {
              const identity = yield* sandbox.ensure({ checkpoint: saved });

              if (identity.restoredSnapshotId !== saved.snapshotId) {
                yield* baselines.invalidate(
                  {
                    repository: baseline.repository,
                    classIdentity: saved.classIdentity,
                    imageDigest: saved.imageDigest,
                    head: saved.head,
                  },
                  saved.snapshotId,
                );

                return;
              }
              const checkpoint = yield* sandbox.checkpoint({
                captureId: `refresh:${saved.snapshotId}:${identity.instanceId}`,
                instanceId: identity.instanceId,
                setupFingerprint: saved.setupFingerprint,
                head: saved.head,
                materializationId: saved.materializationId,
                snapshotAllowed: true,
              });

              if (checkpoint !== null)
                yield* baselines.publish({ ...baseline, checkpoint }, saved.snapshotId);
            }),
          () => sandbox.destroy,
        );
      }).pipe(
        // Preserve the original cause and continue the bounded batch. No accepted
        // user execution occurs in these dedicated immutable cache owners.
        Effect.catchCause((cause) =>
          cause.reasons.every(
            (reason) =>
              Cause.isFailReason(reason) &&
              (Predicate.isTagged(reason.error, "SandboxOperationError") ||
                Predicate.isTagged(reason.error, "WorkspaceBaselineError")),
          )
            ? reportSanitizedCause(cause, { operation: "WorkspaceBaseline.refresh" })
            : Effect.failCause(cause),
        ),
      ),
    { concurrency: 1, discard: true },
  );
});
