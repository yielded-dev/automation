import { Cause, Clock, Data, Effect, Exit, Option, Schema } from "effect";

import type { ContainerSize } from "../Contract";
import { SessionSandbox } from "../SessionSandbox";
import { RepositorySetupError } from "../model/RepositorySetup";
import { inMeasure } from "../tracing/RunSpanCapture";
import { MATERIALIZATION_MARKER, REPO_DIR } from "../workspace-paths";
import { ArtifactsCheckout, type ArtifactsRepositorySource } from "./ArtifactsCheckout";
import { sandboxFor } from "./Bindings";
import { SandboxAgentConfig, sandboxAgentDeadlinePolicy } from "./Config";
import { NativeWorkspaceIdentity } from "./NativeWorkspaceIdentity";
import { OutboundAuthority } from "./OutboundAuthority";
import { recoverRepositoryCheckpoint } from "./RepositoryCheckpoint";
import {
  prepareRepositorySetup,
  readRepositorySetup,
  repositorySetupCheckpointMatches,
  repositorySetupFingerprint,
} from "./RepositorySetup";
import { resourceHash, sandboxNameFor } from "./resource-names";
import { withSandboxLease } from "./SandboxLease";
import { SessionStateStore } from "./SessionStateStore";
import {
  WORKSPACE_PREPARATION_VERSION,
  WORKSPACE_SNAPSHOT_HEADROOM,
  WorkspaceBaselineRepository,
  type WorkspaceBaseline,
} from "./WorkspaceBaselineRepository";
import type { StoredWorkspaceBaselinePreparation } from "./WorkspaceBaselinePreparationState";

export class WorkspaceBaselinePreparationError extends Data.TaggedError(
  "WorkspaceBaselinePreparationError",
)<{
  readonly reason:
    | "request-mismatch"
    | "workspace-mismatch"
    | "workspace-lost"
    | "source-changed"
    | "setup-failed"
    | "publication-unavailable"
    | "publication-conflict";
  readonly cause?: unknown;
}> {}

const isTerminalPreparationError = Schema.is(
  Schema.TaggedStruct("WorkspaceBaselinePreparationError", {
    reason: Schema.Literals([
      "publication-conflict",
      "workspace-lost",
      "workspace-mismatch",
      "source-changed",
      "setup-failed",
    ]),
  }),
);

/** Only a nonempty, entirely terminal cause permits a new execution generation. */
export const isTerminalWorkspaceBaselinePreparationCause = <E>(cause: Cause.Cause<E>): boolean =>
  cause.reasons.length > 0 &&
  cause.reasons.every(
    (reason) => Cause.isFailReason(reason) && isTerminalPreparationError(reason.error),
  );

const publishCapturedBaseline = Effect.fnUntraced(function* (
  baseline: WorkspaceBaseline,
  previousSnapshotId: string | null,
) {
  if (
    baseline.checkpoint.expiresAt <=
    (yield* Clock.currentTimeMillis) + WORKSPACE_SNAPSHOT_HEADROOM
  )
    return { status: "expired", baseline } as const;
  const baselines = yield* WorkspaceBaselineRepository;
  const captured: WorkspaceBaseline = {
    ...baseline,
    report: {
      ...baseline.report,
      checkpoint: { capture: "captured", publication: "published" },
    },
  };
  if (
    yield* baselines
      .publish(captured, previousSnapshotId)
      .pipe(inMeasure("workspace-checkpoint-publish"))
  )
    return { status: "prepared", baseline: captured } as const;
  const published = yield* baselines.find({
    repository: baseline.repository,
    classIdentity: baseline.checkpoint.classIdentity,
    imageDigest: baseline.checkpoint.imageDigest,
    head: baseline.checkpoint.head,
    setupFingerprint: baseline.checkpoint.setupFingerprint,
  });

  if (published?.checkpoint.head !== baseline.checkpoint.head)
    return yield* new WorkspaceBaselinePreparationError({ reason: "publication-conflict" });
  return { status: "prepared", baseline: published } as const;
});

/**
 * Prepare the accepted exact commit in a dedicated execution owner. Hosts
 * serialize/coalesce preparation per repository and select current main under
 * that gate. Retain this request and execution generation across interruptions.
 * Pure publication-conflict, workspace-lost, workspace-mismatch, source-changed
 * or setup-failed failures, and expired results, require a new execution generation.
 * Transport/registry and mixed failures retry the same owner, capture and
 * predecessor; cleanup failures stay visible.
 * Demand repairs may still restore compatible older heads. No task, writable
 * fork, runtime service, model or injected secret enters shared preparation.
 */
export const prepareWorkspaceBaseline = Effect.fn("WorkspaceBaseline.prepare")(function* (input: {
  readonly sessionId: string;
  readonly turnId: string;
  /** Omit to share the source-generation baseline used by native repair tasks. */
  readonly preparationKey?: string;
  readonly source: ArtifactsRepositorySource;
  readonly repositoryId?: string;
  readonly containerSize?: ContainerSize;
}) {
  const config = yield* SandboxAgentConfig;
  const store = yield* SessionStateStore;
  const baselines = yield* WorkspaceBaselineRepository;
  const checkout = yield* ArtifactsCheckout;
  const size = input.containerSize ?? "standard";
  const requestKey = yield* resourceHash(
    JSON.stringify([
      input.sessionId,
      input.turnId,
      input.preparationKey ?? null,
      input.source.repositoryName,
      input.source.repositoryId,
      input.source.commitSha,
      input.source.artifactsRef ?? null,
      input.repositoryId ?? null,
      size,
      config.artifactsHostname,
      config.artifactsNamespace,
      config.sandboxNamespace,
      config.repositorySetupPath,
      WORKSPACE_PREPARATION_VERSION,
    ]),
  );
  const saved = yield* store.workspaceBaselinePreparation.get;
  let retained: StoredWorkspaceBaselinePreparation = saved ?? { requestKey, result: null };

  if (retained.requestKey !== requestKey)
    return yield* new WorkspaceBaselinePreparationError({ reason: "request-mismatch" });
  if (saved === null) yield* store.workspaceBaselinePreparation.put(retained);
  const name = sandboxNameFor(yield* resourceHash(input.sessionId));
  const owner = yield* sandboxFor(size, name);
  const previousInstance = yield* store.containerInstanceId.get;

  if (retained.result !== null) {
    yield* owner.destroy;
    if (
      retained.result.baseline !== null &&
      retained.result.baseline.checkpoint.head !== input.source.commitSha
    )
      return yield* new WorkspaceBaselinePreparationError({ reason: "publication-conflict" });
    return retained.result;
  }
  const repository =
    input.preparationKey === undefined
      ? `${config.artifactsHostname}/${config.artifactsNamespace}/${input.source.repositoryName}@${input.source.repositoryId}`
      : `${config.artifactsHostname}/${config.artifactsNamespace}/prepared:${input.preparationKey}`;
  const description = yield* owner.describe;
  const selection = {
    repository,
    classIdentity: description.classIdentity,
    imageDigest: description.imageDigest,
    head: input.source.commitSha,
  };
  const result = yield* Effect.gen(function* () {
    const savedCheckpoint =
      (yield* store.workspaceCheckpoint.get) ??
      (yield* recoverRepositoryCheckpoint(
        description.checkpoint,
        yield* store.workspaceCheckpointInput.get,
      ));
    const savedSetup = yield* store.repositorySetupReceipt.get;

    // An acknowledged native capture can be published after the scratch container is lost.
    if (savedCheckpoint !== null && savedSetup !== null) {
      if (
        savedCheckpoint.head !== input.source.commitSha ||
        savedCheckpoint.classIdentity !== selection.classIdentity ||
        savedCheckpoint.imageDigest !== selection.imageDigest ||
        savedCheckpoint.instanceId !== previousInstance ||
        savedCheckpoint.materializationId !== savedSetup.materializationId ||
        savedCheckpoint.setupFingerprint !== savedSetup.fingerprint ||
        savedSetup.sourceHead !== input.source.commitSha
      )
        return yield* new WorkspaceBaselinePreparationError({ reason: "workspace-mismatch" });
      return yield* publishCapturedBaseline(
        { repository, checkpoint: savedCheckpoint, report: savedSetup.report },
        retained.previousSnapshotId ?? null,
      );
    }
    // Acknowledged captures above survive owner loss. Without one, never replay
    // installation in an execution whose accepted native instance is gone.
    if (previousInstance !== null && description.current?.instanceId !== previousInstance)
      return yield* new WorkspaceBaselinePreparationError({ reason: "workspace-lost" });
    if (retained.base === undefined) {
      retained = {
        ...retained,
        base: yield* baselines.find({ ...selection, includeExpired: true }),
      };
      yield* store.workspaceBaselinePreparation.put(retained);
    }
    const base = retained.base;
    let identity = yield* owner.ensure({
      ...(previousInstance === null
        ? { checkpoint: base?.checkpoint }
        : { expectedInstanceId: previousInstance }),
      recovery: previousInstance === null ? "freshAllowed" : "exactWorkspaceRequired",
    });
    yield* store.containerInstanceId.put(identity.instanceId);

    while (true) {
      const sandbox = yield* sandboxFor(size, name, {
        recovery: "exactWorkspaceRequired",
        expectedInstanceId: identity.instanceId,
      });
      const prepared = yield* withSandboxLease(
        input.turnId,
        Effect.gen(function* () {
          const authority = yield* (yield* OutboundAuthority).mint({
            sessionId: input.sessionId,
            turnId: input.turnId,
            instanceId: identity.instanceId,
          });
          if (Option.isSome(authority)) yield* sandbox.configureOutboundAuthority(authority.value);
          const previousMaterialization = yield* store.materializationId.get;
          const materializationId = previousMaterialization ?? requestKey;
          let installed: WorkspaceBaseline | undefined;

          if (identity.restoredSnapshotId !== null) {
            if (base == null || base.checkpoint.snapshotId !== identity.restoredSnapshotId)
              return yield* new WorkspaceBaselinePreparationError({ reason: "workspace-mismatch" });
            const marker = (yield* sandbox.readFileString(MATERIALIZATION_MARKER)).trim();
            if (
              marker !== (previousMaterialization ?? base.checkpoint.materializationId) &&
              marker !== materializationId
            )
              return yield* new WorkspaceBaselinePreparationError({ reason: "workspace-mismatch" });
            yield* checkout.readSourcePin(input.source);
            yield* checkout.ensureCommit(input.source.repositoryName, input.source.commitSha, {
              includeHistory: true,
              repositoryId: input.source.repositoryId,
            });
            installed = base;
          } else if (previousMaterialization === null) {
            const cloned = yield* checkout.cloneSourceIntoSandbox(
              input.source,
              input.source.repositoryName,
            );
            yield* sandbox.writeFile(MATERIALIZATION_MARKER, materializationId);
            yield* store.materializationId.put(materializationId);
            yield* store.verifiedHead.put(cloned.head);
          } else if (
            (yield* sandbox.readFileString(MATERIALIZATION_MARKER)).trim() !== materializationId
          ) {
            return yield* new WorkspaceBaselinePreparationError({ reason: "workspace-mismatch" });
          }
          const recipe = yield* readRepositorySetup(input.source.commitSha);
          if ((recipe?.requiredSecrets.length ?? 0) > 0)
            return { status: "disabled", baseline: null } as const;
          const fingerprint = yield* repositorySetupFingerprint(input.source.commitSha, recipe);
          if (
            installed !== undefined &&
            !repositorySetupCheckpointMatches(installed, recipe, fingerprint)
          )
            return null;

          if (installed !== undefined && previousMaterialization === null) {
            // A retry can find checkout complete but its marker acknowledgement missing.
            const head = yield* sandbox.inspect(["git", "rev-parse", "HEAD"], {
              cwd: REPO_DIR,
              timeout: sandboxAgentDeadlinePolicy(config).gitCommandMillis,
              maxBytes: 128,
            });
            if (
              head.exitCode !== 0 ||
              head.timedOut ||
              head.truncated ||
              (head.stdout.trim() !== installed.checkpoint.head &&
                head.stdout.trim() !== input.source.commitSha)
            )
              return yield* new WorkspaceBaselinePreparationError({ reason: "workspace-mismatch" });
            yield* checkout.restoreSnapshotCheckout(
              input.source.repositoryName,
              input.source.commitSha,
              head.stdout.trim(),
            );
            yield* sandbox.writeFile(MATERIALIZATION_MARKER, materializationId);
            yield* store.materializationId.put(materializationId);
            yield* store.verifiedHead.put(input.source.commitSha);
          }
          yield* checkout.readSourcePin(input.source);
          if (retained.previousSnapshotId === undefined) {
            const existing =
              base?.checkpoint.setupFingerprint === fingerprint
                ? base
                : yield* baselines.find({
                    ...selection,
                    setupFingerprint: fingerprint,
                    includeExpired: true,
                  });
            if (
              existing !== null &&
              existing !== undefined &&
              existing.checkpoint.head === input.source.commitSha &&
              existing.checkpoint.expiresAt >
                (yield* Clock.currentTimeMillis) + WORKSPACE_SNAPSHOT_HEADROOM &&
              repositorySetupCheckpointMatches(existing, recipe, fingerprint)
            )
              return { status: "reused", baseline: existing } as const;
            retained = { ...retained, previousSnapshotId: existing?.checkpoint.snapshotId ?? null };
            yield* store.workspaceBaselinePreparation.put(retained);
          }
          if (installed === undefined)
            yield* checkout.ensureCommit(input.source.repositoryName, input.source.commitSha, {
              includeHistory: true,
              repositoryId: input.source.repositoryId,
            });
          return yield* Effect.gen(function* () {
            const report = yield* prepareRepositorySetup({
              sourceKey: input.source.repositoryName,
              sourceHead: input.source.commitSha,
              execution: {
                sessionId: input.sessionId,
                turnId: input.turnId,
                instanceId: identity.instanceId,
                ...(input.repositoryId === undefined ? {} : { repositoryId: input.repositoryId }),
              },
              inputs: { sourceHead: input.source.commitSha, recipe, fingerprint },
              installed,
              phase: "installation",
              snapshot: { repository, identity, share: false },
            });
            const checkpoint = yield* store.workspaceCheckpoint.get;
            if (checkpoint === null)
              return yield* new WorkspaceBaselinePreparationError({
                reason: "publication-unavailable",
              });
            return yield* publishCapturedBaseline(
              { repository, checkpoint, report },
              retained.previousSnapshotId ?? null,
            );
          }).pipe(
            Effect.provideService(NativeWorkspaceIdentity, {
              path: MATERIALIZATION_MARKER,
              id: materializationId,
            }),
          );
        }),
      ).pipe(Effect.provideService(SessionSandbox, sandbox));

      if (prepared !== null) return prepared;
      // Installation can write outside the checkout. Changed inputs require a clean boot.
      // No install has been admitted in this incompatible restore.
      yield* store.containerInstanceId.delete;
      yield* store.materializationId.delete;
      yield* owner.destroy;
      identity = yield* owner.ensure({ recovery: "freshAllowed" });
      yield* store.containerInstanceId.put(identity.instanceId);
    }
  }).pipe(
    Effect.catchCause((cause) => {
      if (cause.reasons.length === 0) return Effect.failCause(cause);
      if (
        cause.reasons.every(
          (reason) =>
            Cause.isFailReason(reason) &&
            Schema.is(RepositorySetupError)(reason.error) &&
            reason.error.recovery === "terminal",
        )
      )
        return Effect.fail(
          new WorkspaceBaselinePreparationError({ reason: "setup-failed", cause }),
        );
      return cause.reasons.every(
        (reason) =>
          Cause.isFailReason(reason) &&
          (reason.error._tag === "ArtifactsRepositoryGenerationMismatchError" ||
            (reason.error._tag === "RepositoryForkIntentError" &&
              reason.error.reason === "source-pin")),
      )
        ? Effect.fail(new WorkspaceBaselinePreparationError({ reason: "source-changed", cause }))
        : Effect.failCause(cause);
    }),
    Effect.onExit((exit) =>
      Exit.isFailure(exit) &&
      exit.cause.reasons.some(
        (reason) => Cause.isFailReason(reason) && isTerminalPreparationError(reason.error),
      )
        ? owner.destroy.pipe(Effect.orDie)
        : Effect.void,
    ),
  );
  yield* store.workspaceBaselinePreparation.put({ requestKey, result });
  yield* owner.destroy;
  return result;
});
