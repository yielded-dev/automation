import { Cause, Clock, Effect, Predicate, Random, Schema } from "effect";
import type { Artifacts } from "effect-cf";

import { reportSanitizedCause } from "../observability/RuntimeDiagnostics";
import { withFailureDiagnostic } from "../observability/FailureDiagnostic";

import type { SandboxAgentDeadlinePolicy } from "./Config";

/** One provider operation exceeded its bound and was interrupted. */
export class ArtifactsOperationTimeoutError extends Schema.TaggedError<ArtifactsOperationTimeoutError>()(
  "ArtifactsOperationTimeoutError",
  {
    operation: Schema.String,
    timeoutMillis: Schema.Int,
  },
) {}

/** The writable Artifacts fork did not become readable before its setup deadline. */
export class ForkNotReadyError extends Schema.TaggedError<ForkNotReadyError>()(
  "ForkNotReadyError",
  {
    forkName: Schema.String,
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

const missingRepositoryMessage =
  /^ArtifactsError: Repository not found: [A-Za-z0-9][A-Za-z0-9._-]{0,62}\.?$/u;

/** True when a provider error describes a fork that is absent or not readable yet. */
export const isForkProvisioning = (error: Artifacts.ArtifactsOperationError): boolean => {
  if (
    error.code === "NOT_FOUND" ||
    error.numericCode === 10_200 ||
    error.code === "FORK_IN_PROGRESS" ||
    error.numericCode === 10_303
  ) {
    return true;
  }

  // Wrangler's remote Artifacts bridge currently preserves only the provider
  // Error message for a missing repository. Restrict that compatibility path
  // to the exact get-repository shape so unrelated provider failures stay fatal.
  return (
    error.operation === "get" &&
    Predicate.isObject(error.cause) &&
    Predicate.hasProperty(error.cause, "message") &&
    typeof error.cause.message === "string" &&
    missingRepositoryMessage.test(error.cause.message)
  );
};

export interface ArtifactsForkPolicy {
  readonly operationTimeoutMillis: number;
  readonly readyDeadlineMillis: number;
  readonly eagerPollMillis?: number;
  readonly eagerPollAttempts?: number;
  readonly steadyPollMillis?: number;
}

export const artifactsForkPolicyFromDeadlines = (
  deadlines: SandboxAgentDeadlinePolicy,
): ArtifactsForkPolicy => ({
  operationTimeoutMillis: deadlines.artifactsOperationMillis,
  readyDeadlineMillis: deadlines.forkReadyMillis,
});

export interface ArtifactsForkInput {
  readonly artifacts: Artifacts.ArtifactsClient;
  readonly seedRepository: string;
  readonly forkName: string;
  readonly policy: ArtifactsForkPolicy;
  readonly description?: string;
  readonly defaultBranchOnly?: boolean;
  readonly readyRef?: string;
  readonly deadlineAtMillis?: number;
}

export type ArtifactsForkDisposition = "created" | "existing" | "indeterminate";

/** Result of finding or provisioning a durable writable fork. */
export interface EnsureArtifactsForkResult {
  readonly head: string;
  /** What the provider proved about this caller's fork request. */
  readonly disposition: ArtifactsForkDisposition;
  readonly pollAttempts: number;
}

const providerTimeout =
  (operation: string, timeoutMillis: number) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.timeoutOrElse({
        duration: timeoutMillis,
        orElse: () =>
          ArtifactsOperationTimeoutError.make({
            operation,
            timeoutMillis,
          }),
      }),
      (bounded) => withFailureDiagnostic(bounded, { operation: `ArtifactsFork.${operation}` }),
    );

/** Reads a branch (main by default); null means that ref has no readable commit yet. */
export const artifactsMainHead = Effect.fn("ArtifactsFork.artifactsMainHead")(function* (
  artifacts: Artifacts.ArtifactsClient,
  forkName: string,
  operationTimeoutMillis: number,
  ref = "main",
) {
  return yield* Effect.gen(function* () {
    const repository = yield* artifacts.get(forkName);
    const commits = yield* repository.log({ ref, limit: 1 });

    return commits[0]?.hash ?? null;
  }).pipe(providerTimeout("mainHead", operationTimeoutMillis));
});

/**
 * Requests a fork without waiting for materialization. A timed-out mutation is
 * indeterminate and therefore adopted: the kernel's readiness loop reconciles
 * the target instead of issuing an unsafe alternate mutation.
 */
export const requestArtifactsFork = Effect.fn("ArtifactsFork.requestArtifactsFork")(function* (
  input: ArtifactsForkInput,
) {
  const disposition = yield* Effect.gen(function* () {
    const seed = yield* input.artifacts.get(input.seedRepository);

    yield* seed.fork(input.forkName, {
      defaultBranchOnly: input.defaultBranchOnly ?? true,
      readOnly: false,
      ...(input.description === undefined ? {} : { description: input.description }),
    });

    return "created" as const;
  }).pipe(
    providerTimeout("fork", input.policy.operationTimeoutMillis),
    Effect.catchCause((cause) => {
      const uncertain = cause.reasons.every(
        (reason) =>
          Cause.isFailReason(reason) &&
          (reason.error._tag === "ArtifactsOperationTimeoutError" ||
            reason.error.code === "ALREADY_EXISTS" ||
            reason.error.code === "FORK_IN_PROGRESS"),
      );

      if (!uncertain || cause.reasons.length === 0) return Effect.failCause(cause);

      // The mutation may have reached Artifacts. Reconcile this exact target;
      // defects and interruption must never be mistaken for a lost response.
      return Effect.succeed(
        cause.reasons.every(
          (reason) =>
            Cause.isFailReason(reason) &&
            reason.error._tag === "ArtifactsOperationError" &&
            reason.error.code === "ALREADY_EXISTS",
        )
          ? ("existing" as const)
          : ("indeterminate" as const),
      );
    }),
  );

  return { disposition } as const;
});

/** Waits for the fork's selected ref with bounded provider probes and jitter. */
export const awaitArtifactsForkReady = Effect.fn("ArtifactsFork.awaitArtifactsForkReady")(
  function* (
    input: Omit<ArtifactsForkInput, "seedRepository"> & {
      readonly onPollAttempt?: () => void;
    },
  ) {
    const eagerPollMillis = input.policy.eagerPollMillis ?? 300;
    const eagerPollAttempts = input.policy.eagerPollAttempts ?? 20;
    const steadyPollMillis = input.policy.steadyPollMillis ?? 1_000;
    const deadlineAt =
      input.deadlineAtMillis ?? (yield* Clock.currentTimeMillis) + input.policy.readyDeadlineMillis;
    let lastFailure:
      | Cause.Cause<Artifacts.ArtifactsOperationError | ArtifactsOperationTimeoutError>
      | undefined;

    for (let attempt = 0; ; attempt += 1) {
      const beforeProbe = yield* Clock.currentTimeMillis;

      if (beforeProbe >= deadlineAt) break;
      input.onPollAttempt?.();
      const head = yield* artifactsMainHead(
        input.artifacts,
        input.forkName,
        Math.max(1, Math.min(input.policy.operationTimeoutMillis, deadlineAt - beforeProbe)),
        input.readyRef,
      ).pipe(
        Effect.catchCause((cause) => {
          if (
            cause.reasons.length === 0 ||
            !cause.reasons.every(
              (reason) =>
                Cause.isFailReason(reason) &&
                (reason.error._tag === "ArtifactsOperationTimeoutError" ||
                  isForkProvisioning(reason.error) ||
                  reason.error.code === "INTERNAL_ERROR"),
            )
          )
            return Effect.failCause(cause);
          // A materializing fork can briefly fail reads after accepting creation.
          // Reconcile that same fork; the original deadline still bounds readiness.
          lastFailure = cause;

          if (
            cause.reasons.some(
              (reason) =>
                Cause.isFailReason(reason) &&
                reason.error._tag === "ArtifactsOperationError" &&
                reason.error.code === "INTERNAL_ERROR",
            )
          )
            return reportSanitizedCause(cause, {
              operation: "ArtifactsFork.awaitArtifactsForkReady",
            }).pipe(Effect.as(null));

          return Effect.succeed(null);
        }),
      );

      if (head !== null) return head;
      const now = yield* Clock.currentTimeMillis;

      if (now >= deadlineAt) break;
      const baseDelay = attempt < eagerPollAttempts ? eagerPollMillis : steadyPollMillis;
      const random = yield* Random.next;
      const jitteredDelay = Math.round(baseDelay * (0.8 + random * 0.4));

      yield* Effect.sleep(Math.min(jitteredDelay, deadlineAt - now));
    }

    return yield* ForkNotReadyError.make({
      forkName: input.forkName,
      message: `Fork was not ready after ${input.policy.readyDeadlineMillis / 1_000} seconds`,
      ...(lastFailure === undefined ? {} : { cause: lastFailure }),
    });
  },
);

/** Finds or provisions a fork and returns only after its `main` head is readable. */
export const ensureArtifactsFork = Effect.fn("ArtifactsFork.ensureArtifactsFork")(function* (
  input: ArtifactsForkInput & {
    readonly onPollAttempt?: () => void;
  },
) {
  const existingHead = yield* artifactsMainHead(
    input.artifacts,
    input.forkName,
    input.policy.operationTimeoutMillis,
  ).pipe(
    Effect.catchTags({
      ArtifactsOperationError: (error) =>
        isForkProvisioning(error) ? Effect.succeed(null) : Effect.fail(error),
      ArtifactsOperationTimeoutError: () => Effect.succeed(null),
    }),
  );

  if (existingHead !== null) {
    return {
      head: existingHead,
      disposition: "existing",
      pollAttempts: 0,
    } satisfies EnsureArtifactsForkResult;
  }
  const requested = yield* requestArtifactsFork(input);
  let pollAttempts = 0;
  const head = yield* awaitArtifactsForkReady({
    artifacts: input.artifacts,
    forkName: input.forkName,
    policy: input.policy,
    onPollAttempt: () => {
      pollAttempts += 1;
      input.onPollAttempt?.();
    },
  });

  return {
    head,
    disposition: requested.disposition,
    pollAttempts,
  } satisfies EnsureArtifactsForkResult;
});
