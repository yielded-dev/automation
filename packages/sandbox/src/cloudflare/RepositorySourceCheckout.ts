import { Context, Effect, Layer, Schema } from "effect";
import type { Artifacts } from "effect-cf";

import type { RepositorySourceAccess } from "../model/RepositorySource";

import type { HarnessTurnError } from "../harness/Harness";
import { SessionSandbox } from "../SessionSandbox";
import {
  ArtifactsCheckout,
  type ArtifactsRepositoryGenerationMismatchError,
  type GitCommandError,
  type NonFastForwardPushError,
  type PersistedPathsLimitError,
  type PersistResult,
  type PreparedCommitUnavailableError,
  type PushVerificationError,
} from "./ArtifactsCheckout";
import { type ArtifactsOperationTimeoutError, type ForkNotReadyError } from "./ArtifactsFork";
import type * as Sandbox from "./NativeSandbox";
import type { SessionStateStore, SessionStateStoreError } from "./SessionStateStore";

/** The restored repository did not match the source accepted by the host. */
export class RepositorySourceRestoreHeadMismatchError extends Schema.TaggedError<RepositorySourceRestoreHeadMismatchError>()(
  "RepositorySourceRestoreHeadMismatchError",
  {
    repositoryName: Schema.String,
    acceptedSourceSha: Schema.String,
    restoredHead: Schema.String,
  },
) {}

export type RepositorySourcePersistenceError =
  | Artifacts.ArtifactsOperationError
  | ArtifactsOperationTimeoutError
  | ArtifactsRepositoryGenerationMismatchError
  | ForkNotReadyError
  | GitCommandError
  | NonFastForwardPushError
  | PersistedPathsLimitError
  | PreparedCommitUnavailableError
  | PushVerificationError
  | Sandbox.SandboxOperationError;

export interface RepositorySourceCheckoutService {
  readonly verifySource: Effect.Effect<
    void,
    RepositorySourcePersistenceError | RepositorySourceRestoreHeadMismatchError
  >;
  /** Reads the exact generation's current durable main head. */
  readonly mainHead: Effect.Effect<
    string,
    | Artifacts.ArtifactsOperationError
    | ArtifactsOperationTimeoutError
    | ArtifactsRepositoryGenerationMismatchError
    | ForkNotReadyError
  >;
  /** Restores the accepted repository source into the current sandbox workspace. */
  readonly restore: Effect.Effect<
    void,
    | Artifacts.ArtifactsOperationError
    | ArtifactsOperationTimeoutError
    | ArtifactsRepositoryGenerationMismatchError
    | GitCommandError
    | Sandbox.SandboxOperationError
    | RepositorySourceRestoreHeadMismatchError
    | HarnessTurnError
    | SessionStateStoreError,
    SessionSandbox | SessionStateStore
  >;
  /** Persists sandbox changes and verifies the resulting Artifacts main head. */
  readonly persist: <E, R>(
    beforePush: (prepared: PersistResult) => Effect.Effect<void, E, R>,
  ) => Effect.Effect<PersistResult, RepositorySourcePersistenceError | E, SessionSandbox | R>;
  /** Replays only the exact staged commit through the same generation-bound push. */
  readonly pushPrepared: (
    preparedHead: string,
  ) => Effect.Effect<PersistResult, RepositorySourcePersistenceError, SessionSandbox>;
}

/**
 * Restricted generation-bound repository checkout capability for one sandbox turn.
 * @effect-expect-leaking SessionSandbox
 */
export class RepositorySourceCheckout extends Context.Service<
  RepositorySourceCheckout,
  {
    /** Restricts the shared checkout adapter to one freshly verified repository source. */
    readonly forSource: (access: RepositorySourceAccess) => RepositorySourceCheckoutService;
  }
>()("kommunikasie/RepositorySourceCheckout") {
  static readonly layer = Layer.effect(
    RepositorySourceCheckout,
    Effect.gen(function* () {
      const checkout = yield* ArtifactsCheckout;

      const forSource = (access: RepositorySourceAccess): RepositorySourceCheckoutService => {
        const generation = {
          repositoryName: access.repositoryName,
          repositoryId: access.repositoryId,
        };
        const verifySource = Effect.gen(function* () {
          const head = yield* checkout.readGenerationMainHead(generation);

          if (head !== access.acceptedSourceSha)
            return yield* RepositorySourceRestoreHeadMismatchError.make({
              repositoryName: access.repositoryName,
              acceptedSourceSha: access.acceptedSourceSha,
              restoredHead: head,
            });
        });

        const restore = Effect.gen(function* () {
          const restored = yield* checkout.cloneGenerationIntoSandbox(generation);

          if (restored.head !== access.acceptedSourceSha) {
            return yield* RepositorySourceRestoreHeadMismatchError.make({
              repositoryName: access.repositoryName,
              acceptedSourceSha: access.acceptedSourceSha,
              restoredHead: restored.head,
            });
          }
        });

        const mainHead = checkout.readGenerationMainHead(generation);
        const persist = <E, R>(
          beforePush: (prepared: PersistResult) => Effect.Effect<void, E, R>,
        ) => checkout.persistGeneration(generation, access.acceptedSourceSha, beforePush);
        const pushPrepared = (preparedHead: string) =>
          checkout.pushPreparedGeneration(generation, access.acceptedSourceSha, preparedHead);

        return { verifySource, mainHead, restore, persist, pushPrepared };
      };

      return RepositorySourceCheckout.of({ forSource });
    }),
  );
}
