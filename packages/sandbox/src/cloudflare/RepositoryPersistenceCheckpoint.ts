import { Context, Effect, Schema } from "effect";

import type { PersistResult } from "./ArtifactsCheckout";

export class RepositoryPersistenceCheckpointError extends Schema.TaggedError<RepositoryPersistenceCheckpointError>()(
  "RepositoryPersistenceCheckpointError",
  { cause: Schema.Defect() },
) {}

/** Optional host authority for retaining an exact candidate before its remote write. */
export const RepositoryPersistenceCheckpoint = Context.Reference<{
  readonly prepared: (
    candidate: PersistResult,
    transportDirectory: string | null,
  ) => Effect.Effect<void, RepositoryPersistenceCheckpointError>;
}>("kommunikasie/RepositoryPersistenceCheckpoint", {
  defaultValue: () => ({ prepared: () => Effect.void }),
});
