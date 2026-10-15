import { Context, Effect, Schema } from "effect";
//#region src/cloudflare/RepositoryPersistenceCheckpoint.ts
var RepositoryPersistenceCheckpointError = class extends Schema.TaggedError()("RepositoryPersistenceCheckpointError", { cause: Schema.Defect() }) {};
/** Optional host authority for retaining an exact candidate before its remote write. */
const RepositoryPersistenceCheckpoint = Context.Reference("kommunikasie/RepositoryPersistenceCheckpoint", { defaultValue: () => ({ prepared: () => Effect.void }) });
//#endregion
export { RepositoryPersistenceCheckpoint, RepositoryPersistenceCheckpointError };
