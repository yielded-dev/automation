import { Context, Effect, Layer, Schema } from "effect";
//#region src/cloudflare/RunSink.ts
const MAX_RUN_ARCHIVE_BYTES = 8388608;
/** A host's final archive document exceeded the agreed retention ceiling. */
var RunArchiveLimitError = class extends Schema.TaggedError()("RunArchiveLimitError", {
	observedBytes: Schema.Int,
	maxBytes: Schema.Int
}) {};
/** Failure raised by a host-provided run-retention adapter. */
var RunSinkError = class extends Schema.TaggedError()("RunSinkError", { cause: Schema.Defect() }) {};
/** Optional host adapter for retaining complete message-run observations. */
var SessionRunSink = class SessionRunSink extends Context.Service()("sandbox-agent/SessionRunSink") {
	/** Disables retention while preserving the same session execution path. */
	static noopLayer = Layer.succeed(SessionRunSink)(SessionRunSink.of({ archive: () => Effect.succeed(null) }));
};
//#endregion
export { MAX_RUN_ARCHIVE_BYTES, RunArchiveLimitError, RunSinkError, SessionRunSink };
