import { Context, Effect, Layer, Schema } from "effect";

import type { RunArchiveReceipt, SessionRunRecord } from "../RunRecord";

export const MAX_RUN_ARCHIVE_BYTES = 8 * 1024 * 1024;

/** A host's final archive document exceeded the agreed retention ceiling. */
export class RunArchiveLimitError extends Schema.TaggedError<RunArchiveLimitError>()(
  "RunArchiveLimitError",
  {
    observedBytes: Schema.Int,
    maxBytes: Schema.Int,
  },
) {}

/** Failure raised by a host-provided run-retention adapter. */
export class RunSinkError extends Schema.TaggedError<RunSinkError>()("RunSinkError", {
  cause: Schema.Defect(),
}) {}

/** Optional host adapter for retaining complete message-run observations. */
export class SessionRunSink extends Context.Service<
  SessionRunSink,
  {
    /** Retains one completed or failed run and returns host-issued retrieval data. */
    readonly archive: (
      record: SessionRunRecord,
    ) => Effect.Effect<RunArchiveReceipt | null, RunSinkError>;
  }
>()("sandbox-agent/SessionRunSink") {
  /** Disables retention while preserving the same session execution path. */
  static readonly noopLayer = Layer.succeed(SessionRunSink)(
    SessionRunSink.of({ archive: () => Effect.succeed(null) }),
  );
}
