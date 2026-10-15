import { Schema } from "effect";

export class ConflictError extends Schema.TaggedError<ConflictError>()("RepairConflict", {
  message: Schema.String,
}) {}

export class ServiceUnavailableError extends Schema.TaggedError<ServiceUnavailableError>()(
  "RepairUnavailable",
  { message: Schema.String },
) {
  declare cause?: unknown;
}
