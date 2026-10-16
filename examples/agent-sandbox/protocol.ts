import { Schema } from "effect";

export const CodingRequest = Schema.Struct({
  instructions: Schema.NonEmptyString.check(Schema.isMaxLength(8_000)),
});
export const CodingResult = Schema.Struct({
  head: Schema.String,
  forkName: Schema.String,
  changedPaths: Schema.Array(Schema.String),
});
export class CodingError extends Schema.TaggedError<CodingError>()("CodingError", {
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}
