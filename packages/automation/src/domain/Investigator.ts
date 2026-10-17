import { Schema } from "effect";

/** Evidence is supplied by authorized readers. Its kind and contents belong to the application. */
export const InvestigationEvidence = Schema.Struct({
  id: Schema.NonEmptyString,
  kind: Schema.String,
  label: Schema.String,
  sourceId: Schema.NullOr(Schema.String),
  sourceName: Schema.String,
  occurredAt: Schema.NullOr(Schema.String),
  status: Schema.String,
  text: Schema.String,
  details: Schema.JsonObject,
});
export type InvestigationEvidence = typeof InvestigationEvidence.Type;

/** The common result of an investigation tool. Readers report unavailable evidence explicitly. */
export const InvestigationObservation = Schema.Struct({
  items: Schema.Array(InvestigationEvidence),
  limitations: Schema.Array(Schema.String),
  status: Schema.optionalKey(Schema.Literals(["completed", "unavailable", "invalid"])),
  resultCount: Schema.optionalKey(Schema.NullOr(Schema.Natural)),
});
export type InvestigationObservation = typeof InvestigationObservation.Type;

export const InvestigationCall = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  params: Schema.Json,
});
export type InvestigationCall = typeof InvestigationCall.Type;

export const InvestigationActivity = Schema.Struct({
  operation: Schema.String,
  round: Schema.Natural,
  startedAt: Schema.String,
  completedAt: Schema.String,
  status: Schema.Literals(["completed", "unavailable", "invalid"]),
  resultCount: Schema.NullOr(Schema.Natural),
});

/** Business progress, including the exact model conversation and each committed tool result. */
export const InvestigationCheckpoint = Schema.Struct({
  version: Schema.Literal("2"),
  id: Schema.NonEmptyString,
  definitionVersion: Schema.NonEmptyString,
  phase: Schema.Literals(["researching", "reading", "reporting", "completed"]),
  history: Schema.String,
  evidence: InvestigationObservation,
  calls: Schema.Array(InvestigationCall),
  cursor: Schema.Natural,
  round: Schema.Natural,
  corrections: Schema.Natural,
  remainingMs: Schema.Natural,
  retainedBytes: Schema.Natural,
  activity: Schema.Array(InvestigationActivity),
  updatedAt: Schema.String,
  output: Schema.NullOr(Schema.JsonObject),
}).check(
  Schema.makeFilter((checkpoint) => {
    if (checkpoint.phase === "reading" && checkpoint.cursor >= checkpoint.calls.length)
      return { path: ["cursor"], issue: "Reading requires a pending tool call" };
    if (checkpoint.phase !== "reading" && (checkpoint.cursor !== 0 || checkpoint.calls.length > 0))
      return { path: ["calls"], issue: "Only a reading checkpoint may contain pending calls" };
    if ((checkpoint.phase === "completed") !== (checkpoint.output !== null))
      return { path: ["output"], issue: "Only a completed investigation has an output" };
  }),
);
export type InvestigationCheckpoint = typeof InvestigationCheckpoint.Type;

export class InvestigationError extends Schema.TaggedError<InvestigationError>()(
  "InvestigationError",
  { code: Schema.String, cause: Schema.Defect() },
) {}
