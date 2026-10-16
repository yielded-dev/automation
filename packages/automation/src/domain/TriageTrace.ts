import { RepairRequestKind } from "./RepairRequestKind";
import { Schema } from "effect";

import { InvestigationReport, InvestigationSource } from "./Investigation";

export const TriageFailureKind = Schema.Literals([
  "context_limit",
  "rate_limit",
  "authentication",
  "invalid_output",
  "provider_failure",
  "source_limit",
  "source_unavailable",
  "internal_failure",
  "timeout",
  "interrupted",
]);

export const TriageModelPart = Schema.Union([
  Schema.Struct({ type: Schema.Literals(["text", "summary", "refusal"]), text: Schema.String }),
  Schema.Struct({
    type: Schema.Literal("tool_call"),
    callId: Schema.String,
    name: Schema.String,
    arguments: Schema.String,
  }),
]);
export type TriageModelPart = typeof TriageModelPart.Type;

/** An execution journal contains selected diagnostics, never provider request or error bodies. */
export const TriageTraceData = Schema.Struct({
  operationId: Schema.String.check(Schema.isUUID()),
  kind: Schema.Literals([
    "preparation",
    "investigation",
    "evidence",
    "model",
    "tool",
    "report",
    "decision",
    "context",
  ]),
  status: Schema.Literals(["started", "completed", "failed"]),
  label: Schema.NonEmptyString,
  model: Schema.optionalKey(Schema.String),
  input: Schema.optionalKey(
    Schema.Struct({
      requestKind: Schema.optionalKey(RepairRequestKind),
      query: Schema.optionalKey(Schema.String),
      path: Schema.optionalKey(Schema.String),
      revision: Schema.optionalKey(Schema.String),
      startLine: Schema.optionalKey(Schema.Natural),
      startColumn: Schema.optionalKey(Schema.NullOr(Schema.Natural)),
      endLine: Schema.optionalKey(Schema.NullOr(Schema.Natural)),
      characters: Schema.optionalKey(Schema.Natural),
    }),
  ),
  output: Schema.optionalKey(
    Schema.Struct({
      text: Schema.optionalKey(Schema.String),
      content: Schema.optionalKey(Schema.Array(TriageModelPart)),
      paths: Schema.optionalKey(Schema.Array(Schema.String)),
      source: Schema.optionalKey(InvestigationSource),
      sources: Schema.optionalKey(Schema.Array(InvestigationSource)),
      report: Schema.optionalKey(InvestigationReport),
      characters: Schema.optionalKey(Schema.Natural),
      inputTokens: Schema.optionalKey(Schema.NullOr(Schema.Natural)),
      outputTokens: Schema.optionalKey(Schema.NullOr(Schema.Natural)),
    }),
  ),
  failure: Schema.optionalKey(TriageFailureKind),
  httpStatus: Schema.optionalKey(Schema.Natural),
});
export type TriageTraceData = typeof TriageTraceData.Type;
