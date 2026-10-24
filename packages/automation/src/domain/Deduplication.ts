import { Schema } from "effect";

const Identifier = Schema.Trimmed.check(Schema.isMinLength(1), Schema.isMaxLength(512));
const Reason = Schema.Trimmed.check(Schema.isMinLength(1), Schema.isMaxLength(2_000));

/** Internal durable decision input, normalized and namespaced by Automation.make. */
export const DeduplicationInput = Schema.Struct({
  id: Identifier,
  scope: Schema.Trimmed.check(Schema.isMinLength(1), Schema.isMaxLength(4096)),
  sourceId: Schema.Trimmed.check(Schema.isMinLength(1), Schema.isMaxLength(4096)),
  context: Schema.JsonObject,
});
export type DeduplicationInput = typeof DeduplicationInput.Type;

export const DeduplicationCandidate = Schema.Struct({
  id: Identifier,
  sourceId: Schema.Trimmed.check(Schema.isMinLength(1), Schema.isMaxLength(4096)),
  context: Schema.JsonObject,
});
export type DeduplicationCandidate = typeof DeduplicationCandidate.Type;

/** A policy may select only a supplied candidate; null explicitly means distinct. */
export const SemanticDeduplicationResult = Schema.Struct({
  duplicateOf: Schema.NullOr(Identifier),
  reason: Reason,
});
export type SemanticDeduplicationResult = typeof SemanticDeduplicationResult.Type;

export const DeduplicationDecision = Schema.Union([
  Schema.TaggedStruct("Distinct", { reason: Schema.NullOr(Reason) }),
  Schema.TaggedStruct("Duplicate", {
    canonicalId: Identifier,
    matchedBy: Schema.Literals(["identity", "semantic"]),
    reason: Schema.NullOr(Reason),
  }),
]);
export type DeduplicationDecision = typeof DeduplicationDecision.Type;

export const DeduplicationSnapshot = Schema.Union([
  Schema.TaggedStruct("Retained", { decision: DeduplicationDecision }),
  Schema.TaggedStruct("Pending", {
    revision: Identifier,
    identityMatch: Schema.NullOr(Identifier),
    candidates: Schema.Array(DeduplicationCandidate),
  }),
]);
export type DeduplicationSnapshot = typeof DeduplicationSnapshot.Type;

export class DeduplicationError extends Schema.TaggedError<DeduplicationError>()(
  "DeduplicationError",
  {
    reason: Schema.Literals(["store", "policy", "invalid_decision", "conflict"]),
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}
