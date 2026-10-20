import { Schema } from "effect";

const Identifier = Schema.Trimmed.check(Schema.isMinLength(1), Schema.isMaxLength(512));
const Reason = Schema.Trimmed.check(Schema.isMinLength(1), Schema.isMaxLength(2_000));

/** The host scopes source identities and supplies bounded, authorized comparison context. */
export const DeduplicationInput = Schema.Struct({
  id: Identifier,
  scope: Identifier,
  sourceId: Identifier,
  context: Schema.JsonObject,
});
export type DeduplicationInput = typeof DeduplicationInput.Type;

export const DeduplicationCandidate = Schema.Struct({
  id: Identifier,
  sourceId: Identifier,
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
  Schema.Struct({ _tag: Schema.Literal("Distinct"), reason: Schema.NullOr(Reason) }),
  Schema.Struct({
    _tag: Schema.Literal("Duplicate"),
    canonicalId: Identifier,
    matchedBy: Schema.Literals(["identity", "semantic"]),
    reason: Schema.NullOr(Reason),
  }),
]);
export type DeduplicationDecision = typeof DeduplicationDecision.Type;

export const DeduplicationSnapshot = Schema.Union([
  Schema.Struct({
    _tag: Schema.Literal("Retained"),
    decision: DeduplicationDecision,
  }),
  Schema.Struct({
    _tag: Schema.Literal("Pending"),
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
