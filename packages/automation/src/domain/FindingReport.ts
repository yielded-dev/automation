import { Schema } from "effect";

/** The short explanation shown first; supporting evidence keeps the technical detail. */
export const FindingReport = Schema.Struct({
  status: Schema.Literals(["explained", "inconclusive"]),
  summary: Schema.NonEmptyString.check(Schema.isMaxLength(360)),
  facts: Schema.Array(Schema.NonEmptyString.check(Schema.isMaxLength(180))).check(
    Schema.isMaxLength(3),
  ),
  missingEvidence: Schema.NullOr(Schema.NonEmptyString.check(Schema.isMaxLength(240))),
  nextStep: Schema.NonEmptyString.check(Schema.isMaxLength(240)),
}).annotate({
  description:
    "The explanation for the person who reported the problem: two short sentences, up to three observed facts, the exact missing evidence if any, and one concrete next step. No revision hashes, source paths, jargon, or speculative lists of causes. An investigation does not verify a repair.",
});

export type FindingReport = typeof FindingReport.Type;
