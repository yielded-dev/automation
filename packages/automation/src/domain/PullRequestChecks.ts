import { Schema } from "effect";

import { RepairSourceRevision } from "./launch";

export const PullRequestCheckState = Schema.Literals(["pending", "passed", "failed", "skipped"]);
/** Disposable GitHub observations, independent of saved candidate validation receipts. */
export const PullRequestChecks = Schema.Struct({
  headSha: RepairSourceRevision,
  fetchedAt: Schema.DateTimeUtcFromString,
  complete: Schema.Boolean,
  items: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      name: Schema.String,
      state: PullRequestCheckState,
      description: Schema.String,
      descriptionTruncated: Schema.optionalKey(Schema.Boolean),
      hasReviewFindings: Schema.optionalKey(Schema.Boolean),
    }),
  ),
});
export type PullRequestChecks = typeof PullRequestChecks.Type;
