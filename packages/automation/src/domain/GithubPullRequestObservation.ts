import { Schema } from "effect";

/** Host-owned GitHub status, independent of the immutable publication receipt. */
export const GithubPullRequestObservation = Schema.Struct({
  number: Schema.Int.check(Schema.isGreaterThan(0)),
  state: Schema.Literals(["open", "closed", "merged"]),
  headSha: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/)),
  updatedAt: Schema.String,
  observedAt: Schema.String,
});
export type GithubPullRequestObservation = typeof GithubPullRequestObservation.Type;
