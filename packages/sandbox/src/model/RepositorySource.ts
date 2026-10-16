import { Schema } from "effect";

export const GithubRepositoryBranch = Schema.NonEmptyString.check(
  Schema.isMaxLength(255),
  // JSON Schema consumers cannot rely on JavaScript Unicode property escapes.
  // oxlint-disable-next-line no-control-regex -- Reject the full Unicode Cc range with portable syntax.
  Schema.isPattern(/^[^\u0000-\u001f\u007f-\u009f]+$/u),
  Schema.isPattern(
    /^(?!-)(?!.*(?:\.\.|@\{|\/\/|\.lock(?:\/|$)))(?!.*[ ~^:?*[\\])(?!.*\.$)(?!.*\/$)(?!\/).+$/u,
  ).annotate({
    // OpenAI rejects lookaround; the decoding codec still enforces the full Git rules.
    expected: "a valid Git branch name",
    toJsonSchema: () => ({}),
  }),
);
export const RepositoryCommitSha = Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/u));

/** Host-verified generation-bound repository authority for one turn. */
export interface RepositorySourceAccess {
  readonly repositoryName: string;
  readonly repositoryId: string;
  readonly acceptedSourceSha: string;
  readonly draft?: { readonly sourceSha: string };
}
