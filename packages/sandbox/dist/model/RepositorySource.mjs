import { Schema } from "effect";
//#region src/model/RepositorySource.ts
const GithubRepositoryBranch = Schema.NonEmptyString.check(Schema.isMaxLength(255), Schema.isPattern(/^[^\u0000-\u001f\u007f-\u009f]+$/u), Schema.isPattern(/^(?!-)(?!.*(?:\.\.|@\{|\/\/|\.lock(?:\/|$)))(?!.*[ ~^:?*[\\])(?!.*\.$)(?!.*\/$)(?!\/).+$/u).annotate({
	expected: "a valid Git branch name",
	toJsonSchema: () => ({})
}));
const RepositoryCommitSha = Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/u));
//#endregion
export { GithubRepositoryBranch, RepositoryCommitSha };
