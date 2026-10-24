import { Effect, Match, Schema } from "effect";

import { RepairSourceRevision } from "./launch";

export { GithubPullRequestObservation } from "./GithubPullRequestObservation";

const Id = Schema.Int.check(Schema.isGreaterThan(0));

export const GithubRepository = Schema.String.check(
  Schema.isPattern(/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/),
);
export const GithubComment = Schema.Struct({
  kind: Schema.Literals(["issue_comment", "pull_request_review_comment", "pull_request_review"]),
  id: Id,
  author: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9-]+(?:\[bot\])?$/)),
  url: Schema.String.check(Schema.isPattern(/^https:\/\/github\.com\//)),
  threadId: Schema.NullOr(Id),
  /** Polling metadata; older accepted webhook work may omit it. */
  createdAt: Schema.optionalKey(Schema.String),
  commitId: Schema.optionalKey(RepairSourceRevision),
});
export type GithubComment = typeof GithubComment.Type;
export const isAutomatedGithubReview = (comment: GithubComment | null) =>
  comment?.kind === "pull_request_review" && comment.author.endsWith("[bot]");
const Scope = {
  repository: GithubRepository,
  repositoryId: Id,
  installationId: Schema.NullOr(Id),
};

export const GithubWork = Schema.Union([
  Schema.TaggedStruct("check", {
    ...Scope,
    pullRequestNumber: Id,
    /** Check notifications may only act on the head that produced them. */
    headSha: Schema.optionalKey(RepairSourceRevision),
    /** Dashboard observations coalesce by head; changed state preserves a wake. */
    observation: Schema.optionalKey(
      Schema.Struct({
        kind: Schema.Literals(["checks", "conflicts"]),
        key: Schema.NonEmptyString,
        queryStartedAt: Schema.Natural,
        receipt: Schema.String.check(Schema.isUUID()),
      }),
    ),
    requestId: Schema.optionalKey(Schema.String.check(Schema.isUUID())),
  }),
  /** A tracked repair PR merged; the host owns downstream resolution. */
  Schema.TaggedStruct("merged", { ...Scope, pullRequestNumber: Id }),
  Schema.TaggedStruct("comment", {
    ...Scope,
    pullRequestNumber: Id,
    comment: GithubComment,
    question: Schema.NonEmptyString.check(Schema.isMaxLength(4_000)),
  }),
]);
export type GithubWork = typeof GithubWork.Type;
const Envelope = {
  repository: Schema.Struct({ id: Id, full_name: GithubRepository }),
  installation: Schema.optionalKey(Schema.Struct({ id: Id })),
};
const Comment = Schema.Struct({
  id: Id,
  body: Schema.String,
  html_url: Schema.String,
  user: Schema.Struct({ login: Schema.String, type: Schema.String }),
  in_reply_to_id: Schema.optionalKey(Id),
  path: Schema.optionalKey(Schema.String),
  diff_hunk: Schema.optionalKey(Schema.String),
});
const CommentEvent = Schema.Struct({
  ...Envelope,
  action: Schema.String,
  comment: Comment,
  issue: Schema.optionalKey(
    Schema.Struct({ number: Id, pull_request: Schema.optionalKey(Schema.Unknown) }),
  ),
  pull_request: Schema.optionalKey(Schema.Struct({ number: Id })),
});
const PushEvent = Schema.Struct({ ...Envelope, ref: Schema.String, deleted: Schema.Boolean });
const CheckEvent = Schema.Struct({ head_sha: RepairSourceRevision });
const CheckRunEvent = Schema.Struct({ ...Envelope, action: Schema.String, check_run: CheckEvent });
const CheckSuiteEvent = Schema.Struct({
  ...Envelope,
  action: Schema.String,
  check_suite: CheckEvent,
});
const StatusEvent = Schema.Struct({
  ...Envelope,
  sha: RepairSourceRevision,
  state: Schema.Literals(["pending", "success", "failure", "error"]),
});
const PullEvent = Schema.Struct({
  ...Envelope,
  action: Schema.String,
  number: Id,
  pull_request: Schema.optionalKey(
    Schema.Struct({
      number: Schema.optionalKey(Id),
      state: Schema.optionalKey(Schema.Literals(["open", "closed"])),
      merged: Schema.optionalKey(Schema.NullOr(Schema.Boolean)),
      updated_at: Schema.optionalKey(Schema.String),
      additions: Schema.optionalKey(Schema.Natural),
      deletions: Schema.optionalKey(Schema.Natural),
      changed_files: Schema.optionalKey(Schema.Natural),
      mergeable: Schema.optionalKey(Schema.NullOr(Schema.Boolean)),
      mergeable_state: Schema.optionalKey(Schema.String),
      head: Schema.optionalKey(
        Schema.Struct({
          sha: Schema.optionalKey(RepairSourceRevision),
          ref: Schema.optionalKey(Schema.String),
          repo: Schema.optionalKey(Schema.NullOr(Schema.Struct({ id: Id }))),
        }),
      ),
      base: Schema.optionalKey(
        Schema.Struct({
          ref: Schema.optionalKey(Schema.String),
          repo: Schema.optionalKey(Schema.NullOr(Schema.Struct({ id: Id }))),
        }),
      ),
    }),
  ),
});
const ReviewEvent = Schema.Struct({
  ...Envelope,
  action: Schema.String,
  pull_request: Schema.Struct({ number: Id }),
  review: Schema.Struct({
    id: Id,
    body: Schema.NullOr(Schema.String),
    user: Schema.Struct({ login: Schema.String, type: Schema.String }),
  }),
});

export const githubConflictStatus = (input: {
  readonly mergeable?: boolean | null;
  readonly mergeable_state?: string;
}): boolean | null => {
  if (input.mergeable_state === "dirty") return true;
  if (
    input.mergeable === true ||
    (input.mergeable_state !== undefined &&
      ["clean", "unstable", "blocked", "behind", "draft", "has_hooks"].includes(
        input.mergeable_state,
      ))
  )
    return false;

  return null;
};

export const boundedGithubEvidence = (text: string, limit: number) => {
  const suffix = "\n[truncated]";

  return text.length <= limit
    ? text
    : limit < suffix.length
      ? "…".slice(0, Math.max(0, limit))
      : text.slice(0, limit - suffix.length) + suffix;
};

export const githubMergeReady = (pr: GithubPullRequest) =>
  pr.state === "open" && !pr.draft && pr.mergeable === true && pr.mergeable_state === "clean";

/** Keep only the event facts needed for execution, never the full webhook payload. */
export const decodeGithubEvent = Effect.fnUntraced(function* (event: string, body: string) {
  return yield* Match.value(event).pipe(
    Match.when(Match.is("check_run", "check_suite", "status"), (kind) =>
      Effect.gen(function* () {
        const input = yield* Match.value(kind).pipe(
          Match.when("check_run", () =>
            Schema.decodeEffect(Schema.fromJsonString(CheckRunEvent))(body).pipe(
              Effect.map((value) => ({
                ...value,
                headSha: value.check_run.head_sha,
                completed: value.action === "completed",
              })),
            ),
          ),
          Match.when("check_suite", () =>
            Schema.decodeEffect(Schema.fromJsonString(CheckSuiteEvent))(body).pipe(
              Effect.map((value) => ({
                ...value,
                headSha: value.check_suite.head_sha,
                completed: value.action === "completed",
              })),
            ),
          ),
          Match.when("status", () =>
            Schema.decodeEffect(Schema.fromJsonString(StatusEvent))(body).pipe(
              Effect.map((value) => ({
                ...value,
                headSha: value.sha,
                completed: value.state !== "pending",
              })),
            ),
          ),
          Match.exhaustive,
        );

        return input.completed
          ? {
              _tag: "checks" as const,
              repository: input.repository.full_name,
              repositoryId: input.repository.id,
              installationId: input.installation?.id ?? null,
              headSha: input.headSha,
            }
          : null;
      }),
    ),
    Match.when("push", () =>
      Schema.decodeEffect(Schema.fromJsonString(PushEvent))(body).pipe(
        Effect.map((input) =>
          input.deleted
            ? null
            : {
                _tag: "push" as const,
                repository: input.repository.full_name,
                repositoryId: input.repository.id,
                installationId: input.installation?.id ?? null,
                ref: input.ref,
              },
        ),
      ),
    ),
    Match.when("pull_request", () =>
      Schema.decodeEffect(Schema.fromJsonString(PullEvent))(body).pipe(
        Effect.map((input) => {
          const pr = input.pull_request;

          return {
            _tag: "pull_request" as const,
            repository: input.repository.full_name,
            repositoryId: input.repository.id,
            installationId: input.installation?.id ?? null,
            pullRequestNumber: input.number,
            action: input.action,
            snapshot: {
              number: pr?.number ?? null,
              state:
                pr?.merged === undefined || pr.merged === null || pr.state === undefined
                  ? ("unavailable" as const)
                  : pr.merged
                    ? pr.state === "closed"
                      ? ("merged" as const)
                      : ("unavailable" as const)
                    : pr.state,
              changes:
                pr?.additions === undefined ||
                pr.deletions === undefined ||
                pr.changed_files === undefined
                  ? null
                  : { additions: pr.additions, deletions: pr.deletions, files: pr.changed_files },
              conflicts: githubConflictStatus({
                mergeable: pr?.mergeable,
                mergeable_state: pr?.mergeable_state,
              }),
              headSha: pr?.head?.sha ?? null,
              headRef: pr?.head?.ref ?? null,
              headRepositoryId: pr?.head?.repo?.id ?? null,
              baseRef: pr?.base?.ref ?? null,
              baseRepositoryId: pr?.base?.repo?.id ?? null,
              updatedAt: pr?.updated_at ?? null,
            },
          };
        }),
      ),
    ),
    Match.when("pull_request_review", () =>
      Schema.decodeEffect(Schema.fromJsonString(ReviewEvent))(body).pipe(
        Effect.flatMap((input) => {
          if (
            input.action !== "submitted" ||
            !["User", "Bot"].includes(input.review.user.type) ||
            !input.review.body?.trim()
          )
            return Effect.succeed(null);

          return Schema.decodeEffect(GithubWork)({
            _tag: "comment",
            repository: input.repository.full_name,
            repositoryId: input.repository.id,
            installationId: input.installation?.id ?? null,
            pullRequestNumber: input.pull_request.number,
            question:
              input.review.body.length > 4_000
                ? `${input.review.body.slice(0, 3_940)}\n[Truncated; see the GitHub review.]`
                : input.review.body,
            comment: {
              kind: "pull_request_review",
              id: input.review.id,
              author: input.review.user.login,
              url: `https://github.com/${input.repository.full_name}/pull/${input.pull_request.number}#pullrequestreview-${input.review.id}`,
              threadId: null,
            },
          });
        }),
      ),
    ),
    Match.when(Match.is("issue_comment", "pull_request_review_comment"), (kind) =>
      Schema.decodeEffect(Schema.fromJsonString(CommentEvent))(body).pipe(
        Effect.flatMap((input) => {
          const number =
            kind === "issue_comment"
              ? input.issue?.pull_request === undefined
                ? undefined
                : input.issue.number
              : input.pull_request?.number;

          if (
            input.action !== "created" ||
            input.comment.user.type !== "User" ||
            number === undefined ||
            !input.comment.body.trim()
          )
            return Effect.succeed(null);
          const question = [
            input.comment.body,
            ...(input.comment.path
              ? [`Review context: ${input.comment.path}\n${input.comment.diff_hunk ?? ""}`]
              : []),
          ].join("\n\n");

          return Schema.decodeEffect(GithubWork)({
            _tag: "comment",
            repository: input.repository.full_name,
            repositoryId: input.repository.id,
            installationId: input.installation?.id ?? null,
            pullRequestNumber: number,
            question:
              question.length > 4_000
                ? `${question.slice(0, 3_940)}\n[Truncated; see the GitHub comment.]`
                : question,
            comment: {
              kind,
              id: input.comment.id,
              author: input.comment.user.login,
              url: `https://github.com/${input.repository.full_name}/pull/${number}#${kind === "issue_comment" ? "issuecomment-" : "discussion_r"}${input.comment.id}`,
              threadId:
                kind === "issue_comment"
                  ? null
                  : (input.comment.in_reply_to_id ?? input.comment.id),
            },
          });
        }),
      ),
    ),
    Match.orElse(() => Effect.succeed(null)),
  );
});

export const GithubPullRequest = Schema.Struct({
  number: Id,
  updated_at: Schema.String,
  state: Schema.Literals(["open", "closed"]),
  draft: Schema.Boolean,
  merged: Schema.Boolean,
  mergeable: Schema.NullOr(Schema.Boolean),
  mergeable_state: Schema.optionalKey(Schema.String),
  title: Schema.String,
  body: Schema.NullOr(Schema.String),
  additions: Schema.Natural,
  deletions: Schema.Natural,
  changed_files: Schema.Natural,
  user: Schema.Struct({ login: Schema.String }),
  head: Schema.Struct({
    sha: RepairSourceRevision,
    ref: Schema.String,
    repo: Schema.NullOr(Schema.Struct({ id: Id })),
  }),
  base: Schema.Struct({
    sha: RepairSourceRevision,
    ref: Schema.String,
    repo: Schema.Struct({ id: Id }),
  }),
});
export type GithubPullRequest = typeof GithubPullRequest.Type;
