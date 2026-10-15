import { Cause, Clock, DateTime, Effect, Match, Option, Redacted, Schema } from "effect";
import { Base64 } from "effect/encoding";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/http";
import { SqlClient } from "effect/sql";

import { reportSanitizedCause } from "./RepairErrorReporter";
import { RunConfig } from "./RunConfig";
import { ConflictError } from "../domain/errors";
import { contentHash } from "./hash";
import { RepairPublicationReceipt, type RepairPublicationRequest } from "../domain/proof";
import { repairIdentity } from "../domain/launch";
import { SessionSandbox } from "@yielded/sandbox/SessionSandbox";
import { REPO_DIR } from "@yielded/sandbox/workspace-paths";
import { GithubAppAuth } from "./GithubAppAuth";

import { readRunState, requireLiveRun, authorizePublication } from "./RepairExecutionAuthority";
import { RepairExecutionContext } from "./RepairExecutionContext";
import {
  repairExecutionSnapshot,
  repairHelperSandbox,
  ensureRepairCommit,
  prepareRepairHelperRepository,
} from "./RepairWorkspace";
import {
  classifyPublicationPush,
  PublicationPushFailure,
  publicationPushMessage,
} from "./PublicationPushFailure";
import { RepairPresentation } from "./RepairPresentation";

const GIT_PUSH_TIMEOUT_MILLIS = 120_000;

export interface PublicationContent {
  readonly kind: "repair";
  readonly title: string;
  readonly body: string;
  readonly validationReceiptId?: string;
}
export class PublicationError extends Schema.TaggedError<PublicationError>()("PublicationError", {
  message: Schema.String,
  reason: Schema.optionalKey(PublicationPushFailure.fields.reason),
  diagnostic: PublicationPushFailure.fields.diagnostic,
  status: PublicationPushFailure.fields.status,
}) {}

const isExpectedPublicationFailure = Schema.is(Schema.Union([PublicationError, ConflictError]));

export const isPublicationRejection = (error: unknown) =>
  Schema.is(PublicationError)(error) &&
  (error.reason === "write-access" ||
    error.reason === "branch-conflict" ||
    error.reason === "rejected");

const PublicationIntent = Schema.Struct({
  sourceHead: Schema.String,
  previousHead: Schema.NullOr(Schema.String),
  content: Schema.Struct({
    kind: Schema.Literal("repair"),
    title: Schema.String,
    body: Schema.String,
    validationReceiptId: Schema.optionalKey(Schema.String),
  }),
});
const IntentJson = Schema.fromJsonString(PublicationIntent);
const PublicationJson = Schema.fromJsonString(RepairPublicationReceipt);
const repairLabel = "bot-repair";
const requestLabels = { bug: "bug", feature: "enhancement" } as const;
const GithubLabels = Schema.Array(Schema.Struct({ name: Schema.String }));

export const readPublication = Effect.gen(function* () {
  const runId = yield* RepairExecutionContext;
  const sql = (yield* SqlClient.SqlClient).withoutTransforms();
  const rows = yield* sql<{
    receipt: string | null;
  }>`SELECT receipt FROM repair_publication WHERE run_id = ${runId}`;

  return rows[0]?.receipt === null || rows[0]?.receipt === undefined
    ? undefined
    : yield* Schema.decodeEffect(PublicationJson)(rows[0].receipt);
});

export const readPublicationPending = Effect.gen(function* () {
  const runId = yield* RepairExecutionContext;
  const sql = yield* SqlClient.SqlClient;

  return (
    (yield* sql<{
      pending: boolean;
    }>`SELECT EXISTS(SELECT 1 FROM repair_publication WHERE run_id = ${runId} AND intent IS NOT NULL AND receipt IS NULL
      AND NOT EXISTS (SELECT 1 FROM repair_publication_abandonment WHERE run_id = ${runId})) AS pending`)[0]
      ?.pending === true
  );
});

const GithubPullRequest = Schema.Struct({
  number: Schema.Int,
  title: Schema.String,
  body: Schema.NullOr(Schema.String),
  html_url: RepairPublicationReceipt.fields.url,
  state: Schema.Literals(["open", "closed"]),
  draft: Schema.Boolean,
  labels: GithubLabels,
  head: Schema.Struct({
    sha: Schema.String,
    ref: Schema.String,
    repo: Schema.Struct({ id: Schema.Int }),
  }),
  base: Schema.Struct({ ref: Schema.String, repo: Schema.Struct({ id: Schema.Int }) }),
  user: Schema.Struct({ login: Schema.String }),
});
const GithubRef = Schema.Struct({ object: Schema.Struct({ sha: Schema.String }) });

// Codesmith appends its own footer after publication; it does not change the
// repair-owned evidence. All other body edits still require exact readback.
const codesmithFooterEnd = "<!-- /codesmith:footer -->";
const matchesPublicationBody = (body: string | null, expected: string) =>
  body === expected ||
  (body !== null &&
    body.startsWith(`${expected}\n\n<!-- codesmith:footer -->`) &&
    body.indexOf(codesmithFooterEnd, expected.length) === body.length - codesmithFooterEnd.length);

/** Credentials are used only in a dedicated checkout that never executes repository code. */
export const publishRepository = Effect.fn("RepairPublication.publish")(
  function* (sourceHead: string, content: PublicationContent, reconcileOnly = false) {
    const state = yield* readRunState;

    if (!state || (!reconcileOnly && !(yield* authorizePublication)))
      return yield* ConflictError.make({ message: "Repair publication authority is unavailable" });
    const runId = yield* RepairExecutionContext;
    const sql = (yield* SqlClient.SqlClient).withoutTransforms();
    const previous = (yield* sql<{
      source_head: string;
      kind: string;
      intent: string | null;
      receipt: string | null;
    }>`SELECT source_head, kind, intent, receipt FROM repair_publication WHERE run_id = ${runId}`)[0];

    if (previous !== undefined && previous.kind !== content.kind)
      return yield* PublicationError.make({
        message: "The publication belongs to another workflow",
      });
    const previousIntent =
      previous?.intent === null || previous?.intent === undefined
        ? undefined
        : yield* Schema.decodeEffect(IntentJson)(previous.intent);

    // An acknowledged retry reads its receipt without reopening the external write.
    if (
      previous?.receipt !== undefined &&
      previous.receipt !== null &&
      previous.source_head === sourceHead &&
      previousIntent?.sourceHead === sourceHead &&
      Schema.toEquivalence(PublicationIntent.fields.content)(previousIntent.content, content)
    )
      return yield* Schema.decodeEffect(PublicationJson)(previous.receipt);
    const source = {
      githubRepositoryId: String(state.launch.githubRepositoryId),
      repositoryRef: state.launch.targetRef,
    };
    const config = yield* RunConfig;
    const app = yield* GithubAppAuth;
    const repositoryId = state.launch.githubRepositoryId;
    const installation = yield* app.repositoryInstallation(
      `https://github.com/${state.origin.repository}`,
    );

    if (
      Option.isNone(installation) ||
      String(installation.value.id) !== config.githubInstallationId ||
      installation.value.suspended_at !== null
    )
      return yield* ConflictError.make({
        message: "Repair repository installation does not match",
      });
    const token = yield* app.repositoryWriteToken(repositoryId, config.githubInstallationId, {
      workflows: installation.value.permissions.workflows === "write",
    });
    const bot = yield* app.botCommitter(token);
    const slug = state.origin.repository;
    const owner = slug.split("/")[0];
    const continuation = state.launch.continuation?.publication;
    const branch =
      state.launch.rebase?.branch ??
      `repair/${state.origin.incidentId}-${continuation?.attemptId ?? state.origin.attemptId}`;
    const baseUrl = `https://api.github.com/repos/${slug}`;
    const authorizedClient = (yield* HttpClient.HttpClient).pipe(
      HttpClient.mapRequest((request) =>
        request.pipe(
          HttpClientRequest.bearerToken(token),
          HttpClientRequest.setHeaders({
            accept: "application/vnd.github+json",
            "user-agent": "reve-repairs",
            "x-github-api-version": "2022-11-28",
          }),
        ),
      ),
    );
    const client = authorizedClient.pipe(HttpClient.filterStatusOk);
    const existingPullRequest = client
      .get(`${baseUrl}/pulls`, {
        urlParams: {
          head: `${owner}:${branch}`,
          state: "all",
          per_page: "100",
        },
      })
      .pipe(
        Effect.flatMap(HttpClientResponse.schemaBodyJson(Schema.Array(GithubPullRequest))),
        Effect.flatMap((pulls) => {
          if (pulls.length > 1)
            return Effect.fail(
              PublicationError.make({
                message: "The repair branch has multiple PR histories; publication requires review",
              }),
            );
          const existing = pulls.find((pull) => pull.head.ref === branch);

          return existing?.state === "closed" && !reconcileOnly
            ? Effect.fail(
                PublicationError.make({
                  message: "The repair's pull request was closed. Publication requires review.",
                }),
              )
            : Effect.succeed(existing);
        }),
      );
    let pullRequest = yield* existingPullRequest;

    const expectedPullRequest =
      continuation?.pullRequestNumber ?? state.launch.rebase?.pullRequestNumber;

    if (expectedPullRequest !== undefined && pullRequest?.number !== expectedPullRequest)
      return yield* ConflictError.make({ message: "The original repair PR no longer matches" });

    if (
      pullRequest !== undefined &&
      (pullRequest.user.login !== bot.name ||
        pullRequest.draft ||
        pullRequest.head.repo.id !== repositoryId ||
        pullRequest.base.repo.id !== repositoryId ||
        pullRequest.base.ref !== source.repositoryRef)
    ) {
      return yield* PublicationError.make({
        message:
          "The repair branch is attached to a different PR. Publication was stopped without changing it.",
      });
    }
    const {
      repositoryName: forkName,
      containerSize,
      verifiedHead,
    } = yield* repairExecutionSnapshot;

    if (forkName === null || containerSize === null || verifiedHead !== sourceHead)
      return yield* PublicationError.make({
        message: "The saved repository source is unavailable.",
      });
    const readRemoteHead = authorizedClient.get(`${baseUrl}/git/ref/heads/${branch}`).pipe(
      Effect.flatMap((response) =>
        response.status === 404
          ? Effect.succeed(null)
          : HttpClientResponse.filterStatusOk(response).pipe(
              Effect.flatMap(HttpClientResponse.schemaBodyJson(GithubRef)),
              Effect.map((ref) => ref.object.sha),
            ),
      ),
    );
    const remoteHead = yield* readRemoteHead;
    const expectedHead = continuation?.candidateSha ?? state.launch.rebase?.headSha;

    if (previous === undefined && expectedHead !== undefined && remoteHead !== expectedHead)
      return yield* ConflictError.make({ message: "The repair PR changed before publication" });
    if (expectedHead !== undefined && remoteHead === null)
      return yield* PublicationError.make({ message: "The original repair branch is unavailable" });
    const priorHead =
      previous?.source_head === sourceHead
        ? (previousIntent?.previousHead ?? null)
        : (previous?.source_head ?? expectedHead ?? null);

    if (remoteHead !== null && remoteHead !== previous?.source_head && remoteHead !== priorHead)
      return yield* PublicationError.make({
        message: "The repair branch changed outside its publication intent",
      });
    if (
      previous !== undefined &&
      previous.source_head !== sourceHead &&
      previous.receipt === null &&
      remoteHead !== previous.source_head
    )
      return yield* PublicationError.make({
        message: "The preceding publication has an unresolved external outcome",
      });
    const encodedIntent = yield* Schema.encodeEffect(IntentJson)({
      sourceHead,
      previousHead: priorHead,
      content,
    });
    const publicationHash = yield* contentHash(encodedIntent);

    if (
      reconcileOnly &&
      (previous?.intent !== encodedIntent ||
        pullRequest === undefined ||
        pullRequest.head.sha !== sourceHead ||
        pullRequest.title !== content.title ||
        !matchesPublicationBody(pullRequest.body, content.body))
    )
      return yield* PublicationError.make({
        message: "The saved publication has an unresolved GitHub outcome",
      });
    if (!reconcileOnly && !(yield* authorizePublication))
      return yield* PublicationError.make({ message: "Repair publication authority has expired" });

    // The intent is committed before external mutations. The stable branch and exact
    // source are reconciled on retry, including a lost push or create-PR response.
    if (!reconcileOnly)
      yield* sql`INSERT INTO repair_publication (run_id, source_head, pull_request_url, kind, intent, receipt)
    VALUES (${runId}, ${sourceHead}, NULL, ${content.kind}, ${encodedIntent}, NULL)
    ON CONFLICT (run_id) DO UPDATE SET source_head = excluded.source_head,
      intent = excluded.intent, receipt = NULL,
      pull_request_url = CASE
        WHEN repair_publication.source_head = excluded.source_head
        THEN repair_publication.pull_request_url
        ELSE NULL
      END`;
    if (!reconcileOnly && remoteHead !== sourceHead) {
      const unresolved = yield* repairHelperSandbox("export");

      // Native preparation can outlive a controller poll. Keep this run's helper
      // until retirement so retries recover its accepted preparation receipt.
      const pushed = yield* Effect.gen(function* () {
        const identity = yield* unresolved.ensure({});
        const sandbox = yield* repairHelperSandbox("export", {
          recovery: "exactWorkspaceRequired",
          expectedInstanceId: identity.instanceId,
        });

        return yield* Effect.gen(function* () {
          yield* prepareRepairHelperRepository(
            sourceHead,
            `repository-export:${identity.instanceId}`,
            identity.instanceId,
          );
          // Git rejects a push whose shallow boundary is a candidate the receiver has never seen.
          yield* ensureRepairCommit(sourceHead);
          if (!(yield* authorizePublication))
            return yield* PublicationError.make({
              message: "Repair publication authority has expired",
            });
          const remote = `https://github.com/${slug}.git`;
          const credential = `Basic ${Base64.encode(`x-access-token:${Redacted.value(token)}`)}`;
          const deadlineAt = (yield* Clock.currentTimeMillis) + GIT_PUSH_TIMEOUT_MILLIS;

          const authorization = {
            remote,
            instanceId: identity.instanceId,
            grantId: crypto.randomUUID(),
          };

          yield* Effect.gen(function* () {
            // Clearing must also cover an accepted grant whose acknowledgment was lost.
            yield* sandbox.setRepositoryAuthorization(
              remote,
              credential,
              deadlineAt,
              authorization.grantId,
            );
            const command = yield* sandbox.exec(
              [
                "git",
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "credential.helper=",
                "-c",
                "http.followRedirects=false",
                "push",
                "--porcelain",
                "--no-progress",
                `--force-with-lease=refs/heads/${branch}:${remoteHead ?? ""}`,
                remote,
                `${sourceHead}:refs/heads/${branch}`,
              ],
              {
                cwd: REPO_DIR,
                deadlineAt,
                timeout: Math.max(1, deadlineAt - (yield* Clock.currentTimeMillis)),
                env: {
                  LC_ALL: "C",
                  GIT_CONFIG_NOSYSTEM: "1",
                  GIT_CONFIG_SYSTEM: "/dev/null",
                  GIT_CONFIG_GLOBAL: "/dev/null",
                  GIT_TERMINAL_PROMPT: "0",
                  GIT_ALLOW_PROTOCOL: "https",
                  GIT_SSL_NO_VERIFY: "0",
                },
              },
            );
            const output = yield* command.outputText({
              timeout: Math.max(1, deadlineAt - (yield* Clock.currentTimeMillis)),
              maxBytes: 64 * 1_024,
            });

            if (output.exitCode !== 0 || output.timedOut) {
              const failure = classifyPublicationPush(output);

              return yield* PublicationError.make({
                message: publicationPushMessage(failure.reason, failure.diagnostic),
                ...failure,
              });
            }
          }).pipe(Effect.onExit(() => sandbox.clearRepositoryAuthorization(authorization)));
        }).pipe(Effect.provideService(SessionSandbox, sandbox));
      }).pipe(
        Effect.tapCause((cause) =>
          cause.reasons.every(
            (reason) =>
              Cause.isInterruptReason(reason) ||
              (Cause.isFailReason(reason) && isPublicationRejection(reason.error)),
          )
            ? Effect.void
            : reportSanitizedCause(cause, { operation: "RepairPublication.push" }),
        ),
        Effect.result,
      );
      const confirmedHead = yield* readRemoteHead;

      if (confirmedHead !== sourceHead) {
        if (pushed._tag === "Failure") return yield* Effect.fail(pushed.failure);

        return yield* PublicationError.make({
          message: "GitHub has not confirmed the repair branch at the validated source.",
        });
      }
    }
    if (pullRequest === undefined) {
      if (reconcileOnly || !(yield* authorizePublication))
        return yield* PublicationError.make({
          message: "Repair publication authority has expired",
        });
      const created = yield* client
        .execute(
          HttpClientRequest.post(`${baseUrl}/pulls`).pipe(
            HttpClientRequest.bodyJsonUnsafe({
              title: content.title,
              head: branch,
              base: source.repositoryRef,
              draft: false,
              body: content.body,
            }),
          ),
        )
        .pipe(
          Effect.flatMap(HttpClientResponse.schemaBodyJson(GithubPullRequest)),
          Effect.tapCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.void
              : reportSanitizedCause(cause, {
                  operation: "RepairPublication.writePullRequest",
                }),
          ),
          Effect.result,
        );

      pullRequest = created._tag === "Success" ? created.success : yield* existingPullRequest;
      if (pullRequest === undefined)
        return yield* PublicationError.make({
          message:
            "The repair branch is saved on GitHub, but PR creation could not be confirmed. Retry to recover the existing publication.",
        });
    } else {
      // Re-read after a branch push; the earlier snapshot may name its previous commit.
      pullRequest = yield* existingPullRequest;
    }
    if (
      pullRequest !== undefined &&
      (pullRequest.title !== content.title ||
        !matchesPublicationBody(pullRequest.body, content.body))
    ) {
      if (
        pullRequest.user.login !== bot.name ||
        pullRequest.state !== "open" ||
        pullRequest.head.sha !== sourceHead ||
        pullRequest.head.repo.id !== repositoryId ||
        pullRequest.base.repo.id !== repositoryId ||
        pullRequest.base.ref !== source.repositoryRef
      )
        return yield* PublicationError.make({
          message: "The repair PR changed before its evidence update",
        });
      if (reconcileOnly || !(yield* authorizePublication))
        return yield* PublicationError.make({
          message: "Repair publication authority has expired",
        });
      const updated = yield* client
        .execute(
          HttpClientRequest.patch(`${baseUrl}/pulls/${pullRequest.number}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ title: content.title, body: content.body }),
          ),
        )
        .pipe(
          Effect.flatMap(HttpClientResponse.schemaBodyJson(GithubPullRequest)),
          Effect.tapCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.void
              : reportSanitizedCause(cause, {
                  operation: "RepairPublication.writePullRequest",
                }),
          ),
          Effect.result,
        );

      pullRequest = updated._tag === "Success" ? updated.success : yield* existingPullRequest;
    }
    if (
      pullRequest === undefined ||
      (!reconcileOnly && pullRequest.state !== "open") ||
      pullRequest.draft ||
      pullRequest.head.sha !== sourceHead ||
      pullRequest.head.repo.id !== repositoryId ||
      pullRequest.base.repo.id !== repositoryId ||
      pullRequest.base.ref !== source.repositoryRef ||
      pullRequest.user.login !== bot.name ||
      pullRequest.title !== content.title ||
      !matchesPublicationBody(pullRequest.body, content.body)
    ) {
      return yield* PublicationError.make({
        message: "GitHub has not confirmed an open bot-authored PR for the saved candidate.",
      });
    }
    const requiredLabels = [
      repairLabel,
      ...(state.launch.requestKind ? [requestLabels[state.launch.requestKind]] : []),
    ];
    const hasRequiredLabels = (labels: typeof GithubLabels.Type) =>
      requiredLabels.every((required) => labels.some((label) => label.name === required));

    if (!hasRequiredLabels(pullRequest.labels)) {
      if (reconcileOnly || !(yield* authorizePublication))
        return yield* PublicationError.make({
          message: "Repair publication authority is unavailable to label the PR.",
        });
      const labels = yield* client
        .execute(
          HttpClientRequest.post(`${baseUrl}/issues/${pullRequest.number}/labels`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ labels: requiredLabels }),
          ),
        )
        .pipe(Effect.flatMap(HttpClientResponse.schemaBodyJson(GithubLabels)));

      if (!hasRequiredLabels(labels))
        return yield* PublicationError.make({
          message: "GitHub has not confirmed the repair PR label. Retry publication.",
        });
    }
    const result = RepairPublicationReceipt.make({
      id: repairIdentity("publication", state.runId, sourceHead, publicationHash),
      kind: content.kind,
      sourceHead,
      ...(content.validationReceiptId === undefined
        ? {}
        : { validationReceiptId: content.validationReceiptId }),
      contentHash: publicationHash,
      branch,
      number: pullRequest.number,
      url: pullRequest.html_url,
    });
    const encodedReceipt = yield* Schema.encodeEffect(PublicationJson)(result);

    yield* sql`UPDATE repair_publication SET pull_request_url = ${result.url}, receipt = ${encodedReceipt}
    WHERE run_id = ${runId} AND source_head = ${sourceHead} AND intent = ${encodedIntent}`;

    return result;
  },
  Effect.timeout("5 minutes"),
  Effect.tapCause((cause) =>
    cause.reasons.every(
      (reason) => Cause.isFailReason(reason) && isExpectedPublicationFailure(reason.error),
    )
      ? Effect.void
      : reportSanitizedCause(cause, { operation: "RepairPublication.publish" }),
  ),
  Effect.catchCause((cause) =>
    Effect.failCause(
      Cause.map(cause, (error) =>
        isExpectedPublicationFailure(error)
          ? error
          : PublicationError.make({
              message:
                "Repair PR publication failed. The saved commit is retained. Retry publication; if it fails again, inspect the repair diagnostics.",
            }),
      ),
    ),
  ),
);

/** Cancellation can still read back a previously issued write, but never issues a new one. */
export const reconcilePublication = Effect.gen(function* () {
  const runId = yield* RepairExecutionContext;
  const sql = yield* SqlClient.SqlClient;
  const row = (yield* sql<{
    intent: string;
  }>`SELECT intent FROM repair_publication WHERE run_id = ${runId} AND intent IS NOT NULL AND receipt IS NULL
    AND NOT EXISTS (SELECT 1 FROM repair_publication_abandonment WHERE run_id = ${runId})`)[0];

  if (!row) return;
  const intent = yield* Schema.decodeEffect(IntentJson)(row.intent);

  yield* publishRepository(intent.sourceHead, intent.content, true);
});

/** A stopped run may leave its intent unresolved; defects and interruptions must still escape. */
export const reconcilePublicationAfterStop = reconcilePublication.pipe(
  Effect.catchCause((cause) =>
    cause.reasons.every(
      (reason) => Cause.isFailReason(reason) && isExpectedPublicationFailure(reason.error),
    )
      ? Effect.void
      : Effect.failCause(cause),
  ),
);

/** An operator may abandon an expired/cancelled run's write intent, without erasing it. */
export const abandonPublication = Effect.fn("RepairPublication.abandon")(function* (
  requestId: string,
) {
  const state = yield* readRunState;

  if (!state || state.retirement !== "retired" || state.stopReason === null)
    return yield* ConflictError.make({
      message: "Only a stopped and retired run can abandon publication",
    });
  // Recover a confirmed write first. Abandonment never claims the write failed.
  yield* reconcilePublicationAfterStop;
  const runId = yield* RepairExecutionContext;
  const sql = yield* SqlClient.SqlClient;
  const row = (yield* sql<{ intent: string }>`SELECT intent FROM repair_publication
    WHERE run_id = ${runId} AND intent IS NOT NULL AND receipt IS NULL`)[0];

  if (!row) return;
  const intentHash = yield* contentHash(row.intent);
  const now = DateTime.toEpochMillis(yield* DateTime.now);

  yield* sql`INSERT INTO repair_publication_abandonment (run_id, request_id, intent_hash, created_at)
    VALUES (${runId}, ${requestId}, ${intentHash}, ${now}) ON CONFLICT(run_id) DO NOTHING`;
});

/** Publish the saved candidate; the repository's required CI controls merge eligibility. */
export const publishRepair = Effect.fn("RepairRun.publish")(function* (
  request: RepairPublicationRequest,
) {
  const state = yield* requireLiveRun;

  if (!(yield* authorizePublication))
    return yield* ConflictError.make({ message: "Repair publication authority is unavailable" });
  const origin = state.origin;
  const admittedHead =
    state.launch.continuation?.candidateSha ?? state.launch.rebase?.headSha ?? origin.targetSha;

  if (
    (yield* repairExecutionSnapshot).verifiedHead !== request.candidateSha ||
    request.candidateSha === admittedHead
  )
    return yield* ConflictError.make({ message: "Publication requires the exact saved candidate" });
  const sql = yield* SqlClient.SqlClient;
  const prior = (yield* sql<{ intent: string | null }>`SELECT intent FROM repair_publication
    WHERE run_id = ${state.runId}`)[0];

  // A retry continues the accepted write, including publications accepted before
  // host validation was removed. It cannot replace content or invent a receipt.
  if (prior?.intent !== undefined && prior.intent !== null) {
    const intent = yield* Schema.decodeEffect(IntentJson)(prior.intent);

    if (intent.sourceHead !== request.candidateSha)
      return yield* ConflictError.make({ message: "The saved publication candidate changed" });

    return yield* publishRepository(intent.sourceHead, intent.content);
  }
  const content = yield* (yield* RepairPresentation).publicationContent(state, request);

  return yield* publishRepository(request.candidateSha, content).pipe(
    Effect.timeout(Math.max(1, origin.deadlineAtMs - DateTime.toEpochMillis(yield* DateTime.now))),
  );
});
