import { assert, it } from "vite-plus/test";
import { GithubAppAuth } from "../../packages/automation/src/server/GithubAppAuth";
import { DateTime, Effect, Exit, Fiber, Layer, Redacted } from "effect";
import { HttpClient, HttpClientResponse } from "effect/http";
import { TestClock } from "effect/testing";

import { RepairErrorReporter } from "../../packages/automation/src/server/RepairErrorReporter";

import { RepairGithubConfig } from "../../packages/automation/src/server/RepairGithubConfig";
import { RepairGithubClient } from "../../packages/automation/src/server/RepairGithubClient";
// Real GitHub cannot safely/repeatably supply an oversized failing check on demand.
it("retains linked current-head review findings ahead of verbose check output", () => {
  const headSha = "a".repeat(40);
  const finding = "Preserve a newer observation received while the previous wake is leased.";
  const bodyFinding = "Fresh unhealthy state must revive an exhausted observation.";
  const secondFinding = "A second review must not truncate fitting findings from the first.";
  const secondInlineFinding =
    "Keep this short inline finding when another linked review overflows.";
  const responses: Record<string, unknown> = {
    "/repos/fixture/repair/pulls/1": {
      number: 1,
      updated_at: "2026-09-28T12:00:00Z",
      state: "open",
      draft: false,
      merged: false,
      mergeable: true,
      mergeable_state: "unstable",
      title: "Repair",
      body: null,
      additions: 1,
      deletions: 0,
      changed_files: 1,
      user: { login: "repairs[bot]" },
      head: { sha: headSha, ref: "repair/fixture", repo: { id: 1 } },
      base: { sha: "b".repeat(40), ref: "main", repo: { id: 1 } },
    },
    [`/repos/fixture/repair/commits/${headSha}/check-runs`]: {
      total_count: 1,
      check_runs: [
        {
          id: 1,
          name: "Effect Agent review",
          head_sha: headSha,
          status: "completed",
          conclusion: "failure",
          // Regression: https://repairs.reve.build/incidents/a0e0ac80-5c83-5022-b0df-af78ce3bba60
          details_url: "https://github.com/fixture/repair/runs/1",
          output: {
            title: "Changes requested",
            summary:
              "Reviews: https://github.com/fixture/repair/pull/1#pullrequestreview-2 https://github.com/fixture/repair/pull/1#pullrequestreview-3",
            text: "x".repeat(8_100),
          },
        },
      ],
    },
    [`/repos/fixture/repair/commits/${headSha}/status`]: {
      sha: headSha,
      total_count: 0,
      statuses: [],
    },
    "/repos/fixture/repair/pulls/1/reviews/2": {
      commit_id: headSha,
      state: "CHANGES_REQUESTED",
      body: bodyFinding + "b".repeat(8_100),
    },
    "/repos/fixture/repair/pulls/1/reviews/2/comments": [
      { path: "RepairGithubRepository.ts", body: finding },
      { path: "RepairGithubRepository.ts", line: 451, body: "x".repeat(8_100) },
    ],
    "/repos/fixture/repair/pulls/1/reviews/3": {
      commit_id: headSha,
      state: "CHANGES_REQUESTED",
      body: secondFinding,
    },
    "/repos/fixture/repair/pulls/1/reviews/3/comments": [
      { path: "RepairGithubRepository.ts", body: secondInlineFinding },
    ],
  };
  const statusCodes: Record<string, number> = {};
  let captureCount = 0;
  let delayMs = 0;
  let reviewDelayMs = 0;
  const client = HttpClient.make((request, url) => {
    const body = responses[url.pathname + url.search] ?? responses[url.pathname];
    const response =
      body === undefined
        ? Effect.die(new Error("Unexpected GitHub request"))
        : Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              Response.json(body, { status: statusCodes[url.pathname] ?? 200 }),
            ),
          );

    const requestDelay = url.pathname.includes("/reviews/") ? delayMs + reviewDelayMs : delayMs;

    return requestDelay === 0 ? response : response.pipe(Effect.delay(requestDelay));
  });

  return Effect.runPromise(
    Effect.gen(function* () {
      const github = yield* RepairGithubClient;
      const checks = yield* github.checks(
        {
          repository: "fixture/repair",
          repositoryId: 1,
          pullRequestNumber: 1,
        },
        { conflicts: false },
      );

      assert.include(checks.items[0]!.description, finding);
      assert.include(checks.items[0]!.description, bodyFinding);
      assert.isAtMost(checks.items[0]!.description.length, 8_000);
      assert.deepEqual(
        {
          shortBodyRetained: checks.items[0]!.description.includes(secondFinding),
          shortInlineRetained: checks.items[0]!.description.includes(secondInlineFinding),
        },
        { shortBodyRetained: true, shortInlineRetained: true },
      );
      responses["/repos/fixture/repair/pulls/1/reviews/2"] = {
        commit_id: headSha,
        state: "CHANGES_REQUESTED",
        body: "b".repeat(3_600) + bodyFinding,
      };
      responses["/repos/fixture/repair/pulls/1/reviews/2/comments"] = [
        { path: "RepairGithubRepository.ts", line: 450, body: "i".repeat(3_600) + finding },
      ];
      const fitting = yield* github.checks(
        {
          repository: "fixture/repair",
          repositoryId: 1,
          pullRequestNumber: 1,
        },
        { conflicts: false },
      );

      assert.include(fitting.items[0]!.description, bodyFinding);
      assert.include(fitting.items[0]!.description, finding);
      assert.include(fitting.items[0]!.description, secondFinding);
      const laterFinding = "The second page finding must reach recovery.";

      responses["/repos/fixture/repair/pulls/1/reviews/2/comments"] = Array.from(
        { length: 100 },
        () => ({
          path: "x",
          line: null,
          body: ".",
        }),
      );
      responses["/repos/fixture/repair/pulls/1/reviews/2/comments?per_page=100&page=2"] = [
        { path: "x", line: null, body: laterFinding },
      ];
      const paginated = yield* github.checks(
        { repository: "fixture/repair", repositoryId: 1, pullRequestNumber: 1 },
        { conflicts: false },
      );

      assert.include(paginated.items[0]!.description, laterFinding);
      const sixthFinding = "The sixth linked review must reach recovery.";

      responses[`/repos/fixture/repair/commits/${headSha}/check-runs`] = {
        total_count: 6,
        check_runs: Array.from({ length: 6 }, (_, index) => ({
          id: index + 1,
          name: `Review ${index + 1}`,
          head_sha: headSha,
          status: "completed",
          conclusion: [
            "failure",
            "cancelled",
            "timed_out",
            "action_required",
            "stale",
            "startup_failure",
          ][index],
          details_url: `https://github.com/fixture/repair/pull/1#pullrequestreview-${index + 2}`,
          output: { title: "Changes requested", summary: null, text: null },
        })),
      };
      for (let id = 2; id <= 7; id++) {
        responses[`/repos/fixture/repair/pulls/1/reviews/${id}`] = {
          commit_id: headSha,
          state: "CHANGES_REQUESTED",
          body: "",
        };
        responses[`/repos/fixture/repair/pulls/1/reviews/${id}/comments`] = [
          { path: "x", line: null, body: id === 7 ? sixthFinding : `Finding ${id}` },
        ];
      }
      const multiple = yield* github.checks(
        { repository: "fixture/repair", repositoryId: 1, pullRequestNumber: 1 },
        { conflicts: false },
      );
      const edgeFinding = "Keep this entire finding.";
      const edgeBody = "b".repeat(7_990 - edgeFinding.length) + edgeFinding;

      responses[`/repos/fixture/repair/commits/${headSha}/check-runs`] = {
        total_count: 2,
        check_runs: ["action_required", "failure"].map((conclusion, index) => ({
          id: index + 1,
          name: `Review ${index + 1}`,
          head_sha: headSha,
          status: "completed",
          conclusion,
          details_url: "https://github.com/fixture/repair/pull/1#pullrequestreview-2",
          output: { title: "x", summary: null, text: null },
        })),
      };
      responses["/repos/fixture/repair/pulls/1/reviews/2"] = {
        commit_id: headSha,
        state: "CHANGES_REQUESTED",
        body: edgeBody,
      };
      responses["/repos/fixture/repair/pulls/1/reviews/2/comments"] = [];
      const edge = yield* github.checks(
        { repository: "fixture/repair", repositoryId: 1, pullRequestNumber: 1 },
        { conflicts: false },
      );

      responses["/repos/fixture/repair/pulls/1/reviews/2"] = {
        commit_id: headSha,
        state: "CHANGES_REQUESTED",
        body: "",
      };
      const lastPageFinding = "Retain this finding beyond ten full comment pages.";

      for (let page = 1; page <= 11; page++) {
        responses[`/repos/fixture/repair/pulls/1/reviews/2/comments?per_page=100&page=${page}`] =
          page === 11
            ? [{ path: "x", line: null, body: lastPageFinding }]
            : Array.from({ length: 100 }, () => ({ path: "x", line: null, body: "." }));
      }
      const extendedReview = yield* github
        .checks(
          { repository: "fixture/repair", repositoryId: 1, pullRequestNumber: 1 },
          { conflicts: false },
        )
        .pipe(Effect.exit);

      assert.deepStrictEqual(multiple.items.map((item) => item.id).sort(), [
        "check:1",
        "check:2",
        "check:3",
        "check:4",
        "check:5",
        "check:6",
      ]);
      assert.deepEqual(
        {
          everyFailedConclusionRetained: multiple.items.every((item, index) =>
            item.description.includes(index === 5 ? sixthFinding : `Finding ${index + 2}`),
          ),
          checkOutputOverflowMarked:
            "descriptionTruncated" in fitting.items[0]! &&
            fitting.items[0].descriptionTruncated === true,
          fittingReviewIntact: edge.items[1]!.description.includes(edgeBody),
          latePageFindingRetained:
            Exit.isSuccess(extendedReview) &&
            extendedReview.value.items[0]!.description.includes(lastPageFinding),
        },
        {
          everyFailedConclusionRetained: true,
          checkOutputOverflowMarked: true,
          fittingReviewIntact: true,
          latePageFindingRetained: true,
        },
      );
      responses[`/repos/fixture/repair/commits/${headSha}/check-runs`] = {
        total_count: 100,
        check_runs: Array.from({ length: 100 }, (_, index) => ({
          id: index + 1,
          name: `Review ${index}`,
          head_sha: headSha,
          status: "completed",
          conclusion: "failure",
          details_url: `https://github.com/fixture/repair/pull/1#pullrequestreview-${index + 1_000}`,
        })),
      };
      for (let index = 0; index < 100; index++) {
        responses[`/repos/fixture/repair/pulls/1/reviews/${index + 1_000}`] = {
          commit_id: headSha,
          state: index === 0 ? "COMMENTED" : "CHANGES_REQUESTED",
          body: `Finding ${index}`,
        };
        responses[`/repos/fixture/repair/pulls/1/reviews/${index + 1_000}/comments`] = [];
      }
      const dispositions = yield* github.checks(
        { repository: "fixture/repair", repositoryId: 1, pullRequestNumber: 1 },
        { conflicts: false },
      );

      delayMs = 150;
      const timed = yield* github
        .checks(
          { repository: "fixture/repair", repositoryId: 1, pullRequestNumber: 1 },
          { conflicts: false },
        )
        .pipe(Effect.forkChild);

      yield* TestClock.adjust("20 seconds");
      const timedResult = yield* Fiber.await(timed);

      assert.deepEqual(
        {
          commentedFindingRetained: dispositions.items[0]!.description.includes("Finding 0"),
          completedWithinDeadline:
            Exit.isSuccess(timedResult) &&
            timedResult.value.items[99]!.description.includes("Finding 99"),
        },
        { commentedFindingRetained: true, completedWithinDeadline: true },
      );
      delayMs = 0;
      responses[`/repos/fixture/repair/commits/${headSha}/check-runs`] = {
        total_count: 3_000,
        check_runs: Array.from({ length: 3_000 }, (_, index) => ({
          id: index + 1,
          name: `Check ${index}`,
          head_sha: headSha,
          status: "completed",
          conclusion: "failure",
          output: { title: null, summary: null, text: "x".repeat(64) },
        })),
      };
      const oversized = yield* github
        .checks(
          { repository: "fixture/repair", repositoryId: 1, pullRequestNumber: 1 },
          { conflicts: false },
        )
        .pipe(Effect.exit);

      assert.isTrue(
        Exit.isFailure(oversized),
        "an oversized GitHub page must not enter the handoff",
      );
      const reviewPath = "/repos/fixture/repair/pulls/1/reviews/10000";
      const availableOutput = "Fix the current check's actionable failure.";
      const availableBody = "Keep the review body when comments are missing.";

      responses[`/repos/fixture/repair/commits/${headSha}/check-runs`] = {
        total_count: 1,
        check_runs: [
          {
            id: 1,
            name: "Check",
            head_sha: headSha,
            status: "completed",
            conclusion: "failure",
            details_url: "https://github.com/fixture/repair/pull/1#pullrequestreview-10000",
            output: { title: null, summary: null, text: availableOutput + "x".repeat(8_100) },
          },
        ],
      };
      responses[reviewPath] = { message: "Not Found" };
      const capturesBeforeMissing = captureCount;

      statusCodes[reviewPath] = 404;
      const missingReview = yield* github
        .checks(
          { repository: "fixture/repair", repositoryId: 1, pullRequestNumber: 1 },
          { conflicts: false },
        )
        .pipe(Effect.exit);

      responses[reviewPath] = { commit_id: headSha, body: availableBody + "r".repeat(8_100) };
      statusCodes[reviewPath] = 200;
      responses[`${reviewPath}/comments`] = { message: "Not Found" };
      statusCodes[`${reviewPath}/comments`] = 404;
      const missingComments = yield* github
        .checks(
          { repository: "fixture/repair", repositoryId: 1, pullRequestNumber: 1 },
          { conflicts: false },
        )
        .pipe(Effect.exit);

      assert.deepEqual(
        {
          missingReviewKeepsOutput:
            Exit.isSuccess(missingReview) &&
            missingReview.value.items[0]!.description.includes(availableOutput),
          missingEvidenceMarked:
            Exit.isSuccess(missingReview) &&
            missingReview.value.items[0]!.description.includes("unavailable"),
          missingCommentsMarked:
            Exit.isSuccess(missingComments) &&
            missingComments.value.items[0]!.description.includes("unavailable"),
          descriptionsBounded:
            Exit.isSuccess(missingReview) &&
            Exit.isSuccess(missingComments) &&
            missingReview.value.items[0]!.description.length <= 8_000 &&
            missingComments.value.items[0]!.description.length <= 8_000,
          missingCommentsKeepBody:
            Exit.isSuccess(missingComments) &&
            missingComments.value.items[0]!.description.includes(availableBody),
        },
        {
          missingReviewKeepsOutput: true,
          missingEvidenceMarked: true,
          missingCommentsKeepBody: true,
          missingCommentsMarked: true,
          descriptionsBounded: true,
        },
      );
      const missingReviewsQuiet = captureCount === capturesBeforeMissing;
      statusCodes[reviewPath] = 503;
      const unavailableReview = yield* github
        .checks(
          { repository: "fixture/repair", repositoryId: 1, pullRequestNumber: 1 },
          { conflicts: false },
        )
        .pipe(Effect.exit);

      statusCodes[reviewPath] = 200;
      statusCodes[`${reviewPath}/comments`] = 503;
      const unavailableComments = yield* github
        .checks(
          { repository: "fixture/repair", repositoryId: 1, pullRequestNumber: 1 },
          { conflicts: false },
        )
        .pipe(Effect.exit);
      delayMs = 150;
      reviewDelayMs = 11_000;
      const slowReview = yield* github
        .checks(
          { repository: "fixture/repair", repositoryId: 1, pullRequestNumber: 1 },
          { conflicts: true, reviewDeadline: (yield* DateTime.nowAsDate).getTime() + 4_000 },
        )
        .pipe(Effect.timeout("5 seconds"), Effect.forkChild);

      yield* TestClock.adjust("20 seconds");
      const slowResult = yield* Fiber.await(slowReview);

      delayMs = 0;
      reviewDelayMs = 0;
      statusCodes[`${reviewPath}/comments`] = 200;
      responses[`${reviewPath}/comments`] = [];
      const completedRuns = ["failure", "success", "neutral"].map((conclusion, index) => ({
        id: index + 1,
        name: index === 0 ? "CI" : "Review",
        head_sha: headSha,
        status: "completed",
        conclusion,
        ...(index === 0
          ? {}
          : { details_url: "https://github.com/fixture/repair/pull/1#pullrequestreview-10000" }),
        output: { title: null, summary: null, text: "Check result" },
      }));

      responses[`/repos/fixture/repair/commits/${headSha}/check-runs`] = {
        total_count: completedRuns.length,
        check_runs: completedRuns,
      };
      const passedReviews = yield* github.checks(
        { repository: "fixture/repair", repositoryId: 1, pullRequestNumber: 1 },
        { conflicts: false },
      );
      responses[`/repos/fixture/repair/commits/${headSha}/check-runs`] = {
        total_count: 2,
        check_runs: completedRuns.slice(1),
      };
      const prPath = "/repos/fixture/repair/pulls/1";

      responses[prPath] = {
        ...(responses[prPath] as Record<string, unknown>),
        mergeable: null,
        mergeable_state: "unknown",
      };
      // Mergeability retries and serial check reads must not delay known-conflict recovery.
      delayMs = 150;
      const knownConflictRead = yield* github
        .checks(
          { repository: "fixture/repair", repositoryId: 1, pullRequestNumber: 1 },
          { conflicts: true },
        )
        .pipe(Effect.timeout(800), Effect.forkChild);

      yield* TestClock.adjust("10 seconds");
      const knownConflict = yield* Fiber.join(knownConflictRead);

      assert.deepStrictEqual(knownConflict.items.map((item) => item.id).sort(), [
        "check:2",
        "check:3",
      ]);
      assert.deepStrictEqual(passedReviews.items.map((item) => item.id).sort(), [
        "check:1",
        "check:2",
        "check:3",
      ]);
      assert.deepEqual(
        {
          knownConflictRetainsFindings: knownConflict.items.every(
            (item) => item.description.includes(availableBody) && item.hasReviewFindings,
          ),
          missingReviewsQuiet,
          providerFailuresReported: captureCount > capturesBeforeMissing,
          passedReviewsKeepFindings: passedReviews.items
            .slice(1)
            .every((item) => item.description.includes(availableBody) && item.hasReviewFindings),
          reviewFailureKeepsOutput:
            Exit.isSuccess(unavailableReview) &&
            unavailableReview.value.items[0]!.description.includes(availableOutput) &&
            unavailableReview.value.items[0]!.description.includes("unavailable"),
          commentsFailureKeepsBody:
            Exit.isSuccess(unavailableComments) &&
            unavailableComments.value.items[0]!.description.includes(availableBody) &&
            unavailableComments.value.items[0]!.description.includes("unavailable"),
          slowReviewKeepsOutput:
            Exit.isSuccess(slowResult) &&
            slowResult.value.items[0]!.description.includes(availableOutput) &&
            slowResult.value.items[0]!.description.includes("unavailable"),
        },
        {
          knownConflictRetainsFindings: true,
          missingReviewsQuiet: true,
          providerFailuresReported: true,
          passedReviewsKeepFindings: true,
          reviewFailureKeepsOutput: true,
          commentsFailureKeepsBody: true,
          slowReviewKeepsOutput: true,
        },
      );
    }).pipe(
      Effect.provide(
        RepairGithubClient.layer.pipe(
          Layer.provide([
            Layer.succeed(RepairErrorReporter, {
              report: () =>
                Effect.sync(() => {
                  captureCount++;
                }),
            }),
            Layer.succeed(HttpClient.HttpClient, client),
            Layer.mock(GithubAppAuth, {
              availability: "configured",
              repositoryReadToken: () => Effect.succeed(Redacted.make("fixture")),
            }),
            Layer.succeed(RepairGithubConfig, {
              githubInstallationId: "42",
            }),
          ]),
        ),
      ),
      Effect.provide(TestClock.layer()),
      Effect.scoped,
    ),
  );
});
