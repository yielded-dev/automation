import { DateTime } from "effect";
import { expect, it } from "vite-plus/test";
import type { RepairLaunchIntent } from "../../packages/automation/src/domain/launch";
import type { GithubPullRequest } from "../../packages/automation/src/domain/RepairGithub";
import { planGithubRecovery } from "../../packages/automation/src/server/planGithubRecovery";

const head = "a".repeat(40);
const base = "b".repeat(40);
const nextBase = "c".repeat(40);
const attemptId = "11111111-1111-4111-8111-111111111111";
const sourceRunId = "22222222-2222-4222-8222-222222222222";
const previousLaunch: RepairLaunchIntent = {
  seedRepository: "fixture/repair",
  githubRepositoryId: 1,
  targetSha: base,
  targetRef: "main",
  title: "Repair",
  prompt: "Repair the reported failure",
  modelSettings: { model: "test", reasoningEffort: "high", fast: true },
  evidenceHash: "d".repeat(64),
  allowedScopes: ["repository_read", "repository_write", "host_publication"],
  promptPolicy: "test",
};
const pr: GithubPullRequest = {
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
  head: { sha: head, ref: "repair/fixture", repo: { id: 1 } },
  base: { sha: nextBase, ref: "main", repo: { id: 1 } },
};
const input = {
  previousLaunch,
  sourceRunId,
  attemptId,
  published: true,
  pr,
  checks: {
    headSha: head,
    fetchedAt: DateTime.makeUnsafe("2026-09-28T12:00:00Z"),
    complete: true,
    items: [{ id: "check:1", name: "Check", state: "failed" as const, description: "failure" }],
  },
  targetSha: nextBase,
  instructions: "Follow repository policy and return the repair report.",
  retainedReport: null,
};

it("pins check recovery to the accepted head/base and keeps retry and publication identities", () => {
  const plan = planGithubRecovery(input);
  expect(plan._tag).toBe("recovery");
  if (plan._tag !== "recovery") return;
  expect(plan.launch.targetSha).toBe(base);
  expect(plan.launch.rebase?.headSha).toBe(head);
  expect(plan.launch.rebase?.requestId).toBe(plan.requestId);
  expect(plan.launch.continuation).toEqual({
    runId: sourceRunId,
    candidateSha: head,
    publication: { attemptId, candidateSha: head, pullRequestNumber: 1 },
  });
  expect(planGithubRecovery({ ...input, targetSha: "e".repeat(40) })).toEqual(plan);
  expect(
    planGithubRecovery({
      ...input,
      checks: { ...input.checks, headSha: "f".repeat(40) },
    }),
  ).toEqual({ _tag: "pending" });
  const conflict = planGithubRecovery({
    ...input,
    checks: undefined,
    pr: { ...pr, mergeable: false, mergeable_state: "dirty" },
  });
  expect(conflict._tag).toBe("recovery");
  if (conflict._tag !== "recovery") return;
  expect(conflict.launch.targetSha).toBe(nextBase);
  expect(conflict.requestId).not.toBe(plan.requestId);
  expect(planGithubRecovery({ ...input, requestId: attemptId })).toMatchObject({
    requestId: attemptId,
  });
});
it("retains fitting failure and successful-review findings within the shared evidence budget", () => {
  const finding = "Retain this finding when short checks leave unused capacity.";
  const review = "Preserve findings linked from a successful review check.";
  const plan = planGithubRecovery({
    ...input,
    checks: {
      ...input.checks,
      items: [
        ...Array.from({ length: 4 }, (_, index) => ({
          id: `check:${index}`,
          name: `Check ${index}`,
          state: "failed" as const,
          description: index === 0 ? finding.padStart(7650, "x") : "failure".padEnd(8000, "x"),
          descriptionTruncated: index === 0,
        })),
        {
          id: "review",
          name: "Review",
          state: "passed",
          description: review,
          hasReviewFindings: true,
        },
      ],
    },
  });
  expect(plan._tag).toBe("recovery");
  if (plan._tag !== "recovery") return;
  expect(plan.launch.prompt).toContain(finding);
  expect(plan.launch.prompt).toContain(review);
  expect(plan.launch.prompt).toContain("[truncated]");
});
