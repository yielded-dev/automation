import { Effect, Exit } from "effect";
import { expect, it } from "vite-plus/test";
import { RunState } from "../../packages/automation/src/domain/RepairExecution";
import { RepairExecutionRepository } from "../../packages/automation/src/server/database/RepairExecutionRepository";
import { RepairExecutionContext } from "../../packages/automation/src/server/RepairExecutionContext";
import { reconcileRepairNativeStatus } from "../../packages/automation/src/server/RepairExecutionNativeStatus";
import { RepairSandboxTaskClient } from "../../packages/automation/src/server/RepairSandboxTaskClient";
import { SandboxTaskId } from "../../packages/automation/src/server/sandbox-contracts/ids";
import { SandboxTaskDetail } from "../../packages/automation/src/server/sandbox-contracts/sandbox-task";

const id = "11111111-1111-4111-8111-111111111111";
const state = RunState.make({
  runId: id,
  outcome: "running",
  stopReason: null,
  retirement: "active",
  retiredAtMs: null,
  origin: {
    incidentId: id,
    attemptId: id,
    evidenceManifestId: id,
    repository: "example/project",
    evidenceHash: "b".repeat(64),
    offendingSha: "a".repeat(40),
    targetSha: "a".repeat(40),
    deadlineAtMs: Date.parse("2030-01-01T00:00:00Z"),
  },
  launch: {
    seedRepository: "example/project",
    githubRepositoryId: 1,
    targetSha: "a".repeat(40),
    targetRef: "main",
    title: "Repair",
    prompt: "Repair the reported failure",
    modelSettings: { model: "test", reasoningEffort: "high", fast: false },
    evidenceHash: "b".repeat(64),
    allowedScopes: [],
    promptPolicy: "test",
  },
});

// Application ports only: no container, Durable Object, alarm or database emulation.
const fixture = (
  options: {
    readonly nativeStatus?: SandboxTaskDetail["status"];
    readonly nativeId?: string;
    readonly nativePresent?: boolean;
    readonly acknowledged?: boolean;
    readonly current?: RunState;
    readonly atTransition?: RunState;
  } = {},
) => {
  let saved = options.current ?? state;
  let inspections = 0;
  let transitions = 0;
  const unused = () =>
    Effect.die("Unexpected operation: reconciliation must not require a Codex snapshot");
  const records = RepairExecutionRepository.of({
    read: () =>
      Effect.succeed({
        state: saved,
        encodedLaunch: "immutable launch",
        candidateSha: null,
        admissionFingerprint: options.acknowledged === false ? null : "accepted fingerprint",
        progressRevision: 0,
      }),
    transition: (runId, change) =>
      Effect.sync(() => {
        expect(runId).toBe(id);
        transitions++;
        saved = change(options.atTransition ?? saved);
        return saved;
      }),
    accept: unused,
    retainLaunch: unused,
    confirmLaunch: unused,
  });
  const client = RepairSandboxTaskClient.of({
    inspect: (runId) =>
      Effect.sync(() => {
        expect(runId).toBe(id);
        inspections++;
        if (options.nativePresent === false) return null;
        return SandboxTaskDetail.make({
          id: SandboxTaskId.make(options.nativeId ?? id),
          requestFingerprint: "accepted fingerprint",
          status: options.nativeStatus ?? "failed",
          activeTurnId: null,
          latestSequence: 0,
        });
      }),
    create: unused,
    snapshot: unused,
    events: unused,
    cancel: unused,
    native: unused,
    forward: unused,
  });
  return {
    run: reconcileRepairNativeStatus.pipe(
      Effect.provideService(RepairExecutionRepository, records),
      Effect.provideService(RepairSandboxTaskClient, client),
      Effect.provideService(RepairExecutionContext, id),
    ),
    counts: () => ({ inspections, transitions }),
  };
};

it("reconciles native failure without a Codex result even when the launch reply was lost", async () => {
  for (const acknowledged of [true, false]) {
    const test = fixture({ acknowledged });
    const result = await Effect.runPromise(test.run);
    expect(result).toEqual({ ...state, outcome: "failed" });
    expect(result.origin).toBe(state.origin);
    expect(result.launch).toBe(state.launch);
    expect(test.counts()).toEqual({ inspections: 1, transitions: 1 });
    await Effect.runPromise(test.run);
    expect(test.counts()).toEqual({ inspections: 1, transitions: 1 });
  }
});

it("preserves running state and distinguishes an actually absent native record", async () => {
  for (const options of [
    { nativeStatus: "running" as const },
    { acknowledged: false, nativePresent: false },
  ]) {
    const test = fixture(options);
    expect(await Effect.runPromise(test.run)).toBe(state);
    expect(test.counts().transitions).toBe(0);
    expect(test.counts().inspections).toBe(1);
  }
});

it("does not overwrite cancellation or retirement concurrent with inspection", async () => {
  for (const atTransition of [
    { ...state, outcome: "cancelled" as const, stopReason: "cancelled" as const },
    { ...state, retirement: "draining" as const },
  ]) {
    expect(await Effect.runPromise(fixture({ atTransition }).run)).toBe(atTransition);
  }
});

it("rejects a native status for a different accepted identity", async () => {
  const test = fixture({ nativeId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" });
  const result = await Effect.runPromise(Effect.exit(test.run));
  expect(Exit.isFailure(result)).toBe(true);
  expect(test.counts().transitions).toBe(0);
});
