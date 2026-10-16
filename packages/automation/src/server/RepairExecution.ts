import { sourcePreparationDeadlineMs } from "./limits";
import { Cause, DateTime, Effect, Result, Schema } from "effect";
import { SqlClient } from "effect/sql";

import { SandboxTaskMachineCreate } from "./sandbox-contracts/sandbox-task-machine";
import { reportSanitizedCause } from "./RepairErrorReporter";
import { RepairExecutionRepository } from "./database/RepairExecutionRepository";
import { ConflictError } from "../domain/errors";
import { contentHash } from "./hash";
import { RepairRuntimeInspection } from "../domain/proof";
import { repairIdentity } from "../domain/launch";
import { RunLaunch } from "../domain/RepairExecution";
import { RepairReportJson, repairReportOutputSchema } from "../domain/RepairReport";
import { RepairRunReport } from "../domain/RunOutput";
import { RepairSandboxTaskClient } from "./RepairSandboxTaskClient";
import { reconcileRepairNativeStatus } from "./RepairExecutionNativeStatus";
import { SessionSandbox } from "@yielded/sandbox/SessionSandbox";
import { RepositoryGitPackSandboxLive } from "@yielded/sandbox/repository/RepositoryGitPackSandbox";
import {
  RepositoryMutationFence,
  RepositoryMutationFenceFailure,
} from "@yielded/sandbox/repository/RepositoryMutationFence";

import { RepairModel } from "./RepairModel";
import {
  readPublication,
  readPublicationPending,
  reconcilePublicationAfterStop,
} from "./Publication";
import { authorizeRun, authorizeRunState, updateRunState } from "./RepairExecutionAuthority";
import {
  leaseRepairSourcePreparation,
  repairSourceSandbox,
  retireRepairSourcePreparations,
} from "./RepairSourceSandbox";
import { repairExecutionSnapshot, repairHelperSandbox } from "./RepairWorkspace";
import { RepairPresentation } from "./RepairPresentation";
import { prepareRepairSource, readRepairSource } from "./Source";

export const startRepairExecution = Effect.fn("RepairExecution.start")(function* (
  input: RunLaunch,
) {
  const records = yield* RepairExecutionRepository;

  const accepted = yield* records.accept(input);
  // Retry with the accepted representation, including historical policy ordering.
  input = accepted.state;
  const sql = (yield* SqlClient.SqlClient).withoutTransforms();
  const [authorized, [previous]] = yield* Effect.all(
    [
      authorizeRunState(accepted.state),
      sql<{
        intent: string;
      }>`SELECT intent FROM repair_execution_receipts WHERE run_id = ${input.runId} AND kind = 'sandbox-admission' AND key = 'launch'`,
    ],
    { concurrency: 2 },
  );

  if (!authorized)
    return yield* ConflictError.make({ message: "Repair execution authority is unavailable" });
  let request: SandboxTaskMachineCreate;

  if (previous !== undefined)
    request = yield* Schema.decodeEffect(Schema.fromJsonString(SandboxTaskMachineCreate))(
      previous.intent,
    );
  else {
    const preparation = yield* readRepairSource(input);

    if (preparation.missing.length > 0) {
      const prepared = yield* Effect.gen(function* () {
        const { taskId, sandbox } = yield* repairSourceSandbox({
          identity: input.runId,
          runId: input.runId,
          authority: yield* contentHash(accepted.encodedLaunch),
          deadlineAtMillis: input.origin.deadlineAtMs,
        });

        const lease = yield* leaseRepairSourcePreparation(taskId, input.origin.deadlineAtMs);

        if (lease === undefined) return false;
        yield* Effect.addFinalizer(() => sandbox.park.pipe(Effect.catchCause(Effect.die)));
        yield* prepareRepairSource(input, preparation).pipe(
          Effect.provideService(RepositoryMutationFence, {
            check: lease.check.pipe(
              Effect.andThen(authorizeRun),
              Effect.flatMap((authorized) =>
                authorized
                  ? Effect.void
                  : ConflictError.make({ message: "Repair source authority expired" }),
              ),
              Effect.catchCause((cause) =>
                reportSanitizedCause(cause, { operation: "RepairSource.authorize" }).pipe(
                  Effect.andThen(
                    Effect.failCause(
                      Cause.map(cause, (error) =>
                        RepositoryMutationFenceFailure.make({
                          reason: "unavailable",
                          message: "Repair source authority is unavailable",
                          cause: error,
                        }),
                      ),
                    ),
                  ),
                ),
              ),
            ),
          }),
          Effect.provide(RepositoryGitPackSandboxLive),
          Effect.provideService(SessionSandbox, sandbox),
          Effect.timeout(
            Math.max(
              1,
              Math.min(
                sourcePreparationDeadlineMs,
                input.origin.deadlineAtMs - DateTime.toEpochMillis(yield* DateTime.now),
              ),
            ),
          ),
        );

        return true;
      }).pipe(Effect.scoped);

      if (!prepared) return { accepted: false as const };
    }
    const modelSettings = yield* (yield* RepairModel).prepare(input.launch.modelSettings);
    request = yield* Schema.decodeEffect(SandboxTaskMachineCreate)({
      id: input.runId,
      commandId: repairIdentity("sandbox-admission", input.runId),
      title: `Repair ${input.origin.repository}`,
      prompt: input.launch.prompt,
      modelSettings,
      containerSize: "standard",
      source: preparation.source,
      ...(input.launch.continuation === undefined
        ? {}
        : {
            continuation: {
              previousTaskId: input.launch.continuation.runId,
              checkoutHead: input.launch.continuation.candidateSha,
            },
          }),
      repair: {
        repairId: input.origin.incidentId,
        attemptId: input.origin.attemptId,
        repositoryId: input.origin.repository,
        evidenceHash: input.origin.evidenceHash,
        // The deployed task API retains this opaque admission field. CI is the
        // new policy; previously accepted runs keep their original fingerprint.
        proofPolicyHash: yield* contentHash(
          yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(
            input.origin.proofPolicy ?? "github-ci",
          ),
        ),
      },
      deadlineAtMillis: input.origin.deadlineAtMs,
      // oxlint-disable-next-line no-restricted-properties -- JSON Schema generator output is an untyped serialization boundary.
      outputSchema: yield* Schema.decodeUnknownEffect(Schema.Record(Schema.String, Schema.Json))(
        repairReportOutputSchema,
      ),
      requiredCommits: [
        ...new Set([
          input.origin.offendingSha,
          input.origin.targetSha,
          ...(input.launch.rebase ? [input.launch.rebase.headSha] : []),
          ...(input.launch.continuation ? [input.launch.continuation.candidateSha] : []),
        ]),
      ],
    });
    yield* records.retainLaunch(
      input.runId,
      yield* Schema.encodeEffect(Schema.fromJsonString(SandboxTaskMachineCreate))(request),
    );
  }
  // Source preparation can outlive host consent. The frozen intent is visible
  // to retirement before this last authorization and the native start.
  if (!(yield* authorizeRun))
    return yield* ConflictError.make({ message: "Repair execution authority is unavailable" });
  const receipt = yield* (yield* RepairSandboxTaskClient).create(request);

  // Native create durably owns the receipt and run alarm. This projection does
  // not gate coding; an interrupted reply is recovered with the same request.
  yield* records.confirmLaunch(input.runId, receipt);

  return { accepted: true as const };
});

/** A settled native turn is successful only after its exact structured candidate and media are retained. */
export const reconcileRepairExecution = Effect.gen(function* () {
  const state = yield* reconcileRepairNativeStatus;
  if (state.retirement !== "active" || !["pending", "running"].includes(state.outcome))
    return state;
  const snapshot = yield* repairExecutionSnapshot;
  const result = snapshot.codexTurn?.result;

  if (result === undefined || result === null) return state;
  if (snapshot.pipeline === null || snapshot.codexTurn?.id !== snapshot.pipeline.id)
    return yield* ConflictError.make({
      message: "The result belongs to a different accepted turn",
    });
  if (result.terminalEventType !== "turn.completed")
    return yield* updateRunState((current) =>
      current.stopReason === null ? { ...current, outcome: "failed" } : current,
    );
  if (snapshot.pipeline.persisted === null) return state;
  const failed = updateRunState((current) =>
    current.stopReason === null ? { ...current, outcome: "failed" } : current,
  );

  if (result.lastAgentMessage === null || result.lastAgentMessage.length > 131_072)
    return yield* failed;
  const decoded = Schema.decodeResult(RepairReportJson)(result.lastAgentMessage);

  if (Result.isFailure(decoded)) return yield* failed;
  const report = decoded.success;

  if (
    report.candidateSha !== snapshot.verifiedHead ||
    report.candidateSha !== snapshot.pipeline.persisted.head
  )
    return yield* failed;
  const sandbox = yield* repairHelperSandbox("turn", {
    expectedInstanceId: snapshot.pipeline.instanceId,
    recovery: "exactWorkspaceRequired",
  });

  yield* (yield* RepairPresentation)
    .retain(report)
    .pipe(Effect.provideService(SessionSandbox, sandbox));

  return yield* updateRunState((current) =>
    current.stopReason === null ? { ...current, outcome: "completed" } : current,
  );
});
export const inspectRepairExecution = Effect.gen(function* () {
  const state = yield* reconcileRepairExecution;
  const snapshot = yield* repairExecutionSnapshot;
  const candidateSha = snapshot.verifiedHead;

  return RepairRuntimeInspection.make({
    runId: state.runId,
    origin: state.origin,
    outcome: state.outcome,
    stopReason: state.stopReason,
    retirement: state.retirement,
    candidateSha,
    receiptId: repairIdentity(
      "run-receipt",
      state.runId,
      state.outcome,
      state.retirement,
      state.retiredAtMs ?? 0,
    ),
    modelUsage: (yield* (yield* RepairModel).usage(state)) ?? null,
    publication: (yield* readPublication) ?? null,
    publicationPending: yield* readPublicationPending,
    outputTruncated: false,
  });
});
export const reportRepairExecution = Effect.gen(function* () {
  const state = yield* reconcileRepairExecution;
  const snapshot = yield* repairExecutionSnapshot;
  const text = snapshot.codexTurn?.result?.lastAgentMessage;

  return RepairRunReport.make({
    runId: state.runId,
    origin: state.origin,
    outcome: state.outcome,
    stopReason: state.stopReason,
    message:
      text === null || text === undefined
        ? null
        : {
            text: text.slice(0, 131_072),
            phase: "final_answer",
            occurredAt: DateTime.toEpochMillis(yield* DateTime.now),
          },
    outputTruncated: (text?.length ?? 0) > 131_072,
  });
});
export const retireRepairExecution = Effect.fn("RepairExecution.retire")(function* (
  input: RunLaunch,
  reason: "deadline" | "authority_revoked" | "cancelled" | null,
) {
  const accepted = yield* (yield* RepairExecutionRepository).accept(input);

  yield* updateRunState((state) => ({
    ...state,
    retirement: "draining",
    stopReason: state.stopReason ?? reason,
    outcome: ["pending", "running"].includes(state.outcome) ? "cancelled" : state.outcome,
  }));
  const sql = (yield* SqlClient.SqlClient).withoutTransforms();
  const prior = (yield* sql<{
    intent: string;
  }>`SELECT intent FROM repair_execution_receipts WHERE run_id = ${input.runId} AND kind = 'sandbox-admission' AND key = 'launch'`)[0];

  if (prior !== undefined) {
    const admission = yield* Schema.decodeEffect(Schema.fromJsonString(SandboxTaskMachineCreate))(
      prior.intent,
    );

    const cancellation = yield* (yield* RepairSandboxTaskClient).cancel(
      input.runId,
      repairIdentity("cancel-execution", input.runId),
      admission,
      { allowDraining: true },
    );
    // Keep the accepted receipt, source preparations and original deadline until
    // the host confirms compute parking. Existing retirement reconciliation retries.
    if ("_tag" in cancellation && cancellation._tag === "SandboxTaskCancellationDraining")
      return yield* inspectRepairExecution;
  }
  yield* retireRepairSourcePreparations(yield* contentHash(accepted.encodedLaunch));
  const retiredAtMs = DateTime.toEpochMillis(yield* DateTime.now);

  yield* updateRunState((state) => ({ ...state, retirement: "retired", retiredAtMs }));
  yield* reconcilePublicationAfterStop;

  return yield* inspectRepairExecution;
});
