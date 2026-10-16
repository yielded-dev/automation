import { Context, DateTime, Effect, Layer, Schema } from "effect";
import { SqlClient } from "effect/sql";

import type { SandboxTaskLaunchReceipt } from "../sandbox-contracts/sandbox-task-admission";
import { SandboxTaskMachineCreate } from "../sandbox-contracts/sandbox-task-machine";
import { ConflictError } from "../../domain/errors";
import { contentHash } from "../hash";
import { repairIdentity } from "../../domain/launch";
import { RunLaunch, RunState } from "../../domain/RepairExecution";

const LaunchJson = Schema.fromJsonString(RunLaunch);
const StateJson = Schema.fromJsonString(RunState);
const make = Effect.gen(function* () {
  const sql = (yield* SqlClient.SqlClient).withoutTransforms();
  const read = Effect.fn("RepairExecutionRepository.read")(function* (runId: string) {
    const row = (yield* sql<{
      launch: string;
      state: string;
      candidate_sha: string | null;
      admission_fingerprint: string | null;
      progress_revision: number;
    }>`
      SELECT launch, state, candidate_sha, admission_fingerprint, progress_revision FROM repair_executions WHERE run_id = ${runId}`)[0];

    return row === undefined
      ? undefined
      : {
          encodedLaunch: row.launch,
          state: yield* Schema.decodeEffect(StateJson)(row.state),
          candidateSha: row.candidate_sha,
          admissionFingerprint: row.admission_fingerprint,
          progressRevision: row.progress_revision,
        };
  });
  const accept = Effect.fn("RepairExecutionRepository.accept")(function* (launch: RunLaunch) {
    if (
      launch.runId !== repairIdentity("run", launch.origin.attemptId) ||
      launch.launch.evidenceHash !== launch.origin.evidenceHash ||
      launch.launch.targetSha !== launch.origin.targetSha ||
      !Schema.toEquivalence(Schema.Json)(
        launch.launch.proofPolicy ?? null,
        launch.origin.proofPolicy ?? null,
      )
    )
      return yield* ConflictError.make({ message: "Repair launch identity does not match" });
    const encoded = yield* Schema.encodeEffect(LaunchJson)(launch);
    const state: RunState = {
      ...launch,
      outcome: "pending",
      stopReason: null,
      retirement: "active",
      retiredAtMs: null,
    };
    const inserted = (yield* sql<{ run_id: string }>`
      INSERT INTO repair_executions (run_id, attempt_id, launch, state, updated_at)
      VALUES (${launch.runId}, ${launch.origin.attemptId}, ${encoded}, ${yield* Schema.encodeEffect(StateJson)(state)}, ${yield* DateTime.nowAsDate})
      ON CONFLICT DO NOTHING RETURNING run_id`)[0];
    // An immutable insert owns acceptance. A conflicting concurrent insert is
    // committed before the following read; no mutable launch lock is needed.
    const accepted =
      inserted === undefined
        ? yield* read(launch.runId)
        : {
            encodedLaunch: encoded,
            state,
            candidateSha: null,
            admissionFingerprint: null,
            progressRevision: 0,
          };

    // JSONB can reorder historical opaque fields. Compare immutable values,
    // retaining the original bytes as authority for accepted external operations.
    if (
      accepted === undefined ||
      (accepted.encodedLaunch !== encoded &&
        !Schema.toEquivalence(RunLaunch)(
          yield* Schema.decodeEffect(LaunchJson)(accepted.encodedLaunch),
          launch,
        ))
    )
      return yield* ConflictError.make({
        message: "The repair run already has a different immutable launch",
      });

    return accepted;
  });

  const transition = Effect.fn("RepairExecutionRepository.transition")(function* (
    runId: string,
    change: (state: RunState) => RunState,
  ) {
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const row = (yield* sql<{
          state: string;
        }>`SELECT state FROM repair_executions WHERE run_id = ${runId} FOR UPDATE`)[0];

        if (row === undefined)
          return yield* ConflictError.make({ message: "The repair run has no admitted authority" });
        const next = change(yield* Schema.decodeEffect(StateJson)(row.state));

        yield* sql`UPDATE repair_executions SET state = ${yield* Schema.encodeEffect(StateJson)(next)}, updated_at = ${yield* DateTime.nowAsDate} WHERE run_id = ${runId}`;

        return next;
      }),
    );
  });
  const retainLaunch = Effect.fn("RepairExecutionRepository.retainLaunch")(function* (
    runId: string,
    intent: string,
  ) {
    const fingerprint = yield* contentHash(intent);

    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const execution = (yield* sql<{
          state: string;
          fingerprint: string | null;
          intent: string | null;
        }>`WITH execution AS (
          SELECT state FROM repair_executions WHERE run_id = ${runId} FOR UPDATE
        ), inserted AS (
          INSERT INTO repair_execution_receipts (run_id, kind, key, fingerprint, intent, created_at)
          SELECT ${runId}, 'sandbox-admission', 'launch', ${fingerprint}, ${intent}, ${yield* DateTime.nowAsDate}
          FROM execution ON CONFLICT DO NOTHING RETURNING fingerprint, intent
        )
        SELECT execution.state, inserted.fingerprint, inserted.intent
        FROM execution LEFT JOIN inserted ON true`)[0];
        const state =
          execution === undefined
            ? undefined
            : yield* Schema.decodeEffect(StateJson)(execution.state);

        // Validation is inside the transaction, so stopped or malformed state
        // rolls back the insert while retaining the cancellation lock.
        if (
          state === undefined ||
          state.retirement !== "active" ||
          state.stopReason !== null ||
          state.origin.deadlineAtMs <= DateTime.toEpochMillis(yield* DateTime.now)
        )
          return yield* ConflictError.make({ message: "Repair admission authority expired" });
        const row =
          execution !== undefined && execution.fingerprint !== null
            ? execution
            : (yield* sql<{
                fingerprint: string;
                intent: string;
              }>`SELECT fingerprint, intent FROM repair_execution_receipts WHERE run_id = ${runId} AND kind = 'sandbox-admission' AND key = 'launch'`)[0];

        if (row === undefined || row.fingerprint !== fingerprint || row.intent !== intent)
          return yield* ConflictError.make({
            message: "The repair operation already has different immutable inputs",
          });
      }),
    );
  });

  const confirmLaunch = Effect.fn("RepairExecutionRepository.confirmLaunch")(function* (
    runId: string,
    receipt: SandboxTaskLaunchReceipt,
  ) {
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const row = (yield* sql<{
          state: string;
          admission_fingerprint: string | null;
          intent: string;
        }>`SELECT execution.state, execution.admission_fingerprint, launch.intent
          FROM repair_executions execution JOIN repair_execution_receipts launch ON launch.run_id = execution.run_id
          WHERE execution.run_id = ${runId} AND launch.kind = 'sandbox-admission' AND launch.key = 'launch'
          FOR UPDATE OF execution`)[0];

        if (row === undefined)
          return yield* ConflictError.make({
            message: "The repair admission has no frozen launch",
          });
        const request = yield* Schema.decodeEffect(Schema.fromJsonString(SandboxTaskMachineCreate))(
          row.intent,
        );

        if (
          request.id !== runId ||
          receipt.result.id !== runId ||
          receipt.commandId !== request.commandId ||
          (row.admission_fingerprint !== null &&
            row.admission_fingerprint !== receipt.requestFingerprint)
        )
          return yield* ConflictError.make({ message: "The repair launch receipt conflicts" });
        const state = yield* Schema.decodeEffect(StateJson)(row.state);
        const next =
          receipt.result.outcome === "launched" &&
          state.outcome === "pending" &&
          state.stopReason === null &&
          state.retirement === "active"
            ? { ...state, outcome: "running" as const }
            : state;

        // The host's admission fingerprint can include authority outside the raw intent.
        // Restore the native fact after an ambiguous reply, without restoring
        // stopped authority. Native SQLite remains the sole launch-receipt owner.
        yield* sql`UPDATE repair_executions SET admission_fingerprint = ${receipt.requestFingerprint},
          state = ${yield* Schema.encodeEffect(StateJson)(next)}, updated_at = ${yield* DateTime.nowAsDate} WHERE run_id = ${runId}`;
      }),
    );
  });

  return { read, accept, transition, retainLaunch, confirmLaunch };
});

export class RepairExecutionRepository extends Context.Service<
  RepairExecutionRepository,
  Effect.Success<typeof make>
>()("kommunikasie/RepairExecutionRepository") {
  static readonly layer = Layer.effect(this, make);
}
