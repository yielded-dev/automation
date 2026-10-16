import { Context, DateTime, Effect, Layer, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { ConflictError } from "../domain/errors";
import { SandboxTaskMachineCreate } from "../server/sandbox-contracts/sandbox-task-machine";
import { SandboxTaskSavedCheckpoint } from "../server/sandbox-contracts/sandbox-task";
import { SandboxTaskLaunchReceipt } from "../server/sandbox-contracts/sandbox-task-admission";
import { SandboxTaskEvent } from "../server/sandbox-contracts/sandbox-task-event";
import { TurnId } from "../server/sandbox-contracts/ids";

export const RepairSandboxState = Schema.Struct({
  request: SandboxTaskMachineCreate,
  fingerprint: Schema.String,
  createdAtMillis: Schema.Natural,
  status: Schema.Literals(["admitted", "accepted", "running", "succeeded", "failed", "cancelled"]),
  cancelled: Schema.Boolean,
  launchReceipt: Schema.NullOr(SandboxTaskLaunchReceipt),
  savedCheckpoint: Schema.NullOr(SandboxTaskSavedCheckpoint),
  latestSequence: Schema.Natural,
});
export type RepairSandboxState = typeof RepairSandboxState.Type;
const StateJson = Schema.fromJsonString(RepairSandboxState);
const Json = Schema.fromJsonString(Schema.Json);

const make = Effect.gen(function* () {
  const sql = (yield* SqlClient.SqlClient).withoutTransforms();
  // These tables recover execution, native dispatch and output after object eviction.
  yield* sql`CREATE TABLE IF NOT EXISTS repair_sandbox_execution (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1), state TEXT NOT NULL
  )`;
  yield* sql`CREATE TABLE IF NOT EXISTS repair_sandbox_events (
    sequence INTEGER PRIMARY KEY, turn_id TEXT, kind TEXT NOT NULL, method TEXT NOT NULL,
    payload TEXT NOT NULL, bytes INTEGER NOT NULL, created_at TEXT NOT NULL
  )`;
  yield* sql`CREATE TABLE IF NOT EXISTS repair_sandbox_cancellations (
    command_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL
  )`;
  yield* sql`CREATE TABLE IF NOT EXISTS sandbox_task_native_terminal (
    purpose TEXT NOT NULL, process_id TEXT NOT NULL, metadata TEXT NOT NULL,
    stdout TEXT NOT NULL, stderr TEXT NOT NULL, PRIMARY KEY (purpose, process_id)
  )`;

  const read = Effect.gen(function* () {
    const row = (yield* sql<{
      state: string;
    }>`SELECT state FROM repair_sandbox_execution WHERE singleton = 1`)[0];
    return row === undefined ? undefined : yield* Schema.decodeEffect(StateJson)(row.state);
  });
  const required = read.pipe(
    Effect.flatMap((state) =>
      state === undefined
        ? ConflictError.make({ message: "The repair execution has not been admitted" })
        : Effect.succeed(state),
    ),
  );
  const put = (state: RepairSandboxState) =>
    Effect.gen(function* () {
      const encoded = yield* Schema.encodeEffect(StateJson)(state);
      yield* sql`INSERT INTO repair_sandbox_execution (singleton, state) VALUES (1, ${encoded})
      ON CONFLICT (singleton) DO UPDATE SET state = excluded.state`;
      return state;
    });
  const update = (change: (state: RepairSandboxState) => RepairSandboxState) =>
    sql.withTransaction(
      Effect.gen(function* () {
        return yield* put(change(yield* required));
      }),
    );
  const accept = (
    request: SandboxTaskMachineCreate,
    fingerprint: string,
    createdAtMillis: number,
  ) =>
    Effect.gen(function* () {
      const previous = yield* read;
      if (previous !== undefined) {
        if (
          previous.fingerprint !== fingerprint ||
          !Schema.toEquivalence(SandboxTaskMachineCreate)(previous.request, request)
        )
          return yield* ConflictError.make({
            message: "The repair execution already has different immutable inputs",
          });
        return previous;
      }
      return yield* put({
        request,
        fingerprint,
        createdAtMillis,
        status: "admitted",
        cancelled: false,
        launchReceipt: null,
        savedCheckpoint: null,
        latestSequence: 0,
      });
    });
  const append = (
    turnId: string | null,
    kind: "runtime" | "codex",
    method: string,
    payload: Schema.Json,
  ) =>
    sql.withTransaction(
      Effect.gen(function* () {
        const state = yield* required;
        let encoded = yield* Schema.encodeEffect(Json)(payload);
        let bytes = new TextEncoder().encode(encoded).byteLength;
        if (bytes > 120_000) {
          method = "repair.output_truncated";
          kind = "runtime";
          encoded = "null";
          bytes = 4;
        }
        const sequence = state.latestSequence + 1;
        yield* sql`INSERT INTO repair_sandbox_events (sequence, turn_id, kind, method, payload, bytes, created_at)
        VALUES (${sequence}, ${turnId}, ${kind}, ${method}, ${encoded}, ${bytes}, ${DateTime.formatIso(yield* DateTime.now)})`;
        yield* put({ ...state, latestSequence: sequence });
        yield* sql`DELETE FROM repair_sandbox_events WHERE sequence IN (
        SELECT sequence FROM (SELECT sequence, SUM(bytes) OVER (ORDER BY sequence DESC) AS retained_bytes,
          ROW_NUMBER() OVER (ORDER BY sequence DESC) AS retained_count FROM repair_sandbox_events)
        WHERE retained_bytes > 8388608 OR retained_count > 2000)`;
        return sequence;
      }),
    );
  const events = (after: number, limit: number) =>
    Effect.gen(function* () {
      const state = yield* required;
      const rows = yield* sql<{
        sequence: number;
        turn_id: string | null;
        kind: "runtime" | "codex";
        method: string;
        payload: string;
        created_at: string;
      }>`SELECT sequence, turn_id, kind, method, payload, created_at
      FROM repair_sandbox_events WHERE sequence > ${after} ORDER BY sequence ASC LIMIT ${limit}`;
      return yield* Effect.forEach(rows, (row) =>
        Effect.gen(function* () {
          return SandboxTaskEvent.make({
            sequence: row.sequence,
            taskId: state.request.id,
            turnId: row.turn_id === null ? null : TurnId.make(row.turn_id),
            kind: row.kind,
            method: row.method,
            payload: yield* Schema.decodeEffect(Json)(row.payload),
            createdAt: row.created_at,
          });
        }),
      );
    });
  const cancel = (commandId: string, fingerprint: string) =>
    Effect.gen(function* () {
      const previous = (yield* sql<{
        fingerprint: string;
      }>`SELECT fingerprint FROM repair_sandbox_cancellations WHERE command_id = ${commandId}`)[0];
      if (previous !== undefined && previous.fingerprint !== fingerprint)
        return yield* ConflictError.make({
          message: "The cancellation identity already has different inputs",
        });
      yield* sql`INSERT INTO repair_sandbox_cancellations (command_id, fingerprint) VALUES (${commandId}, ${fingerprint}) ON CONFLICT DO NOTHING`;
      return yield* update((state) => ({
        ...state,
        cancelled: true,
        status:
          state.status === "succeeded" || state.status === "failed" ? state.status : "cancelled",
      }));
    });
  // Retried phase/recovery messages are visibility, not progress of the accepted work.
  const executionProgress = Effect.map(
    sql<{
      sequence: number;
    }>`SELECT COALESCE(MAX(sequence), 0) AS sequence FROM repair_sandbox_events WHERE kind = 'codex'`,
    (rows) => rows[0]?.sequence ?? 0,
  );
  return { read, required, update, accept, append, events, cancel, executionProgress };
});

export class RepairSandboxTaskStore extends Context.Service<
  RepairSandboxTaskStore,
  Effect.Success<typeof make>
>()("@reve/repairs/RepairSandboxTaskStore") {
  static readonly layer = Layer.effect(this, make);
}
