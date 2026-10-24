import * as Crypto from "effect/Crypto";
import { sourcePreparationDeadlineMs } from "./limits";
import { DateTime, Effect, Schema } from "effect";
import { SqlClient } from "effect/sql";

import { SandboxTaskMachineCreate } from "./sandbox-contracts/sandbox-task-machine";
import { reportSanitizedCause } from "./RepairErrorReporter";
import { ConflictError } from "../domain/errors";
import { repairIdentity } from "../domain/launch";
import { RunState } from "../domain/RepairExecution";
import { RepairSandboxTaskClient } from "./RepairSandboxTaskClient";

import { remoteRepairSandbox } from "./RemoteSandbox";

const SourcePreparationLease = Schema.Struct({
  owner: Schema.String.check(Schema.isUUID()),
  expiresAtMillis: Schema.Natural,
});

/** The shared source owns this recovery lease; interrupted callers cannot overlap a live transfer. */
export const leaseRepairSourcePreparation = Effect.fn("leaseRepairSourcePreparation")(function* (
  id: string,
  deadlineAtMillis: number,
) {
  const client = yield* RepairSandboxTaskClient;
  const key = "repository-preparation";
  const read = client.native(
    id,
    {
      purpose: "source",
      operationId: yield* (yield* Crypto.Crypto).randomUUIDv4().pipe(Effect.orDie),
      action: { _tag: "JournalGet", key },
    },
    Schema.NullOr(SourcePreparationLease),
  );
  const previous = yield* read;
  const now = DateTime.toEpochMillis(yield* DateTime.now);

  if (previous !== null && previous.expiresAtMillis > now) return undefined;
  const lease: typeof SourcePreparationLease.Type = {
    owner: yield* (yield* Crypto.Crypto).randomUUIDv4().pipe(Effect.orDie),
    expiresAtMillis: Math.min(deadlineAtMillis, now + sourcePreparationDeadlineMs + 30_000),
  };
  const compareAndSet = (expectedValue: typeof lease | null, value: typeof lease | null) =>
    client.native(
      id,
      {
        purpose: "source",
        operationId: crypto.randomUUID(),
        action: { _tag: "JournalPut", key, expectedValue, value },
      },
      Schema.Boolean,
    );

  if (!(yield* compareAndSet(previous, lease))) return undefined;
  yield* Effect.addFinalizer(() =>
    compareAndSet(lease, null).pipe(
      Effect.catchCause((cause) =>
        reportSanitizedCause(cause, { operation: "RepairSource.releaseLease" }),
      ),
      Effect.asVoid,
    ),
  );

  return {
    check: Effect.gen(function* () {
      const observed = yield* read;

      if (
        observed?.owner !== lease.owner ||
        observed.expiresAtMillis <= DateTime.toEpochMillis(yield* DateTime.now)
      )
        return yield* ConflictError.make({ message: "Source preparation lease expired" });
    }),
  };
});

/** Every helper admission is immutable; an expired helper is retired before its receipted retry. */
export const repairSourceSandbox = Effect.fn("repairSourceSandbox")(function* (input: {
  readonly identity: string;
  readonly authority: string;
  readonly deadlineAtMillis: number;
  readonly runId?: string;
}) {
  const sql = (yield* SqlClient.SqlClient).withoutTransforms();
  const client = yield* RepairSandboxTaskClient;
  let id = repairIdentity("source-sandbox", input.identity);
  const liveParent = Effect.gen(function* () {
    if (input.runId === undefined) return;
    const row = (yield* sql<{
      state: string;
    }>`SELECT state FROM repair_executions WHERE run_id = ${input.runId} FOR UPDATE`)[0];
    const state =
      row === undefined
        ? undefined
        : yield* Schema.decodeEffect(Schema.fromJsonString(RunState))(row.state);

    if (
      state === undefined ||
      state.retirement !== "active" ||
      state.stopReason !== null ||
      state.origin.deadlineAtMs <= DateTime.toEpochMillis(yield* DateTime.now)
    )
      return yield* ConflictError.make({ message: "Source preparation parent authority expired" });
  });

  while (true) {
    const create = yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* liveParent;
        if (input.deadlineAtMillis <= DateTime.toEpochMillis(yield* DateTime.now))
          return yield* ConflictError.make({ message: "Source preparation deadline expired" });
        const initial = yield* Schema.decodeEffect(SandboxTaskMachineCreate)({
          id,
          commandId: repairIdentity("source-admission", id),
          title: "Repair source preparation",
          prompt: "Prepare the admitted repository source.",
          execution: "commands",
          source: { _tag: "None" },
          modelSettings: { model: "gpt-6.1-sol", reasoningEffort: "high", fast: true },
          deadlineAtMillis: input.deadlineAtMillis,
          outputSchema: {},
          requiredCommits: [],
        });

        yield* sql`INSERT INTO repair_source_executions (id, authority, admission, created_at)
      VALUES (${id}, ${input.authority}, ${yield* Schema.encodeEffect(Schema.fromJsonString(SandboxTaskMachineCreate))(initial)}, ${yield* DateTime.nowAsDate}) ON CONFLICT DO NOTHING`;
        const row = (yield* sql<{
          authority: string;
          admission: string;
        }>`SELECT authority, admission FROM repair_source_executions WHERE id = ${id} FOR UPDATE`)[0];

        if (row === undefined || row.authority !== input.authority)
          return yield* ConflictError.make({ message: "Source preparation authority changed" });

        return yield* Schema.decodeEffect(Schema.fromJsonString(SandboxTaskMachineCreate))(
          row.admission,
        );
      }),
    );

    if (create.deadlineAtMillis <= DateTime.toEpochMillis(yield* DateTime.now)) {
      yield* client.cancel(id, repairIdentity("cancel-source", id), create);
      id = repairIdentity("source-sandbox-retry", id, create.deadlineAtMillis);
      continue;
    }
    yield* client.create(create);
    yield* sql.withTransaction(liveParent);
    const sandbox = yield* remoteRepairSandbox(id, "source", create.deadlineAtMillis);

    // Native forks need only the durable lease. File operations boot the helper
    // lazily when a pinned Git transfer actually needs pack re-encoding.
    return { taskId: id, sandbox };
  }
});

export const retireRepairSourcePreparations = Effect.fn("retireRepairSourcePreparations")(
  function* (authority: string) {
    const sql = (yield* SqlClient.SqlClient).withoutTransforms();
    const client = yield* RepairSandboxTaskClient;
    const rows = yield* sql<{
      admission: string;
    }>`SELECT admission FROM repair_source_executions WHERE authority = ${authority}`;

    for (const row of rows) {
      const create = yield* Schema.decodeEffect(Schema.fromJsonString(SandboxTaskMachineCreate))(
        row.admission,
      );

      yield* client.cancel(create.id, repairIdentity("cancel-source", create.id), create);
    }
  },
);
