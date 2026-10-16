/** PostgreSQL tables for host-owned Drizzle migrations. Stored names and types
 * match the default Automation schema; importing this module does not connect
 * to a database or apply migrations.
 */
import {
  bigint,
  foreignKey,
  integer,
  pgTable,
  primaryKey,
  text,
  timestamp,
  uniqueIndex,
  uuid,
} from "drizzle-orm/pg-core";

export const repairExecutions = pgTable(
  "repair_executions",
  {
    runId: uuid("run_id").primaryKey(),
    attemptId: uuid("attempt_id").notNull(),
    launch: text("launch").notNull(),
    state: text("state").notNull(),
    admissionFingerprint: text("admission_fingerprint"),
    candidateSha: text("candidate_sha"),
    progressRevision: integer("progress_revision").default(0).notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).notNull(),
  },
  (table) => [uniqueIndex("repair_executions_attempt_idx").on(table.attemptId)],
);

export const repairExecutionReceipts = pgTable(
  "repair_execution_receipts",
  {
    runId: uuid("run_id").notNull(),
    kind: text("kind").notNull(),
    key: text("key").notNull(),
    fingerprint: text("fingerprint").notNull(),
    intent: text("intent").notNull(),
    result: text("result"),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull(),
  },
  (table) => [
    primaryKey({
      name: "repair_execution_receipts_pkey",
      columns: [table.runId, table.kind, table.key],
    }),
    foreignKey({
      name: "repair_execution_receipts_run_id_repair_executions_run_id_fkey",
      columns: [table.runId],
      foreignColumns: [repairExecutions.runId],
    }),
  ],
);

export const repairPublication = pgTable(
  "repair_publication",
  {
    runId: uuid("run_id").primaryKey(),
    sourceHead: text("source_head").notNull(),
    pullRequestUrl: text("pull_request_url"),
    kind: text("kind").notNull(),
    intent: text("intent"),
    receipt: text("receipt"),
  },
  (table) => [
    foreignKey({
      name: "repair_publication_run_id_repair_executions_run_id_fkey",
      columns: [table.runId],
      foreignColumns: [repairExecutions.runId],
    }),
  ],
);

export const repairPublicationAbandonment = pgTable(
  "repair_publication_abandonment",
  {
    runId: uuid("run_id").primaryKey(),
    requestId: text("request_id").notNull(),
    intentHash: text("intent_hash").notNull(),
    createdAt: bigint("created_at", { mode: "number" }).notNull(),
  },
  (table) => [
    foreignKey({
      name: "repair_publication_abandonment_Bj7l2ZyiVJQ4_fkey",
      columns: [table.runId],
      foreignColumns: [repairExecutions.runId],
    }),
  ],
);

export const repairSourceExecutions = pgTable("repair_source_executions", {
  id: uuid("id").primaryKey(),
  authority: text("authority").notNull(),
  admission: text("admission").notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull(),
});
