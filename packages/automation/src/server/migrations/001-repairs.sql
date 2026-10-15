-- Apply through the host migration system when creating a new repair-engine database.
-- Existing Repairs deployments already own these tables and must not reapply this file.

CREATE TABLE "repair_executions" (
	"run_id" uuid PRIMARY KEY,
	"attempt_id" uuid NOT NULL,
	"launch" text NOT NULL,
	"state" text NOT NULL,
	"admission_fingerprint" text,
	"candidate_sha" text,
	"progress_revision" integer DEFAULT 0 NOT NULL,
	"updated_at" timestamp with time zone NOT NULL
);

CREATE TABLE "repair_execution_receipts" (
	"run_id" uuid,
	"kind" text,
	"key" text,
	"fingerprint" text NOT NULL,
	"intent" text NOT NULL,
	"result" text,
	"created_at" timestamp with time zone NOT NULL,
	CONSTRAINT "repair_execution_receipts_pkey" PRIMARY KEY("run_id","kind","key")
);

CREATE TABLE "repair_publication" (
	"run_id" uuid PRIMARY KEY,
	"source_head" text NOT NULL,
	"pull_request_url" text,
	"kind" text NOT NULL,
	"intent" text,
	"receipt" text
);

CREATE TABLE "repair_publication_abandonment" (
	"run_id" uuid PRIMARY KEY,
	"request_id" text NOT NULL,
	"intent_hash" text NOT NULL,
	"created_at" bigint NOT NULL
);

CREATE TABLE "repair_source_executions" (
	"id" uuid PRIMARY KEY,
	"authority" text NOT NULL,
	"admission" text NOT NULL,
	"created_at" timestamp with time zone NOT NULL
);

CREATE UNIQUE INDEX "repair_executions_attempt_idx" ON "repair_executions" ("attempt_id");

ALTER TABLE "repair_execution_receipts" ADD CONSTRAINT "repair_execution_receipts_run_id_repair_executions_run_id_fkey" FOREIGN KEY ("run_id") REFERENCES "repair_executions"("run_id");

ALTER TABLE "repair_publication" ADD CONSTRAINT "repair_publication_run_id_repair_executions_run_id_fkey" FOREIGN KEY ("run_id") REFERENCES "repair_executions"("run_id");

ALTER TABLE "repair_publication_abandonment" ADD CONSTRAINT "repair_publication_abandonment_Bj7l2ZyiVJQ4_fkey" FOREIGN KEY ("run_id") REFERENCES "repair_executions"("run_id");
