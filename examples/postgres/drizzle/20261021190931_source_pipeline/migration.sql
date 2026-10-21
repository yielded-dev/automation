CREATE TABLE "automation_deduplication_inputs" (
	"scope" text,
	"id" text,
	"source_id" text NOT NULL,
	"context" text NOT NULL,
	"decision" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "automation_deduplication_inputs_pkey" PRIMARY KEY("scope","id")
);
--> statement-breakpoint
CREATE TABLE "automation_deduplication_scopes" (
	"scope" text PRIMARY KEY,
	"revision" integer DEFAULT 0 NOT NULL
);
--> statement-breakpoint
CREATE INDEX "automation_deduplication_inputs_source_idx" ON "automation_deduplication_inputs" ("scope","source_id");--> statement-breakpoint
CREATE INDEX "automation_deduplication_inputs_recent_idx" ON "automation_deduplication_inputs" ("scope","created_at" DESC NULLS LAST,"id") WHERE "decision"::jsonb->>'_tag' = 'Distinct';--> statement-breakpoint
ALTER TABLE "automation_deduplication_inputs" ADD CONSTRAINT "automation_deduplication_inputs_D1Az4tjrcxcP_fkey" FOREIGN KEY ("scope") REFERENCES "automation_deduplication_scopes"("scope");