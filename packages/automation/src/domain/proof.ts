import { Schema } from "effect";

import { RepairSummary, RepairVerification } from "./RepairReport";
import { GithubPullRequestObservation } from "./GithubPullRequestObservation";
const RepositoryCommitSha = Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/));

const Uuid = Schema.String.check(Schema.isUUID());
const Hash = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
const Millis = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

export const RepairRepository = Schema.String.check(
  Schema.isPattern(/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/),
  Schema.isMaxLength(256),
);

/** Immutable lineage and limits selected by incident admission. Contains no private artifact capability. */
export const RepairOrigin = Schema.Struct({
  incidentId: Uuid,
  attemptId: Uuid,
  repository: RepairRepository,
  evidenceManifestId: Uuid,
  evidenceHash: Hash,
  offendingSha: RepositoryCommitSha,
  targetSha: RepositoryCommitSha,
  deadlineAtMs: Millis,
  /** Historical launch bytes remain part of accepted execution identities. */
  proofPolicy: Schema.optionalKey(Schema.Json),
});
export type RepairOrigin = typeof RepairOrigin.Type;

export const RepairPublicationRequest = Schema.Struct({
  candidateSha: RepositoryCommitSha,
  summary: RepairSummary,
  /** Private reported evidence for the host's publicationContent policy, not independent proof. */
  verification: Schema.optionalKey(Schema.NullOr(RepairVerification)),
  /** Defaults to false. The first accepted publication pins this choice for retries. */
  draft: Schema.optionalKey(Schema.Boolean),
});
export type RepairPublicationRequest = typeof RepairPublicationRequest.Type;

export const RepairPublicationReceipt = Schema.Struct({
  id: Uuid,
  kind: Schema.Literal("repair"),
  sourceHead: RepositoryCommitSha,
  /** Kept when reading an already-published receipt; new publications omit it. */
  validationReceiptId: Schema.optionalKey(Schema.String),
  contentHash: Hash,
  branch: Schema.NonEmptyString,
  number: Schema.Int.check(Schema.isGreaterThan(0)),
  url: Schema.String.check(
    Schema.isPattern(/^https:\/\/github\.com\/[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+\/pull\/[0-9]+$/),
  ),
});
export type RepairPublicationReceipt = typeof RepairPublicationReceipt.Type;

/** Provider usage is telemetry; unknown usage never blocks work. */
export const RepairModelUsage = Schema.Struct({
  model: Schema.NonEmptyString,
  serviceTier: Schema.Literals(["default", "priority", "ultrafast"]),
  requestCount: Schema.Natural,
  inputTokens: Schema.NullOr(Schema.Natural),
  outputTokens: Schema.NullOr(Schema.Natural),
});
export type RepairModelUsage = typeof RepairModelUsage.Type;

export const RepairModelSettings = Schema.Struct({
  model: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  reasoningEffort: Schema.NonEmptyString.check(Schema.isMaxLength(64)),
  fast: Schema.Boolean,
});
export const RepairRuntimeInspection = Schema.Struct({
  runId: Uuid,
  origin: RepairOrigin,
  receiptId: Uuid,
  outcome: Schema.Literals(["pending", "running", "completed", "failed", "cancelled"]),
  stopReason: Schema.NullOr(Schema.Literals(["deadline", "authority_revoked", "cancelled"])),
  candidateSha: Schema.NullOr(RepositoryCommitSha),
  modelUsage: Schema.NullOr(RepairModelUsage),
  retirement: Schema.Literals(["active", "draining", "retired"]),
  publication: Schema.NullOr(RepairPublicationReceipt),
  /** Current host observation; the engine may omit it. */
  pullRequest: Schema.optionalKey(Schema.NullOr(GithubPullRequestObservation)),
  publicationPending: Schema.Boolean,
  outputTruncated: Schema.Boolean,
});
export type RepairRuntimeInspection = typeof RepairRuntimeInspection.Type;
