import { RepairSummary } from "./RepairReport.mjs";
import { GithubPullRequestObservation } from "./GithubPullRequestObservation.mjs";
import { Schema } from "effect";
//#region src/domain/proof.ts
const RepositoryCommitSha = Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/));
const Uuid = Schema.String.check(Schema.isUUID());
const Hash = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
const Millis = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const RepairRepository = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/), Schema.isMaxLength(256));
/** Immutable lineage and limits selected by incident admission. Contains no private artifact capability. */
const RepairOrigin = Schema.Struct({
	incidentId: Uuid,
	attemptId: Uuid,
	repository: RepairRepository,
	evidenceManifestId: Uuid,
	evidenceHash: Hash,
	offendingSha: RepositoryCommitSha,
	targetSha: RepositoryCommitSha,
	deadlineAtMs: Millis,
	/** Historical launch bytes remain part of accepted execution identities. */
	proofPolicy: Schema.optionalKey(Schema.Json)
});
const RepairPublicationRequest = Schema.Struct({
	candidateSha: RepositoryCommitSha,
	summary: RepairSummary
});
const RepairPublicationReceipt = Schema.Struct({
	id: Uuid,
	kind: Schema.Literal("repair"),
	sourceHead: RepositoryCommitSha,
	/** Kept when reading an already-published receipt; new publications omit it. */
	validationReceiptId: Schema.optionalKey(Schema.String),
	contentHash: Hash,
	branch: Schema.NonEmptyString,
	number: Schema.Int.check(Schema.isGreaterThan(0)),
	url: Schema.String.check(Schema.isPattern(/^https:\/\/github\.com\/[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+\/pull\/[0-9]+$/))
});
/** Provider usage is telemetry; unknown usage never blocks work. */
const RepairModelUsage = Schema.Struct({
	model: Schema.NonEmptyString,
	serviceTier: Schema.Literals([
		"default",
		"priority",
		"ultrafast"
	]),
	requestCount: Schema.Natural,
	inputTokens: Schema.NullOr(Schema.Natural),
	outputTokens: Schema.NullOr(Schema.Natural)
});
const RepairModelSettings = Schema.Struct({
	model: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
	reasoningEffort: Schema.NonEmptyString.check(Schema.isMaxLength(64)),
	fast: Schema.Boolean
});
const RepairRuntimeInspection = Schema.Struct({
	runId: Uuid,
	origin: RepairOrigin,
	receiptId: Uuid,
	outcome: Schema.Literals([
		"pending",
		"running",
		"completed",
		"failed",
		"cancelled"
	]),
	stopReason: Schema.NullOr(Schema.Literals([
		"deadline",
		"authority_revoked",
		"cancelled"
	])),
	candidateSha: Schema.NullOr(RepositoryCommitSha),
	modelUsage: Schema.NullOr(RepairModelUsage),
	retirement: Schema.Literals([
		"active",
		"draining",
		"retired"
	]),
	publication: Schema.NullOr(RepairPublicationReceipt),
	/** Current host observation; the engine may omit it. */
	pullRequest: Schema.optionalKey(Schema.NullOr(GithubPullRequestObservation)),
	publicationPending: Schema.Boolean,
	outputTruncated: Schema.Boolean
});
//#endregion
export { RepairModelSettings, RepairModelUsage, RepairOrigin, RepairPublicationReceipt, RepairPublicationRequest, RepairRepository, RepairRuntimeInspection };
