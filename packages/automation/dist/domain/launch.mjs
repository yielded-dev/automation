import { RepairModelSettings } from "./proof.mjs";
import { RepairRequestKind } from "./RepairRequestKind.mjs";
import { Schema } from "effect";
import { v5 } from "uuid";
//#region src/domain/launch.ts
const Identifier = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256));
const RepairSourceRevision = Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/));
/** JSON tuple encoding prevents ambiguous identities when source keys contain separators. */
const repairIdentity = (...parts) => v5(JSON.stringify(["kommunikasie-repair", ...parts]), v5.URL);
/** Host-selected immutable launch authority, persisted before external submission. */
const RepairLaunchIntent = Schema.Struct({
	/** Frozen routing context, when the originating investigation classified the request. */
	requestKind: Schema.optionalKey(RepairRequestKind),
	rebase: Schema.optionalKey(Schema.Struct({
		pullRequestNumber: Schema.Int.check(Schema.isGreaterThan(0)),
		branch: Schema.NonEmptyString,
		headSha: RepairSourceRevision,
		/** Explicit retries keep their receipt even if the base branch moves. */
		requestId: Schema.optionalKey(Schema.String.check(Schema.isUUID()))
	})),
	continuation: Schema.optionalKey(Schema.Struct({
		runId: Schema.String.check(Schema.isUUID()),
		candidateSha: RepairSourceRevision,
		publication: Schema.NullOr(Schema.Struct({
			attemptId: Schema.String.check(Schema.isUUID()),
			candidateSha: RepairSourceRevision,
			pullRequestNumber: Schema.Natural
		}))
	})),
	seedRepository: Identifier,
	githubRepositoryId: Schema.Natural,
	targetSha: RepairSourceRevision,
	targetRef: Identifier,
	title: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(160)),
	prompt: Schema.NonEmptyString,
	modelSettings: RepairModelSettings,
	evidenceHash: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
	allowedScopes: Schema.Array(Schema.Literals([
		"repository_read",
		"repository_write",
		"evidence_read",
		"validation",
		"host_publication"
	])),
	promptPolicy: Identifier,
	/** Retained only to preserve the identity of launches accepted before CI owned validation. */
	proofPolicy: Schema.optionalKey(Schema.Json)
});
const repairRunIdentities = (attemptId) => ({
	commandId: repairIdentity("dispatch", attemptId),
	runId: repairIdentity("run", attemptId)
});
//#endregion
export { RepairLaunchIntent, RepairSourceRevision, repairIdentity, repairRunIdentities };
