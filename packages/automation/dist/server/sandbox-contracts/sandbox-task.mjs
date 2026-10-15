import { SandboxTaskId, TurnId } from "./ids.mjs";
import { SandboxTaskLaunchAssociation } from "./sandbox-task-admission.mjs";
import { Schema } from "effect";
//#region src/server/sandbox-contracts/sandbox-task.ts
const SandboxTaskSavedCheckpoint = Schema.Struct({
	repositoryName: Schema.NonEmptyString,
	artifactsRepositoryId: Schema.NonEmptyString,
	commitSha: Schema.NonEmptyString,
	baseHead: Schema.NonEmptyString,
	turnId: TurnId,
	sequence: Schema.Natural
});
/** Only the authoritative fields required to validate a repair continuation cross this port. */
const SandboxTaskDetail = Schema.Struct({
	id: SandboxTaskId,
	requestFingerprint: Schema.optionalKey(Schema.NonEmptyString),
	workspaceId: Schema.optionalKey(Schema.String),
	launchAssociation: Schema.optionalKey(SandboxTaskLaunchAssociation),
	status: Schema.Literals([
		"ready",
		"running",
		"idle",
		"failed",
		"destroyed"
	]),
	activeTurnId: Schema.NullOr(TurnId),
	savedCheckpoint: Schema.optionalKey(SandboxTaskSavedCheckpoint),
	latestSequence: Schema.Natural
});
//#endregion
export { SandboxTaskDetail, SandboxTaskSavedCheckpoint };
