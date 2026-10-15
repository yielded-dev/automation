import { WorkspaceBaseline } from "./WorkspaceBaselineRepository.mjs";
import { Schema } from "effect";
//#region src/cloudflare/WorkspaceBaselinePreparationState.ts
const WorkspaceBaselinePreparationResult = Schema.Struct({
	status: Schema.Literals([
		"prepared",
		"reused",
		"disabled",
		"expired"
	]),
	baseline: Schema.NullOr(WorkspaceBaseline)
});
/** The execution owner retains this receipt before releasing its scratch container. */
const StoredWorkspaceBaselinePreparation = Schema.Struct({
	requestKey: Schema.String,
	/** Retain restore provenance even when the shared registry advances during a retry. */
	base: Schema.optionalKey(Schema.NullOr(WorkspaceBaseline)),
	/** Set before capture; null means the registry key was absent. Never retarget a retry. */
	previousSnapshotId: Schema.optionalKey(Schema.NullOr(Schema.String)),
	result: Schema.NullOr(WorkspaceBaselinePreparationResult)
});
//#endregion
export { StoredWorkspaceBaselinePreparation, WorkspaceBaselinePreparationResult };
