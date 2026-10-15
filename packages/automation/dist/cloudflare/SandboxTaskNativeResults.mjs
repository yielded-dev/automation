import { NativeContainerIdentity, NativeDirectoryEntries, NativeFileStat } from "../server/sandbox-contracts/sandbox-task-native.mjs";
import { Schema } from "effect";
import { ProcessStatus, RepositoryAuthorizationGrant, WorkspaceCheckpoint } from "@yielded/sandbox/cloudflare";
//#region src/cloudflare/SandboxTaskNativeResults.ts
const NativeContainerDescription = Schema.Struct({
	imageDigest: Schema.String,
	classIdentity: Schema.String,
	current: Schema.NullOr(NativeContainerIdentity),
	running: Schema.Boolean,
	checkpoint: Schema.NullOr(WorkspaceCheckpoint)
});
const NativeForwardPrepared = Schema.Struct({
	operationId: Schema.String,
	instanceId: Schema.String
});
const NativeOperationResult = Schema.Union([
	Schema.Null,
	Schema.String,
	Schema.Boolean,
	NativeContainerIdentity,
	NativeContainerDescription,
	ProcessStatus,
	Schema.Array(ProcessStatus),
	RepositoryAuthorizationGrant,
	NativeForwardPrepared,
	NativeContainerIdentity,
	NativeFileStat,
	NativeDirectoryEntries
]);
const NativeOperationResultJson = Schema.toCodecJson(NativeOperationResult);
//#endregion
export { NativeContainerDescription, NativeForwardPrepared, NativeOperationResult, NativeOperationResultJson };
