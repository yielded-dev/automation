import { Schema } from "effect";

import {
  NativeContainerIdentity,
  NativeFileStat,
  NativeDirectoryEntries,
} from "../server/sandbox-contracts/sandbox-task-native";
import {
  ProcessStatus,
  RepositoryAuthorizationGrant,
  WorkspaceCheckpoint,
} from "@yielded/sandbox/cloudflare";

export const NativeContainerDescription = Schema.Struct({
  imageDigest: Schema.String,
  classIdentity: Schema.String,
  current: Schema.NullOr(NativeContainerIdentity),
  running: Schema.Boolean,
  checkpoint: Schema.NullOr(WorkspaceCheckpoint),
});
export const NativeForwardPrepared = Schema.Struct({
  operationId: Schema.String,
  instanceId: Schema.String,
});
export const NativeOperationResult = Schema.Union([
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
  NativeDirectoryEntries,
]);
export const NativeOperationResultJson = Schema.toCodecJson(NativeOperationResult);
