import { Schema } from "effect";
import { SandboxTaskId, TurnId } from "./ids";
import { SandboxTaskLaunchAssociation } from "./sandbox-task-admission";

export const SandboxTaskSavedCheckpoint = Schema.Struct({
  repositoryName: Schema.NonEmptyString,
  artifactsRepositoryId: Schema.NonEmptyString,
  commitSha: Schema.NonEmptyString,
  baseHead: Schema.NonEmptyString,
  turnId: TurnId,
  sequence: Schema.Natural,
});
export type SandboxTaskSavedCheckpoint = typeof SandboxTaskSavedCheckpoint.Type;

/** Only the authoritative fields required to validate a repair continuation cross this port. */
export const SandboxTaskDetail = Schema.Struct({
  id: SandboxTaskId,
  // A new Worker can reach an older Durable Object during a deployment.
  requestFingerprint: Schema.optionalKey(Schema.NonEmptyString),
  workspaceId: Schema.optionalKey(Schema.String),
  launchAssociation: Schema.optionalKey(SandboxTaskLaunchAssociation),
  status: Schema.Literals(["ready", "running", "idle", "failed", "destroyed"]),
  activeTurnId: Schema.NullOr(TurnId),
  savedCheckpoint: Schema.optionalKey(SandboxTaskSavedCheckpoint),
  latestSequence: Schema.Natural,
});
export type SandboxTaskDetail = typeof SandboxTaskDetail.Type;
