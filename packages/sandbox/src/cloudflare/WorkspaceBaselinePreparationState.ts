import { Schema } from "effect";

import { WorkspaceBaseline } from "./WorkspaceBaselineRepository";

export const WorkspaceBaselinePreparationResult = Schema.Struct({
  status: Schema.Literals(["prepared", "reused", "disabled", "expired"]),
  baseline: Schema.NullOr(WorkspaceBaseline),
});
export type WorkspaceBaselinePreparationResult = typeof WorkspaceBaselinePreparationResult.Type;

/** The execution owner retains this receipt before releasing its scratch container. */
export const StoredWorkspaceBaselinePreparation = Schema.Struct({
  requestKey: Schema.String,
  /** Retain restore provenance even when the shared registry advances during a retry. */
  base: Schema.optionalKey(Schema.NullOr(WorkspaceBaseline)),
  /** Set before capture; null means the registry key was absent. Never retarget a retry. */
  previousSnapshotId: Schema.optionalKey(Schema.NullOr(Schema.String)),
  result: Schema.NullOr(WorkspaceBaselinePreparationResult),
});
export type StoredWorkspaceBaselinePreparation = typeof StoredWorkspaceBaselinePreparation.Type;
