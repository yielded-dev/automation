import { Effect, Schema } from "effect";

import { SessionSandbox } from "../SessionSandbox";
import { MATERIALIZATION_MARKER, REPO_DIR } from "../workspace-paths";
import {
  SandboxOperationError,
  type SandboxCheckpoint,
  WorkspaceCheckpoint,
} from "./NativeSandbox";

export const RepositoryCheckpointInput = Schema.Struct({
  instanceId: Schema.String,
  setupFingerprint: Schema.String,
  head: Schema.String,
  materializationId: Schema.String,
  snapshotAllowed: Schema.Boolean,
});
export type RepositoryCheckpointInput = typeof RepositoryCheckpointInput.Type;
export const sameRepositoryCheckpointInput = Schema.toEquivalence(RepositoryCheckpointInput);

export const repositoryCheckpoint = (
  checkpoint: SandboxCheckpoint,
  input: RepositoryCheckpointInput,
): WorkspaceCheckpoint => ({
  ...checkpoint,
  setupFingerprint: input.setupFingerprint,
  head: input.head,
  materializationId: input.materializationId,
});

export const recoverRepositoryCheckpoint = (
  checkpoint: SandboxCheckpoint | null,
  input: RepositoryCheckpointInput | null,
): WorkspaceCheckpoint | null =>
  // Older native receipts carried these fields before preparation owned them.
  Schema.is(WorkspaceCheckpoint)(checkpoint)
    ? checkpoint
    : checkpoint !== null && input?.snapshotAllowed && checkpoint.instanceId === input.instanceId
      ? repositoryCheckpoint(checkpoint, input)
      : null;

/** The preparation owner holds the source generation until capture finishes. */
export const captureRepositoryCheckpoint = Effect.fnUntraced(function* (
  input: RepositoryCheckpointInput,
) {
  if (!input.snapshotAllowed) return null;
  const sandbox = yield* SessionSandbox;

  if ((yield* sandbox.listProcesses).some((job) => job.state !== "exited"))
    return yield* new SandboxOperationError({
      operation: "checkpoint",
      cause: new Error("Workspace process outcomes must close before capture"),
    });
  const verified = yield* sandbox.inspect(
    [
      "bash",
      "-euo",
      "pipefail",
      "-c",
      'test "$(cat "$1")" = "$2" && test "$(git -C "$3" rev-parse HEAD)" = "$4"',
      "checkpoint",
      MATERIALIZATION_MARKER,
      input.materializationId,
      REPO_DIR,
      input.head,
    ],
    { timeout: 30_000, maxBytes: 1_024 },
  );

  if (verified.exitCode !== 0 || verified.timedOut || verified.truncated)
    return yield* new SandboxOperationError({
      operation: "checkpoint",
      cause: new Error("Checkpoint source or materialization does not match"),
    });

  const captured = yield* sandbox.checkpoint({ instanceId: input.instanceId, sequence: 0 });

  if (
    Schema.is(WorkspaceCheckpoint)(captured) &&
    (captured.head !== input.head ||
      captured.materializationId !== input.materializationId ||
      captured.setupFingerprint !== input.setupFingerprint)
  )
    return yield* new SandboxOperationError({
      operation: "checkpoint",
      cause: new Error("The retained checkpoint belongs to different preparation inputs"),
    });

  return repositoryCheckpoint(captured, input);
});
