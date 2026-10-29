import {
  DirectoryBackup,
  type DirectoryBackupGatewayBinding,
  type DirectoryBackupRecord,
  type FileContent,
} from "@cloudflare/sandbox";
import { Clock, Context, Effect, Layer, Schema } from "effect";
import { DurableObjectContainer, DurableObjectState } from "effect-cf";

import { ContainerLifecycle } from "./ContainerLifecycle";
import { ContainerState, CheckpointRecord, error, failureCode } from "./ContainerState";
import {
  CAPACITY_NAME,
  CheckpointInput,
  type EnsureOptions,
  NativeSandboxNamespace,
  type SandboxCheckpoint,
} from "./NativeSandbox";

const SNAPSHOT_TTL = 29 * 24 * 60 * 60_000;

const make = Effect.gen(function* () {
  const container = yield* DurableObjectContainer.DurableObjectContainer;
  const state = yield* DurableObjectState.DurableObjectState;
  const namespace = yield* NativeSandboxNamespace;
  const {
    read,
    identity,
    files,
    readFile,
    readFileString,
    admitted,
    mutateFiles,
    settings: options,
  } = yield* ContainerState;
  const { ensure, protectNativeIO } = yield* ContainerLifecycle;
  const backup = Effect.gen(function* () {
    if (!options.backupBucketBinding)
      return yield* error("backup", "Directory backups are not configured");
    const exports = state.raw.exports as unknown as {
      DirectoryBackupGateway: DirectoryBackupGatewayBinding;
    };

    return new DirectoryBackup(container.raw, exports.DirectoryBackupGateway, {
      binding: options.backupBucketBinding,
      prefix: "backups/",
    });
  });
  const checkpoint = Effect.fn("SandboxContainer.checkpoint")(function* (request: CheckpointInput) {
    const input = yield* Schema.decodeEffect(CheckpointInput)(request);
    const previous = yield* read("workspace-checkpoint", CheckpointRecord);

    if (previous?.input.instanceId === input.instanceId) {
      const sequence = "sequence" in previous.input ? previous.input.sequence : 0;

      if (input.sequence < sequence)
        return yield* error("snapshot", "A newer checkpoint capture has already been accepted");
      if (input.sequence === sequence) {
        if (
          "sequence" in previous.input &&
          (previous.input.name ?? "workspace") !== (input.name ?? "workspace")
        )
          return yield* error(
            "snapshot",
            "This sequence already belongs to a different snapshot name",
          );
        if (previous.result !== null) return previous.result;

        return yield* error("snapshot", "Capture outcome is unknown; snapshot was not repeated");
      }
    }
    const current = yield* identity;

    if (
      !current ||
      current.instanceId !== input.instanceId ||
      current.timings.ownerReadyAt === null ||
      !(yield* container.running)
    )
      return yield* error("snapshot", "No matching running workspace can be captured");
    const now = yield* Clock.currentTimeMillis;

    yield* state.storage.kv.put("workspace-checkpoint", { input, result: null });
    yield* state.storage.sync.pipe(Effect.withSpan("SandboxContainer.snapshotOutputGate"));
    const snapshot = yield* Effect.gen(function* () {
      const requestedAt = yield* Clock.currentTimeMillis;
      const result = yield* container.snapshotContainer({ name: input.name ?? "workspace" });

      yield* Effect.annotateCurrentSpan({
        "sandbox.snapshot.sizeBytes": result.size,
        "sandbox.snapshot.captureMs": (yield* Clock.currentTimeMillis) - requestedAt,
      });

      return result;
    }).pipe(Effect.withSpan("SandboxContainer.nativeSnapshot"));
    const saved: SandboxCheckpoint = {
      kind: "container",
      snapshotId: snapshot.id,
      imageDigest: current.imageDigest,
      classIdentity: (yield* namespace.idFromName(CAPACITY_NAME)).toString(),
      instanceId: current.instanceId,
      capturedAt: now,
      expiresAt: now + SNAPSHOT_TTL,
      name: input.name ?? "workspace",
    };

    yield* state.storage.kv.put("workspace-checkpoint", { input, result: saved });

    return saved;
  });

  return {
    checkpoint: (input: CheckpointInput) => admitted(protectNativeIO(checkpoint(input))),
    exists: (path: string, start: EnsureOptions) =>
      ensure(start).pipe(
        Effect.andThen(files.stat(path)),
        Effect.as(true),
        Effect.catchCauseIf(
          (cause) => failureCode(cause) === "ENOENT",
          () => Effect.succeed(false),
        ),
      ),
    readFileString: (path: string, start: EnsureOptions) =>
      ensure(start).pipe(Effect.andThen(readFileString(path))),
    readFile: (path: string, start: EnsureOptions) =>
      ensure(start).pipe(Effect.andThen(readFile(path))),
    lstat: (path: string, start: EnsureOptions) =>
      ensure(start).pipe(Effect.andThen(files.lstat(path))),
    writeFile: (path: string, content: FileContent, start: EnsureOptions) =>
      admitted(ensure(start).pipe(Effect.andThen(mutateFiles(files.writeFile(path, content))))),
    readDirectory: (path: string, start: EnsureOptions) =>
      ensure(start).pipe(Effect.andThen(files.readDirectory(path))),
    mkdir: (path: string, recursive: boolean, start: EnsureOptions) =>
      admitted(ensure(start).pipe(Effect.andThen(mutateFiles(files.mkdir(path, { recursive }))))),
    remove: (path: string, recursive: boolean, force: boolean, start: EnsureOptions) =>
      admitted(
        ensure(start).pipe(Effect.andThen(mutateFiles(files.remove(path, { recursive, force })))),
      ),
    backupDirectory: (
      input: { readonly dir: string; readonly name?: string },
      start: EnsureOptions,
    ) =>
      admitted(
        ensure(start).pipe(
          Effect.andThen(
            protectNativeIO(
              Effect.gen(function* () {
                const owner = yield* backup;

                return yield* Effect.tryPromise({
                  try: (signal) => owner.backup({ ...input, signal }),
                  catch: (cause) =>
                    new DurableObjectContainer.ContainerError({ operation: "backup", cause }),
                });
              }),
            ),
          ),
        ),
      ),
    restoreDirectory: (receipt: DirectoryBackupRecord, start: EnsureOptions) =>
      admitted(
        ensure(start).pipe(
          Effect.andThen(
            protectNativeIO(
              mutateFiles(
                Effect.gen(function* () {
                  const owner = yield* backup;

                  yield* Effect.tryPromise({
                    try: (signal) => owner.restore(receipt, { signal }),
                    catch: (cause) =>
                      new DurableObjectContainer.ContainerError({
                        operation: "restoreDirectory",
                        cause,
                      }),
                  });
                }),
              ),
            ),
          ),
        ),
      ),
    deleteDirectoryBackup: (receipt: DirectoryBackupRecord) =>
      Effect.gen(function* () {
        const owner = yield* backup;

        yield* Effect.tryPromise({
          try: (signal) => owner.delete(receipt, { signal }),
          catch: (cause) =>
            new DurableObjectContainer.ContainerError({
              operation: "deleteDirectoryBackup",
              cause,
            }),
        });
      }),
  };
});

export class ContainerWorkspace extends Context.Service<
  ContainerWorkspace,
  Effect.Success<typeof make>
>()("kommunikasie/ContainerWorkspace") {
  static readonly layer = Layer.effect(ContainerWorkspace, make);
}
