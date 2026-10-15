import "../workspace-paths.mjs";
import { CAPACITY_NAME, NativeSandboxNamespace } from "./NativeSandbox.mjs";
import { CheckpointRecord, ContainerState, error, failureCode, sameCheckpointInput } from "./ContainerState.mjs";
import { ContainerLifecycle } from "./ContainerLifecycle.mjs";
import { ContainerProcesses } from "./ContainerProcesses.mjs";
import { Clock, Context, Effect, Layer } from "effect";
import { DurableObjectContainer, DurableObjectState } from "effect-cf";
import { DirectoryBackup } from "@cloudflare/sandbox";
//#region src/cloudflare/ContainerWorkspace.ts
const SNAPSHOT_TTL = 25056e5;
const make = Effect.gen(function* () {
	const container = yield* DurableObjectContainer.DurableObjectContainer;
	const state = yield* DurableObjectState.DurableObjectState;
	const namespace = yield* NativeSandboxNamespace;
	const { read, identity, files, readFileString, admitted, mutateFiles, settings: options } = yield* ContainerState;
	const { ensure, protectNativeIO } = yield* ContainerLifecycle;
	const { processes } = yield* ContainerProcesses;
	const backup = Effect.gen(function* () {
		if (!options.backupBucketBinding) return yield* error("backup", "Directory backups are not configured");
		const exports = state.raw.exports;
		return new DirectoryBackup(container.raw, exports.DirectoryBackupGateway, {
			binding: options.backupBucketBinding,
			prefix: "backups/"
		});
	});
	const checkpoint = Effect.fn("SandboxContainer.checkpoint")(function* (input) {
		const current = yield* identity();
		if (!current || current.instanceId !== input.instanceId || current.timings.ownerReadyAt === null || !(yield* container.running)) return yield* error("snapshot", "No running workspace can be captured");
		const previous = yield* read("workspace-checkpoint", CheckpointRecord);
		if (previous?.input.instanceId === current.instanceId) {
			if (!sameCheckpointInput(previous.input, input)) return yield* error("snapshot", "This boot already owns a different checkpoint capture");
			if (!input.snapshotAllowed || previous.result !== null) return previous.result;
			return yield* error("snapshot", "Capture outcome is unknown; snapshot was not repeated");
		}
		if (!input.snapshotAllowed) {
			yield* state.storage.kv.put("workspace-checkpoint", {
				input,
				result: null
			});
			return null;
		}
		if ((yield* readFileString("/workspace/.sandbox-agent-materialization")).trim() !== input.materializationId) return yield* error("snapshot", "Snapshot materialization does not match");
		if ((yield* processes).some((job) => job.state !== "exited")) return yield* error("snapshot", "Workspace process outcomes must close before capture");
		const verified = yield* (yield* container.execScoped([
			"git",
			"-C",
			"/workspace/repo",
			"rev-parse",
			"HEAD"
		])).outputText;
		if (verified.exitCode !== 0 || verified.stdout.trim() !== input.head) return yield* error("snapshot", "Snapshot source does not match");
		const now = yield* Clock.currentTimeMillis;
		yield* state.storage.kv.put("workspace-checkpoint", {
			input,
			result: null
		});
		yield* state.storage.sync().pipe(Effect.withSpan("SandboxContainer.snapshotOutputGate"));
		const saved = {
			kind: "container",
			snapshotId: (yield* Effect.gen(function* () {
				const requestedAt = yield* Clock.currentTimeMillis;
				const result = yield* container.snapshotContainer({ name: "prepared-workspace" });
				yield* Effect.annotateCurrentSpan({
					"sandbox.snapshot.sizeBytes": result.size,
					"sandbox.snapshot.captureMs": (yield* Clock.currentTimeMillis) - requestedAt
				});
				return result;
			}).pipe(Effect.withSpan("SandboxContainer.nativeSnapshot"))).id,
			imageDigest: current.imageDigest,
			setupFingerprint: input.setupFingerprint,
			head: input.head,
			materializationId: input.materializationId,
			classIdentity: (yield* namespace.idFromName(CAPACITY_NAME)).toString(),
			instanceId: current.instanceId,
			capturedAt: now,
			expiresAt: now + SNAPSHOT_TTL
		};
		yield* state.storage.kv.put("workspace-checkpoint", {
			input,
			result: saved
		});
		return saved;
	});
	return {
		checkpoint: (input) => admitted(protectNativeIO(checkpoint(input))),
		exists: (path, start) => ensure(start).pipe(Effect.andThen(files.stat(path)), Effect.as(true), Effect.catchCause((cause) => failureCode(cause) === "ENOENT" ? Effect.succeed(false) : Effect.failCause(cause))),
		readFileString: (path, start) => ensure(start).pipe(Effect.andThen(readFileString(path))),
		readFile: (path, start) => ensure(start).pipe(Effect.andThen(files.readFile(path))),
		lstat: (path, start) => ensure(start).pipe(Effect.andThen(files.lstat(path))),
		writeFile: (path, content, start) => admitted(ensure(start).pipe(Effect.andThen(mutateFiles(files.writeFile(path, content))))),
		readDirectory: (path, start) => ensure(start).pipe(Effect.andThen(files.readDirectory(path))),
		mkdir: (path, recursive, start) => admitted(ensure(start).pipe(Effect.andThen(mutateFiles(files.mkdir(path, { recursive }))))),
		remove: (path, recursive, force, start) => admitted(ensure(start).pipe(Effect.andThen(mutateFiles(files.remove(path, {
			recursive,
			force
		}))))),
		backupDirectory: (input, start) => admitted(ensure(start).pipe(Effect.andThen(protectNativeIO(Effect.gen(function* () {
			const owner = yield* backup;
			return yield* Effect.tryPromise({
				try: (signal) => owner.backup({
					...input,
					signal
				}),
				catch: (cause) => new DurableObjectContainer.ContainerError({
					operation: "backup",
					cause
				})
			});
		}))))),
		restoreDirectory: (receipt, start) => admitted(ensure(start).pipe(Effect.andThen(protectNativeIO(mutateFiles(Effect.gen(function* () {
			const owner = yield* backup;
			yield* Effect.tryPromise({
				try: (signal) => owner.restore(receipt, { signal }),
				catch: (cause) => new DurableObjectContainer.ContainerError({
					operation: "restoreDirectory",
					cause
				})
			});
		})))))),
		deleteDirectoryBackup: (receipt) => Effect.gen(function* () {
			const owner = yield* backup;
			yield* Effect.tryPromise({
				try: (signal) => owner.delete(receipt, { signal }),
				catch: (cause) => new DurableObjectContainer.ContainerError({
					operation: "deleteDirectoryBackup",
					cause
				})
			});
		})
	};
});
var ContainerWorkspace = class ContainerWorkspace extends Context.Service()("kommunikasie/ContainerWorkspace") {
	static layer = Layer.effect(ContainerWorkspace, make);
};
//#endregion
export { ContainerWorkspace };
