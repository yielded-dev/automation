import { failureDiagnostic } from "../observability/FailureDiagnostic.mjs";
import { PreviewRoute, WorkspaceCheckpoint } from "./NativeSandbox.mjs";
import { Cause, Context, Data, Effect, Layer, Predicate, Schema, Semaphore } from "effect";
import { ContainerFiles, DurableObjectContainer, DurableObjectState, WorkerEnvironment } from "effect-cf";
import { Files, SandboxFileError } from "@cloudflare/sandbox";
//#region src/cloudflare/ContainerState.ts
const RepositoryAuthorization = Schema.Struct({
	remote: Schema.String,
	credential: Schema.String,
	expiresAt: Schema.Finite
});
const OutboundProps = Schema.Struct({
	containerId: Schema.String,
	authority: Schema.Unknown,
	repositoryAuthorizations: Schema.Array(RepositoryAuthorization)
});
const Identity = Schema.Struct({
	instanceId: Schema.String,
	imageDigest: Schema.String,
	restoredSnapshotId: Schema.NullOr(Schema.String),
	timings: Schema.Struct({
		nativeStartRequestedAt: Schema.Finite,
		nativeStartReturnedAt: Schema.NullOr(Schema.Finite),
		ownerReadyAt: Schema.NullOr(Schema.Finite)
	})
});
const CapacityReservation = Schema.Struct({
	id: Schema.String,
	instanceId: Schema.String
});
const PreviewIndexRecord = Schema.Struct({
	route: PreviewRoute,
	expiresAt: Schema.Finite
});
const samePreviewRoute = Schema.toEquivalence(PreviewRoute);
const PreviewRecord = Schema.Struct({
	hash: Schema.String,
	route: PreviewRoute,
	expiresAt: Schema.Finite,
	instanceId: Schema.String,
	processIds: Schema.Array(Schema.String)
});
const samePreviewRecord = Schema.toEquivalence(PreviewRecord);
const SupervisorReply = Schema.Union([Schema.Struct({
	ok: Schema.Literal(true),
	value: Schema.Unknown
}), Schema.Struct({
	ok: Schema.Literal(false),
	error: Schema.Struct({
		code: Schema.String,
		message: Schema.String
	})
})]);
const CheckpointRecord = Schema.Struct({
	input: Schema.Struct({
		captureId: Schema.String,
		instanceId: Schema.String,
		setupFingerprint: Schema.String,
		head: Schema.String,
		materializationId: Schema.String,
		snapshotAllowed: Schema.Boolean
	}),
	result: Schema.NullOr(WorkspaceCheckpoint)
});
const sameCheckpointInput = Schema.toEquivalence(CheckpointRecord.fields.input);
const error = (operation, message) => new DurableObjectContainer.ContainerError({
	operation,
	cause: new Error(message)
});
var ReadinessProcessError = class extends Data.TaggedError("ReadinessProcessError") {};
var LeaseLostError = class extends Data.TaggedError("LeaseLostError") {};
var NativeProcessError = class extends Data.TaggedError("NativeProcessError") {};
const failureCode = (cause) => {
	const reason = cause.reasons[0];
	if (cause.reasons.length !== 1 || !reason || !Cause.isFailReason(reason)) return void 0;
	const value = reason.error;
	if (Predicate.isTagged(value, "ContainerReadinessTimeoutError")) return "ETIMEDOUT";
	if (value instanceof ReadinessProcessError) return value.code;
	if (value instanceof LeaseLostError) return "LEASE_LOST";
	if (value instanceof NativeProcessError) return value.code;
	return Predicate.isTagged(value, "ContainerError") && Predicate.hasProperty(value, "cause") && SandboxFileError.is(value.cause) ? value.cause.code : void 0;
};
const reply = (operation, effect) => effect.pipe(Effect.map((value) => ({
	_tag: "Success",
	value
})), Effect.catchCause((cause) => Effect.succeed({
	_tag: "Failure",
	diagnostic: failureDiagnostic(cause, { operation }),
	...failureCode(cause) === void 0 ? {} : { code: failureCode(cause) }
})));
const make = (options) => Effect.gen(function* () {
	const container = yield* DurableObjectContainer.DurableObjectContainer;
	const state = yield* DurableObjectState.DurableObjectState;
	const environment = yield* WorkerEnvironment;
	const cache = {
		revision: 0,
		mutations: 0
	};
	const files = ContainerFiles.fromFiles(new Files(container.raw));
	const readFileString = (path) => Effect.tryPromise({
		try: async (signal) => {
			return await (await files.raw.readFile(path, { signal })).text();
		},
		catch: (cause) => new DurableObjectContainer.ContainerError({
			operation: "files.readFileString",
			cause
		})
	}).pipe(Effect.timeout("30 seconds"));
	const boot = yield* Semaphore.make(1);
	const admission = yield* Semaphore.make(1);
	const previewValidation = yield* Semaphore.make(1);
	const admitted = (effect) => admission.withPermits(1)(effect);
	const mutateFiles = (effect) => Effect.acquireUseRelease(Effect.sync(() => {
		cache.revision++;
		cache.mutations++;
	}), () => effect, () => Effect.sync(() => {
		cache.revision++;
		cache.mutations--;
	}));
	const read = (key, schema) => state.storage.kv.get(key).pipe(Effect.flatMap((value) => value === void 0 ? Effect.succeed(null) : Schema.decodeUnknownEffect(schema)(value)));
	const identity = () => read("container-identity", Identity);
	return {
		read,
		identity,
		files,
		readFileString,
		admitted,
		mutateFiles,
		boot,
		previewValidation,
		cache,
		settings: {
			imageKey: options.imageKey ?? "harness",
			idleMillis: options.idleMillis ?? 3e5,
			capacityBinding: options.capacityBinding ?? "SANDBOX_MAX_INSTANCES",
			backupBucketBinding: options.backupBucketBinding,
			outboundEntrypoint: options.outboundEntrypoint ?? "SandboxOutbound",
			environmentVariables: options.environmentVariables,
			allowedHosts: options.allowedHosts(environment),
			hostOperationEnabled: options.hostOperationFetch !== void 0,
			sentryMcpHostname: options.sentryMcpFetch === void 0 ? void 0 : options.sentryMcpHostname
		}
	};
});
var ContainerState = class ContainerState extends Context.Service()("kommunikasie/ContainerState") {
	static layer = (options) => Layer.effect(ContainerState, make(options));
};
//#endregion
export { CapacityReservation, CheckpointRecord, ContainerState, Identity, LeaseLostError, NativeProcessError, OutboundProps, PreviewIndexRecord, PreviewRecord, ReadinessProcessError, RepositoryAuthorization, SupervisorReply, error, failureCode, reply, sameCheckpointInput, samePreviewRecord, samePreviewRoute };
