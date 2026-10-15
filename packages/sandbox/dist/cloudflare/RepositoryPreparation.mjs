import { reportSanitizedCause } from "../observability/RuntimeDiagnostics.mjs";
import { SandboxAgentConfig } from "./Config.mjs";
import { WorkspaceCheckpoint } from "./NativeSandbox.mjs";
import { resourceHash } from "./resource-names.mjs";
import { Sandboxes } from "./Bindings.mjs";
import { WORKSPACE_PREPARATION_VERSION, WorkspaceBaselineRepository } from "./WorkspaceBaselineRepository.mjs";
import { WorkspaceBaselinePreparationResult } from "./WorkspaceBaselinePreparationState.mjs";
import { OutboundAuthority } from "./OutboundAuthority.mjs";
import { isTerminalWorkspaceBaselinePreparationCause, prepareWorkspaceBaseline } from "./WorkspaceBaselinePreparation.mjs";
import { Context, DateTime, Effect, Layer, Schema, SchemaTransformation, Semaphore } from "effect";
import { DurableObject, DurableObjectAlarm, DurableObjectState } from "effect-cf";
//#region src/cloudflare/RepositoryPreparation.ts
const RepositoryPreparationSource = Schema.Struct({
	repositoryName: Schema.NonEmptyString,
	repositoryId: Schema.NonEmptyString,
	commitSha: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/u)),
	artifactsRef: Schema.optionalKey(Schema.NonEmptyString)
});
var RepositoryPreparationError = class extends Schema.TaggedError()("RepositoryPreparationError", {
	reason: Schema.Literals([
		"unauthorized",
		"owner-mismatch",
		"source-unavailable",
		"unavailable"
	]),
	cause: Schema.optionalKey(Schema.Defect())
}) {};
/** Hosts authorize repository keys and retain exact source generations; events carry no authority. */
var RepositoryPreparationSources = class extends Context.Service()("@reve/repairs/RepositoryPreparationSources") {
	static unconfiguredLayer = Layer.succeed(this, {
		authorize: () => Effect.fail(RepositoryPreparationError.make({ reason: "unauthorized" })),
		select: () => Effect.fail(RepositoryPreparationError.make({ reason: "unauthorized" }))
	});
};
const RepositoryPreparationRequest = Schema.Struct({
	repository: Schema.NonEmptyString,
	source: RepositoryPreparationSource
});
const samePreparation = Schema.toEquivalence(RepositoryPreparationRequest);
const Completion = Schema.Struct({
	status: Schema.Literals([
		"prepared",
		"reused",
		"disabled",
		"expired",
		"superseded"
	]),
	repository: Schema.optionalKey(Schema.String),
	checkpoint: Schema.optionalKey(WorkspaceCheckpoint),
	classIdentity: Schema.String,
	imageDigest: Schema.String,
	/** Older receipts have no configuration identity and are revalidated by a new generation. */
	configurationFingerprint: Schema.optionalKey(Schema.String)
});
const RepositoryPreparationState = Schema.Struct({
	request: RepositoryPreparationRequest,
	generation: Schema.String.check(Schema.isUUID()),
	completed: Schema.NullOr(Completion)
});
const RepositoryPreparationExecution = Schema.Struct({
	request: RepositoryPreparationRequest,
	generation: Schema.String.check(Schema.isUUID())
});
const RepositoryPreparationReply = Schema.Union([
	WorkspaceBaselinePreparationResult,
	Schema.Struct({ status: Schema.Literal("unavailable") }),
	Schema.Struct({ status: Schema.Literal("superseded") })
]);
const PreparationWakeReply = Schema.NullOr(Schema.Void).pipe(Schema.decodeTo(Schema.Void, SchemaTransformation.transform({
	decode: () => void 0,
	encode: () => null
})));
/** The RPC wire contract stays compatible with existing host-owned preparation objects. */
const repositoryPreparationMethods = {
	requestRepositoryPreparation: DurableObject.method({
		args: [Schema.NonEmptyString],
		success: DurableObject.native(PreparationWakeReply)
	}),
	executeRepositoryPreparation: DurableObject.method({
		args: [RepositoryPreparationExecution],
		success: RepositoryPreparationReply
	})
};
var RepositoryPreparationObject = class extends DurableObject.Tag()("@reve/repairs/RepositoryPreparationObject", repositoryPreparationMethods) {};
var RepositoryPreparationGate = class extends Context.Service()("@reve/repairs/RepositoryPreparationGate") {
	static layer = Layer.effect(this, Semaphore.make(1));
};
const repositoryPreparationName = Effect.fnUntraced(function* (repository) {
	return `repository-preparation:${yield* resourceHash(JSON.stringify([repository]))}`;
});
const isRepositoryPreparationOwner = (name) => name?.startsWith("repository-preparation:") === true;
const isRepositoryPreparationCoordinator = (name) => isRepositoryPreparationOwner(name) && name?.split(":").length === 2;
const preparationOwner = Effect.fnUntraced(function* (repository, generation) {
	yield* (yield* RepositoryPreparationSources).authorize(repository);
	const state = yield* DurableObjectState.DurableObjectState;
	const name = yield* repositoryPreparationName(repository);
	if (state.id.name !== (generation === void 0 ? name : `${name}:${generation}`)) return yield* RepositoryPreparationError.make({ reason: "owner-mismatch" });
	return name;
});
/** Coalesce source events in the existing durable alarm lane. */
const scheduleRepositoryPreparation = Effect.fn("RepositoryPreparation.schedule")(function* (repository) {
	yield* preparationOwner(repository);
	yield* (yield* DurableObjectAlarm.DurableObjectAlarm).scheduleAlarm({
		tag: "repository-preparation",
		id: "main",
		payload: repository,
		runAt: yield* DateTime.now
	});
});
/** Call from a durable host event consumer so a rolling-upgrade RPC failure can retry. */
const requestRepositoryPreparation = Effect.fn("RepositoryPreparation.request")(function* (repository) {
	const name = yield* repositoryPreparationName(repository);
	yield* (yield* RepositoryPreparationObject).byName(name).requestRepositoryPreparation(repository);
});
const prepareRepository = Effect.fn("RepositoryPreparation.prepare")(function* (repository) {
	const name = yield* preparationOwner(repository);
	const state = yield* DurableObjectState.DurableObjectState;
	const sources = yield* RepositoryPreparationSources;
	const saved = yield* state.storage.get("repositoryPreparation");
	let execution = saved === void 0 ? void 0 : yield* Schema.decodeUnknownEffect(RepositoryPreparationState)(saved);
	const resumed = execution?.completed === null;
	const description = yield* (yield* Sandboxes.get("repository-preparation-policy")).describe;
	const config = yield* SandboxAgentConfig;
	const configurationFingerprint = yield* resourceHash(JSON.stringify([
		config.repositorySetupPath,
		config.artifactsHostname,
		config.artifactsNamespace,
		config.sandboxNamespace ?? null,
		WORKSPACE_PREPARATION_VERSION
	]));
	if (execution === void 0 || execution.completed !== null) {
		const source = yield* sources.select(repository);
		const request = {
			repository,
			source
		};
		if (execution !== void 0 && execution.completed !== null) {
			const completed = execution.completed;
			if (samePreparation(execution.request, request) && completed.classIdentity === description.classIdentity && completed.imageDigest === description.imageDigest && completed.configurationFingerprint === configurationFingerprint) {
				if (completed.status === "disabled") return;
				if (completed.repository !== void 0 && completed.checkpoint !== void 0) {
					if ((yield* (yield* WorkspaceBaselineRepository).find({
						repository: completed.repository,
						classIdentity: description.classIdentity,
						imageDigest: description.imageDigest,
						head: source.commitSha,
						setupFingerprint: completed.checkpoint.setupFingerprint
					}))?.checkpoint.head === source.commitSha) return;
				}
			}
		}
		execution = {
			request,
			generation: yield* Effect.sync(() => crypto.randomUUID()),
			completed: null
		};
		yield* state.storage.put("repositoryPreparation", execution);
	}
	const { request, generation } = execution;
	const result = yield* (yield* RepositoryPreparationObject).byName(`${name}:${generation}`).executeRepositoryPreparation({
		request,
		generation
	});
	if (result.status === "unavailable") return yield* RepositoryPreparationError.make({ reason: "unavailable" });
	const baseline = result.status === "superseded" ? null : result.baseline;
	if ((result.status === "prepared" || result.status === "reused") && baseline?.checkpoint.head !== request.source.commitSha) return yield* RepositoryPreparationError.make({ reason: "unavailable" });
	execution = {
		request,
		generation,
		completed: {
			status: result.status,
			configurationFingerprint,
			classIdentity: baseline?.checkpoint.classIdentity ?? description.classIdentity,
			imageDigest: baseline?.checkpoint.imageDigest ?? description.imageDigest,
			...baseline === null ? {} : {
				repository: baseline.repository,
				checkpoint: baseline.checkpoint
			}
		}
	};
	yield* state.storage.put("repositoryPreparation", execution);
	if (resumed && !samePreparation(request, {
		repository,
		source: yield* sources.select(repository)
	})) yield* scheduleRepositoryPreparation(repository);
});
const runRepositoryPreparationAlarms = Effect.gen(function* () {
	const gate = yield* RepositoryPreparationGate;
	return yield* (yield* DurableObjectAlarm.DurableObjectAlarm).deferWakes(DurableObjectAlarm.define({ "repository-preparation": Schema.NonEmptyString }).handlers({ "repository-preparation": ({ payload }) => prepareRepository(payload).pipe(gate.withPermits(1), Effect.tapCause((cause) => reportSanitizedCause(cause, { operation: "RepositoryPreparation.prepare" }))) }, { retryFailedAfter: "30 seconds" }));
});
const executeRepositoryPreparation = Effect.fn("RepositoryPreparation.execute")(function* (input) {
	const gate = yield* RepositoryPreparationGate;
	const name = yield* preparationOwner(input.request.repository, input.generation);
	return yield* prepareWorkspaceBaseline({
		sessionId: `${name}:${input.generation}`,
		turnId: input.generation,
		repositoryId: input.request.repository,
		source: input.request.source
	}).pipe(gate.withPermits(1), Effect.provide(OutboundAuthority.noneLayer), Effect.catchCauseIf(isTerminalWorkspaceBaselinePreparationCause, (cause) => reportSanitizedCause(cause, { operation: "RepositoryPreparation.execute" }).pipe(Effect.as({ status: "superseded" }))));
});
//#endregion
export { RepositoryPreparationError, RepositoryPreparationExecution, RepositoryPreparationGate, RepositoryPreparationObject, RepositoryPreparationReply, RepositoryPreparationRequest, RepositoryPreparationSource, RepositoryPreparationSources, RepositoryPreparationState, executeRepositoryPreparation, isRepositoryPreparationCoordinator, isRepositoryPreparationOwner, repositoryPreparationMethods, repositoryPreparationName, requestRepositoryPreparation, runRepositoryPreparationAlarms, scheduleRepositoryPreparation };
