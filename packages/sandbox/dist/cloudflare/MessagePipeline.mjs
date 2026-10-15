import { agentPhases, unknownLifecycleObserved } from "../Contract.mjs";
import { SessionSandbox } from "../SessionSandbox.mjs";
import { AgentEventSink } from "./AgentEventSink.mjs";
import { reportSanitizedCause } from "../observability/RuntimeDiagnostics.mjs";
import { Harness, HarnessTurnError } from "../harness/Harness.mjs";
import { inMeasure, inPhase } from "../tracing/RunSpanCapture.mjs";
import { MATERIALIZATION_MARKER, REPO_DIR } from "../workspace-paths.mjs";
import { SandboxAgentConfig, requireRepositorySeed, sandboxAgentDeadlinePolicy } from "./Config.mjs";
import { isSandboxFileMissing } from "./NativeSandbox.mjs";
import { forkNameFor, resourceHash, sandboxNameFor } from "./resource-names.mjs";
import { sandboxFor } from "./Bindings.mjs";
import "./WorkspaceSnapshotPolicy.mjs";
import { WORKSPACE_PREPARATION_VERSION, WorkspaceBaselineRepository } from "./WorkspaceBaselineRepository.mjs";
import { SessionStateStore } from "./SessionStateStore.mjs";
import { TurnWorkspaceServices, ensureWorkspaceServices, killStrayHarnessProcesses } from "./WorkspaceProcesses.mjs";
import { prepareRepositorySetup, readRepositorySetup, repositorySetupCheckpointMatches, repositorySetupFingerprint } from "./RepositorySetup.mjs";
import { ArtifactsCheckout, RepositoryForkIntentError } from "./ArtifactsCheckout.mjs";
import { OutboundAuthority } from "./OutboundAuthority.mjs";
import { withSandboxLease } from "./SandboxLease.mjs";
import { RepositorySourceCheckout } from "./RepositorySourceCheckout.mjs";
import { Cause, Clock, Context, Effect, Exit, Fiber, Option, Predicate, Ref } from "effect";
//#region src/cloudflare/MessagePipeline.ts
/**
* One message turn: resolve durable state, prepare the sandbox workspace,
* run the harness, persist the result, and emit the lifecycle event stream.
*/
const gitOperationSummary = (command) => {
	switch (command) {
		case "clone": return "clone";
		case "rev-parse": return "revision inspection";
		case "remote": return "remote configuration";
		case "status": return "status inspection";
		case "add": return "change staging";
		case "-c": return "commit";
		case "diff": return "change inspection";
		case "push": return "push";
		default: return "operation";
	}
};
const harnessTurnSummary = (reason) => {
	switch (reason) {
		case "malformedEvent": return {
			reason,
			message: "The harness emitted a malformed event"
		};
		case "oversizedEvent": return {
			reason,
			message: "The harness emitted an oversized event"
		};
		case "missingTerminalEvent": return {
			reason,
			message: "The harness exited without a terminal event"
		};
		case "missingResumeToken": return {
			reason,
			message: "The harness did not report a resume token"
		};
		case "nonZeroExit": return {
			reason,
			message: "The harness process exited unsuccessfully"
		};
		case "deadlineExceeded": return {
			reason,
			message: "The harness turn exceeded its deadline"
		};
		case "turnFailed": return {
			reason,
			message: "The harness reported a failed turn"
		};
		case "freshSessionRequired": return {
			reason,
			message: "The harness session could not be resumed"
		};
		default: return null;
	}
};
/** Flattens a pipeline cause into bounded, content-free wire fields. The full
* cause is logged at the host boundary before this summary is emitted. */
const describeFailure = (cause) => {
	const found = Cause.findErrorOption(cause);
	if (Option.isNone(found)) return {
		reason: "Defect",
		message: "The message pipeline died unexpectedly"
	};
	const failure = found.value;
	if (Predicate.isObject(failure) && Predicate.hasProperty(failure, "_tag")) {
		if (failure._tag === "GitCommandError" && Predicate.hasProperty(failure, "command") && Predicate.isString(failure.command) && Predicate.hasProperty(failure, "exitCode") && Predicate.isNumber(failure.exitCode)) return {
			reason: "GitCommandError",
			message: `Git ${gitOperationSummary(failure.command)} exited with ${failure.exitCode}`
		};
		if (failure._tag === "PersistedPathsLimitError" && Predicate.hasProperty(failure, "pathCount") && Predicate.isNumber(failure.pathCount) && Predicate.hasProperty(failure, "observedBytes") && Predicate.isNumber(failure.observedBytes) && Predicate.hasProperty(failure, "maxBytes") && Predicate.isNumber(failure.maxBytes)) return {
			reason: "PersistedPathsLimitError",
			message: `${failure.pathCount} changed paths encoded to ${failure.observedBytes} bytes (maximum ${failure.maxBytes})`
		};
		if (failure._tag === "HarnessTurnError" && Predicate.hasProperty(failure, "reason") && Predicate.isString(failure.reason)) {
			const summary = harnessTurnSummary(failure.reason);
			if (summary !== null) return summary;
		}
		if (failure._tag === "ArtifactsOperationTimeoutError" && Predicate.hasProperty(failure, "timeoutMillis") && Predicate.isNumber(failure.timeoutMillis)) return {
			reason: "ArtifactsOperationTimeoutError",
			message: `An Artifacts operation exceeded ${failure.timeoutMillis} milliseconds`
		};
		switch (failure._tag) {
			case "ArtifactsOperationError": return {
				reason: "ArtifactsOperationError",
				message: "An Artifacts operation failed"
			};
			case "ForkNotReadyError": return {
				reason: "ForkNotReadyError",
				message: "The durable fork was not ready"
			};
			case "RepositoryForkIntentError": return {
				reason: "RepositoryForkIntentError",
				message: "The repository no longer matches its retained source or private fork"
			};
			case "PushVerificationError": return {
				reason: "PushVerificationError",
				message: "The durable repository did not match the persisted commit"
			};
			case "SandboxOperationError": return {
				reason: "SandboxOperationError",
				message: "A sandbox operation failed"
			};
		}
	}
	return {
		reason: "PipelineFailure",
		message: "The message pipeline failed"
	};
};
/** One-pass extraction of the run facts CI and analysis care about. */
const summarizeRun = (recorded) => {
	let outcome = "failed";
	let failureReason = null;
	let timings = null;
	let harness = null;
	let harnessVersion = null;
	let model = null;
	let reasoningEffort = null;
	let fast = null;
	let containerSize = null;
	let materialization = null;
	let head = null;
	let changedPaths = [];
	let usage = null;
	let resumeToken = null;
	for (const { event } of recorded) if (event.type === "agent.ready") {
		harness = event.harness;
		harnessVersion = event.harnessVersion;
		model = event.model;
		reasoningEffort = event.reasoningEffort;
		fast = event.fast;
		containerSize = event.containerSize;
		materialization = event.materialization;
	} else if (event.type === "agent.persisted") {
		head = event.head;
		changedPaths = event.changedPaths;
	} else if (event.type === "agent.completed") {
		outcome = "completed";
		timings = event.timings;
		usage = event.usage;
		resumeToken = event.resumeToken;
	} else if (event.type === "agent.failed") failureReason = `${event.reason}: ${event.message}`;
	return {
		outcome,
		failureReason,
		timings,
		harness,
		harnessVersion,
		model,
		reasoningEffort,
		fast,
		containerSize,
		materialization,
		head,
		changedPaths,
		usage,
		resumeToken
	};
};
const finishLifecycle = (capture) => capture.preTurn === null ? null : {
	preTurn: capture.preTurn,
	observed: capture.observed ?? unknownLifecycleObserved,
	postTurn: capture.postTurn
};
/** Optional host receiver for the current native turn's steering capability. */
const TurnSteering = Context.Reference("sandbox-agent/TurnSteering", { defaultValue: () => null });
/** Required for generation-bound turns; retain the exact commit before its remote write. */
const GenerationSourceCheckpoint = Context.Reference("sandbox-agent/GenerationSourceCheckpoint", { defaultValue: () => null });
/** Optional per-turn archive capture; ordinary turns need no mutable capture. */
const TurnLifecycleCapture = Context.Reference("sandbox-agent/TurnLifecycleCapture", { defaultValue: () => null });
const runTurn = Effect.fn("SandboxAgentSession.runMessagePipeline")(function* (context) {
	yield* Effect.annotateCurrentSpan({
		"sandbox.task.id": context.sessionId,
		"sandbox.turn.id": context.turnId
	});
	const output = yield* AgentEventSink;
	const store = yield* SessionStateStore;
	const checkout = yield* ArtifactsCheckout;
	const generationCheckouts = yield* RepositorySourceCheckout;
	const harness = yield* Harness;
	const config = yield* SandboxAgentConfig;
	const lifecycle = yield* TurnLifecycleCapture;
	const deadlinePolicy = sandboxAgentDeadlinePolicy(config);
	const saved = yield* store.pipelineTurn.get;
	const savedCodexTurn = context.recover === true ? yield* store.codexTurn.get : null;
	const recovery = context.recover === true && saved?.id === context.turnId ? saved : null;
	if (context.recover === true && recovery === null && savedCodexTurn?.id === context.turnId) return yield* HarnessTurnError.make({
		reason: "turnFailed",
		message: "The active native turn has no matching workspace receipt; its input was not replayed"
	});
	const startedAt = recovery?.startedAtMillis ?? context.startedAtMillis;
	const timings = {};
	const mark = (phase) => Effect.map(Clock.currentTimeMillis, (now) => {
		timings[phase] = now - startedAt;
	});
	const complete = Effect.fnUntraced(function* (persisted, turn) {
		yield* mark("gitPersisted");
		if (persisted.head !== null) yield* store.verifiedHead.put(persisted.head);
		yield* mark("verified");
		if (lifecycle !== null) yield* Ref.update(lifecycle, (capture) => ({
			...capture,
			postTurn: {
				verifiedHead: persisted.head,
				resumeToken: turn.resumeToken
			}
		}));
		if (persisted.head !== null) yield* output.emit({
			type: "agent.persisted",
			head: persisted.head,
			changedPaths: persisted.changedPaths
		});
		yield* mark("total");
		yield* store.lastTimings.put(timings);
		yield* output.emit({
			type: "agent.completed",
			resumeToken: turn.resumeToken,
			usage: turn.usage,
			timings
		});
	});
	yield* mark("doActive");
	const hash = yield* resourceHash(context.sessionId);
	const storedFork = yield* store.forkName.get;
	const storedForkIntent = yield* store.repositoryFork.get;
	const forkName = storedFork ?? forkNameFor(hash);
	const repositoryFree = context.repository?._tag === "None";
	const attached = context.repository?._tag === "AttachedRepository" ? context.repository : void 0;
	const seed = context.repository?._tag === "SeedRepository" ? context.repository : void 0;
	const seeded = seed !== void 0 || context.repository === void 0 && context.generationSource === void 0;
	const seedRepository = seeded ? seed?.repositoryName ?? context.seedRepository ?? config.seedRepository : void 0;
	const hadFork = storedFork !== null;
	const storedSize = yield* store.containerSize.get;
	const containerSize = context.containerSize ?? "standard";
	if (storedSize !== containerSize) yield* store.containerSize.put(containerSize);
	const storedMaterialization = yield* store.materializationId.get;
	const storedResumeToken = yield* store.resumeToken.get;
	const storedHead = yield* store.verifiedHead.get;
	const storedInstance = yield* store.containerInstanceId.get;
	const storedCheckpoint = yield* store.workspaceCheckpoint.get;
	const storedSetup = yield* store.repositorySetupReceipt.get;
	const preTurn = {
		forkRecorded: hadFork,
		materializationRecorded: storedMaterialization !== null,
		resumeTokenRecorded: storedResumeToken !== null,
		containerSize: storedSize,
		verifiedHead: storedHead
	};
	if (lifecycle !== null) yield* Ref.update(lifecycle, (capture) => ({
		...capture,
		preTurn
	}));
	if (recovery !== null && recovery.persisted !== null && savedCodexTurn?.id === context.turnId && savedCodexTurn.result !== null) {
		if (storedForkIntent !== null) {
			if (!storedForkIntent.initialized || storedForkIntent.repositoryId === null) return yield* RepositoryForkIntentError.make({ reason: "target-mismatch" });
			yield* checkout.ensurePlannedFork(storedForkIntent);
		}
		yield* store.resumeToken.put(savedCodexTurn.result.resumeToken);
		return yield* complete(recovery.persisted, savedCodexTurn.result);
	}
	if (recovery !== null && recovery.instanceId === void 0) return yield* HarnessTurnError.make({
		reason: "turnFailed",
		message: "The active turn has no native boot receipt; its input was not replayed"
	});
	const sandboxLookup = yield* Effect.gen(function* () {
		const unresolved = yield* sandboxFor(containerSize, sandboxNameFor(hash), recovery === null ? {} : {
			recovery: "exactWorkspaceRequired",
			expectedInstanceId: recovery.instanceId
		});
		return {
			unresolved,
			description: yield* unresolved.describe
		};
	}).pipe(Effect.forkScoped);
	const baselines = yield* WorkspaceBaselineRepository;
	const admittedSource = seed === void 0 ? context.repositorySource : {
		repositoryName: seed.repositoryName,
		repositoryId: seed.artifactsRepositoryId,
		commitSha: seed.commitSha,
		...seed.artifactsRef === void 0 ? {} : { artifactsRef: seed.artifactsRef }
	};
	const source = seeded && (storedForkIntent !== null || !hadFork) ? admittedSource ?? storedForkIntent?.source ?? (yield* Effect.gen(function* () {
		const generation = yield* checkout.sourceGeneration(yield* requireRepositorySeed(seedRepository));
		return {
			...generation,
			commitSha: yield* checkout.readGenerationMainHead(generation)
		};
	})) : admittedSource;
	const plannedSource = seeded && (storedForkIntent !== null || !hadFork) ? source : void 0;
	const forkIntent = plannedSource !== void 0 ? yield* checkout.retainFork(forkName, plannedSource, context.deadlineAtMs) : null;
	const pinnedReadRetryDeadline = Effect.gen(function* () {
		const currentFork = yield* store.repositoryFork.get;
		return Math.min(context.deadlineAtMs ?? Infinity, startedAt + deadlinePolicy.forkReadyMillis, savedCodexTurn?.id === context.turnId ? savedCodexTurn.deadlineAt : Infinity, currentFork !== null && !currentFork.initialized ? currentFork.deadlineAtMillis : Infinity);
	});
	const forkState = yield* Ref.make({ state: forkIntent?.initialized ? "ready" : "pending" });
	const forkFiber = forkIntent === null ? null : yield* checkout.ensurePlannedFork(forkIntent).pipe(Effect.tap(() => forkIntent.initialized ? Effect.void : mark("forkReady")), inPhase(agentPhases.fork), Effect.exit, Effect.tap((exit) => Exit.isSuccess(exit) ? Ref.set(forkState, { state: "ready" }) : Ref.set(forkState, {
		state: "failed",
		cause: exit.cause
	})), Effect.forkScoped);
	const checkForkBeforeModel = Effect.gen(function* () {
		if (plannedSource === void 0) return;
		if (forkFiber === null || forkIntent === null) return yield* RepositoryForkIntentError.make({ reason: "intent-mismatch" });
		const accepted = yield* store.codexTurn.get;
		if (accepted?.id === context.turnId && accepted.startDispatched) return;
		const state = yield* Ref.get(forkState);
		if (state.state === "failed") return yield* Effect.failCause(state.cause);
	});
	const joinFork = Effect.gen(function* () {
		if (plannedSource === void 0) return void 0;
		if (forkFiber === null || forkIntent === null) return yield* RepositoryForkIntentError.make({ reason: "intent-mismatch" });
		const exit = yield* Fiber.join(forkFiber);
		if (Exit.isFailure(exit)) return yield* Effect.failCause(exit.cause);
		const generation = yield* checkout.ensurePlannedFork(forkIntent);
		if (storedFork === null) yield* store.forkName.put(generation.repositoryName);
		return generation;
	}).pipe(inMeasure("fork-join"));
	const persistRepository = (baseHead) => Effect.gen(function* () {
		const generation = yield* joinFork;
		return yield* checkout.persist(forkName, baseHead, yield* requireRepositorySeed(seedRepository), generation?.repositoryId);
	});
	const ensuredFork = recovery === null && seeded && plannedSource === void 0 ? yield* checkout.ensureFork(forkName, yield* requireRepositorySeed(seedRepository)).pipe(inPhase(agentPhases.fork)) : null;
	const generation = source ?? (recovery === null && seeded && !hadFork ? yield* checkout.sourceGeneration(yield* requireRepositorySeed(seedRepository)) : null);
	const readGeneration = plannedSource === void 0 ? void 0 : forkIntent?.initialized && forkIntent.repositoryId !== null ? {
		repositoryName: forkIntent.forkName,
		repositoryId: forkIntent.repositoryId
	} : plannedSource;
	const repositoryHead = ensuredFork?.head ?? storedHead ?? plannedSource?.commitSha;
	const repository = context.preparationKey !== void 0 ? `${config.artifactsHostname}/${config.artifactsNamespace}/prepared:${context.preparationKey}` : attached !== void 0 ? `${config.artifactsHostname}/${config.artifactsNamespace}/${attached.repositoryName}@${attached.artifactsRepositoryId}` : context.generationSource === void 0 ? `${config.artifactsHostname}/${config.artifactsNamespace}/${generation?.repositoryName ?? seedRepository}@${generation?.repositoryId ?? ""}` : `${config.artifactsHostname}/${config.artifactsNamespace}/${context.generationSource.repositoryName}@${context.generationSource.repositoryId}`;
	const desiredHead = (attached === void 0 ? void 0 : storedHead ?? context.initialCheckout ?? attached.commitSha) ?? (context.generationSource === void 0 ? context.initialCheckout ?? repositoryHead : context.generationSource.draft?.sourceSha ?? context.generationSource.acceptedSourceSha);
	if (recovery === null && storedForkIntent !== null && plannedSource !== void 0 && readGeneration === plannedSource) yield* checkout.readSourcePin(plannedSource, yield* pinnedReadRetryDeadline).pipe(inMeasure("repository-source-validate"));
	const { unresolved, description } = yield* Fiber.join(sandboxLookup);
	const now = yield* Clock.currentTimeMillis;
	let validSessionCheckpoint = !repositoryFree && storedCheckpoint !== null && storedCheckpoint.classIdentity === description.classIdentity && storedCheckpoint.imageDigest === description.imageDigest && storedCheckpoint.expiresAt > now + 3e5 ? storedCheckpoint : null;
	const ownerCheckpoint = description.checkpoint;
	if (validSessionCheckpoint !== null && ownerCheckpoint !== null && ownerCheckpoint.classIdentity === validSessionCheckpoint.classIdentity && ownerCheckpoint.imageDigest === validSessionCheckpoint.imageDigest && ownerCheckpoint.head === validSessionCheckpoint.head && ownerCheckpoint.materializationId === validSessionCheckpoint.materializationId && ownerCheckpoint.setupFingerprint === validSessionCheckpoint.setupFingerprint && ownerCheckpoint.expiresAt > validSessionCheckpoint.expiresAt) {
		validSessionCheckpoint = ownerCheckpoint;
		yield* store.workspaceCheckpoint.put(ownerCheckpoint);
	}
	let selected = validSessionCheckpoint === null || storedSetup === null ? null : {
		repository,
		checkpoint: validSessionCheckpoint,
		report: storedSetup.report
	};
	const continuation = context.continuationCheckpoint;
	const fromContinuation = recovery === null && (seeded || attached !== void 0) && selected === null && validSessionCheckpoint === null && continuation !== void 0 && continuation !== null && (description.current === null || description.current.restoredSnapshotId === continuation.checkpoint.snapshotId) && continuation.checkpoint.classIdentity === description.classIdentity && continuation.checkpoint.imageDigest === description.imageDigest && continuation.checkpoint.expiresAt > now + 3e5;
	if (fromContinuation) selected = {
		repository,
		...continuation
	};
	let shared = false;
	const findBaseline = (key) => baselines.find(key).pipe(inMeasure(key.setupFingerprint === void 0 ? "workspace-checkpoint-lookup" : "workspace-checkpoint-compatible-lookup"), Effect.catchCause((cause) => cause.reasons.every((reason) => Cause.isFailReason(reason) && Predicate.isTagged(reason.error, "WorkspaceBaselineError")) ? reportSanitizedCause(cause, { operation: "MessagePipeline.baselineLookup" }).pipe(Effect.as(null)) : Effect.failCause(cause)));
	if (recovery === null && (seeded || attached !== void 0) && description.current === null && validSessionCheckpoint === null && selected === null && desiredHead !== null && desiredHead !== void 0) {
		selected = yield* findBaseline({
			repository,
			classIdentity: description.classIdentity,
			imageDigest: description.imageDigest,
			head: desiredHead
		});
		shared = selected !== null;
	}
	let identity = yield* unresolved.ensure({
		recovery: recovery === null ? "freshAllowed" : "exactWorkspaceRequired",
		...recovery?.instanceId === void 0 ? {} : { expectedInstanceId: recovery.instanceId },
		...recovery !== null ? {} : { checkpoint: selected?.checkpoint ?? validSessionCheckpoint ?? void 0 }
	}).pipe(inPhase(agentPhases.containerBoot));
	if (recovery === null && (seeded || attached !== void 0) && storedInstance !== identity.instanceId && ((storedMaterialization !== null || storedSetup !== null) && storedSetup?.prepared !== true || saved?.id === context.turnId)) return yield* HarnessTurnError.make({
		reason: "turnFailed",
		message: "The previous workspace has unfinished preparation; its commands were not replayed"
	});
	let selectedWorkspaceCheckpoint = selected?.checkpoint ?? validSessionCheckpoint;
	let checkpointRejected = false;
	let checkpointReselected = false;
	let restoredSetupInputs;
	let restoredCommitWithHistory;
	let validatedRestore;
	while (recovery === null && (seeded || attached !== void 0) && identity.restoredSnapshotId !== null) {
		const restoredSandbox = yield* sandboxFor(containerSize, sandboxNameFor(hash), {
			recovery: "exactWorkspaceRequired",
			expectedInstanceId: identity.instanceId
		});
		let compatibleKey;
		if (yield* withSandboxLease(`${context.turnId}:checkpoint-validation`, Effect.gen(function* () {
			const marker = yield* restoredSandbox.readFileString("/workspace/.sandbox-agent-materialization").pipe(Effect.map((content) => content.trim()), Effect.catchCause((cause) => cause.reasons.length > 0 && cause.reasons.every((reason) => Cause.isFailReason(reason) && isSandboxFileMissing(reason.error)) ? Effect.succeed(null) : Effect.failCause(cause)));
			if (storedInstance === identity.instanceId) {
				if (storedMaterialization !== null && marker === storedMaterialization) return true;
				return yield* HarnessTurnError.make({
					reason: "turnFailed",
					message: "The restored workspace does not match its retained materialization; its commands were not replayed"
				});
			}
			if (selected === null || desiredHead === null || desiredHead === void 0 || selected.checkpoint.snapshotId !== identity.restoredSnapshotId || marker !== selected.checkpoint.materializationId) return false;
			const authority = yield* (yield* OutboundAuthority).mint({
				sessionId: context.sessionId,
				turnId: context.turnId,
				instanceId: identity.instanceId
			});
			if (Option.isSome(authority)) yield* restoredSandbox.configureOutboundAuthority(authority.value);
			const includeHistory = attached !== void 0 || context.requiredCommits?.includes(desiredHead) === true && (desiredHead === selected.checkpoint.head || context.requiredCommits.includes(selected.checkpoint.head));
			yield* checkout.ensureCommit(attached?.repositoryName ?? readGeneration?.repositoryName ?? forkName, desiredHead, {
				includeHistory,
				retryDeadlineAtMillis: yield* pinnedReadRetryDeadline,
				...attached === void 0 ? readGeneration === void 0 ? {} : { repositoryId: readGeneration.repositoryId } : { repositoryId: attached.artifactsRepositoryId }
			});
			const recipe = yield* readRepositorySetup(desiredHead);
			const fingerprint = yield* repositorySetupFingerprint(desiredHead, recipe);
			if (selected.checkpoint.setupFingerprint !== fingerprint && (recipe?.requiredSecrets.length ?? 0) === 0) compatibleKey = {
				repository,
				classIdentity: description.classIdentity,
				imageDigest: description.imageDigest,
				head: desiredHead,
				setupFingerprint: fingerprint
			};
			const matches = repositorySetupCheckpointMatches(selected, recipe, fingerprint);
			if (matches) {
				restoredSetupInputs = {
					sourceHead: desiredHead,
					recipe,
					fingerprint
				};
				if (description.current?.instanceId !== identity.instanceId) validatedRestore = {
					instanceId: identity.instanceId,
					marker
				};
				if (includeHistory && plannedSource !== void 0 && storedForkIntent === null) restoredCommitWithHistory = desiredHead;
			}
			return matches;
		})).pipe(Effect.provideService(SessionSandbox, restoredSandbox), inMeasure("workspace-checkpoint-validate"))) break;
		const replacement = shared && !checkpointReselected && compatibleKey !== void 0 ? yield* findBaseline(compatibleKey) : null;
		yield* unresolved.destroy.pipe(inMeasure("workspace-checkpoint-discard"));
		checkpointRejected = replacement === null;
		if (replacement !== null) {
			selected = replacement;
			selectedWorkspaceCheckpoint = replacement.checkpoint;
			checkpointReselected = true;
		}
		identity = yield* unresolved.ensure({
			recovery: "freshAllowed",
			checkpoint: replacement?.checkpoint
		}).pipe(inPhase(agentPhases.containerBoot));
	}
	const restoredCheckpoint = selected?.checkpoint.snapshotId === identity.restoredSnapshotId ? selected : null;
	const restoredWorkspaceCheckpoint = selectedWorkspaceCheckpoint?.snapshotId === identity.restoredSnapshotId ? selectedWorkspaceCheckpoint : null;
	const containerReused = description.current?.instanceId === identity.instanceId;
	const containerStartMillis = containerReused || identity.timings.ownerReadyAt === null ? 0 : identity.timings.ownerReadyAt - identity.timings.nativeStartRequestedAt;
	const checkpointSource = selectedWorkspaceCheckpoint === null ? "none" : shared ? "shared" : "session";
	const checkpointRestore = checkpointRejected ? "rejected" : containerReused || selectedWorkspaceCheckpoint === null ? "not-requested" : restoredWorkspaceCheckpoint === null ? "miss" : "restored";
	yield* Effect.annotateCurrentSpan({
		"workspace.checkpoint.source": fromContinuation ? "continuation" : checkpointSource,
		"workspace.checkpoint.continuation": continuation === void 0 ? "not-requested" : fromContinuation && restoredCheckpoint !== null && !checkpointRejected ? "hit" : "miss",
		"workspace.checkpoint.restore": checkpointRestore,
		"workspace.checkpoint.snapshotId": restoredWorkspaceCheckpoint?.snapshotId ?? null,
		"workspace.checkpoint.reselected": checkpointReselected,
		"workspace.container.instanceId": identity.instanceId,
		"workspace.container.reused": containerReused,
		"workspace.container.startMillis": containerStartMillis
	});
	yield* mark("containerReady");
	if (shared && selected !== null && restoredCheckpoint === null && !checkpointRejected && description.current === null) yield* baselines.invalidate({
		repository,
		classIdentity: selected.checkpoint.classIdentity,
		imageDigest: selected.checkpoint.imageDigest,
		head: selected.checkpoint.head
	}, selected.checkpoint.snapshotId).pipe(Effect.catchCause((cause) => cause.reasons.every((reason) => Cause.isFailReason(reason) && Predicate.isTagged(reason.error, "WorkspaceBaselineError")) ? reportSanitizedCause(cause, { operation: "MessagePipeline.baselineInvalidation" }) : Effect.failCause(cause)));
	const sandbox = yield* sandboxFor(containerSize, sandboxNameFor(hash), {
		recovery: "exactWorkspaceRequired",
		expectedInstanceId: identity.instanceId
	}).pipe(inMeasure("sandbox-resolve"));
	const turnBody = Effect.gen(function* () {
		const authority = yield* (yield* OutboundAuthority).mint({
			sessionId: context.sessionId,
			turnId: context.turnId,
			instanceId: identity.instanceId
		});
		if (Option.isSome(authority)) yield* sandbox.configureOutboundAuthority(authority.value);
		const bootStep = validatedRestore?.instanceId === identity.instanceId ? Effect.succeed(validatedRestore.marker) : sandbox.readFileString(MATERIALIZATION_MARKER).pipe(Effect.map((content) => content.trim()), Effect.catchIf(isSandboxFileMissing, () => Effect.succeed(null)), inPhase(agentPhases.containerBoot));
		let installedCheckpoint;
		const prepareRepositoryWorkspace = Effect.gen(function* () {
			const ensured = ensuredFork;
			const baseHead = repositoryHead;
			if (baseHead === void 0 || baseHead === null) return yield* Effect.die("Missing admitted repository pin");
			const marker = yield* bootStep;
			yield* killStrayHarnessProcesses();
			const reuse = storedMaterialization !== null && marker === storedMaterialization && storedInstance === identity.instanceId;
			let head;
			let restored = false;
			let resumeToken = null;
			let harnessVersion = null;
			if (ensured !== null && !hadFork) yield* store.forkName.put(forkName);
			if (ensured !== null && ensured.disposition !== "existing") yield* mark("forkReady");
			yield* mark("sandboxResolved");
			if (reuse) {
				head = storedHead ?? baseHead;
				resumeToken = storedResumeToken;
				harnessVersion = yield* store.harnessVersion.get;
			} else {
				yield* ensureWorkspaceServices(true).pipe(Effect.provideService(TurnWorkspaceServices, []));
				yield* store.resumeToken.delete;
				const snapshot = restoredCheckpoint?.checkpoint;
				if (snapshot && marker === snapshot.materializationId) {
					head = context.initialCheckout ?? baseHead;
					yield* checkout.restoreSnapshotCheckout(forkName, head, snapshot.head).pipe(inMeasure("workspace-checkpoint-checkout"));
					restored = true;
					installedCheckpoint = restoredCheckpoint ?? void 0;
				} else {
					const cloned = yield* (plannedSource !== void 0 && readGeneration === plannedSource ? checkout.cloneSourceIntoSandbox(plannedSource, forkName, yield* pinnedReadRetryDeadline) : readGeneration !== void 0 ? checkout.cloneGenerationIntoSandbox(readGeneration) : checkout.cloneIntoSandbox(forkName, "workspace", recovery === null && generation !== null && storedMaterialization === null && storedHead === null && storedResumeToken === null && storedSetup === null && storedCheckpoint === null ? {
						source: generation,
						forkHead: baseHead
					} : void 0)).pipe(inPhase(agentPhases.clone));
					if (plannedSource !== void 0 && cloned.head !== baseHead) return yield* RepositoryForkIntentError.make({ reason: "target-mismatch" });
					head = cloned.head;
					restored = ensured?.disposition === "existing" || hadFork || storedMaterialization !== null;
				}
				yield* mark("cloneComplete");
				const materializationId = crypto.randomUUID();
				yield* sandbox.writeFile(MATERIALIZATION_MARKER, materializationId).pipe(inMeasure("marker-write"));
				yield* store.materializationId.put(materializationId);
				yield* store.containerInstanceId.put(identity.instanceId);
				yield* store.verifiedHead.put(head);
			}
			for (const commit of context.requiredCommits ?? []) {
				if (installedCheckpoint !== void 0 && commit === restoredCommitWithHistory) continue;
				yield* checkout.ensureCommit(readGeneration?.repositoryName ?? forkName, commit, {
					includeHistory: true,
					retryDeadlineAtMillis: yield* pinnedReadRetryDeadline,
					...readGeneration === void 0 ? {} : { repositoryId: readGeneration.repositoryId }
				});
			}
			if (context.initialCheckout !== void 0 && head !== context.initialCheckout) {
				yield* checkout.checkoutCommit(context.initialCheckout);
				head = context.initialCheckout;
			}
			return {
				marker,
				reuse,
				head,
				restored,
				resumeToken,
				harnessVersion,
				forkDisposition: ensured?.disposition ?? "indeterminate",
				forkPollAttempts: ensured === null || ensured.disposition === "existing" ? null : ensured.pollAttempts,
				baseHead,
				persist: persistRepository(baseHead)
			};
		});
		const prepareIndependentWorkspace = Effect.gen(function* () {
			const marker = yield* bootStep;
			yield* killStrayHarnessProcesses();
			const sourceHead = attached === void 0 ? null : storedHead ?? context.initialCheckout ?? attached.commitSha;
			const reuse = storedMaterialization !== null && marker === storedMaterialization && storedInstance === identity.instanceId && (attached === void 0 || storedHead === sourceHead);
			if (attached !== void 0) {
				if ((yield* checkout.sourceGeneration(attached.repositoryName)).repositoryId !== attached.artifactsRepositoryId) return yield* HarnessTurnError.make({
					reason: "turnFailed",
					message: "The attached repository generation changed"
				});
			}
			if (!reuse) {
				yield* ensureWorkspaceServices(true).pipe(Effect.provideService(TurnWorkspaceServices, []));
				yield* store.resumeToken.delete;
				if (attached === void 0) {
					yield* sandbox.mkdir(REPO_DIR, { recursive: true });
					yield* store.verifiedHead.delete;
				} else {
					const snapshot = restoredCheckpoint?.checkpoint;
					if (snapshot !== void 0 && marker === snapshot.materializationId) {
						yield* checkout.restoreSnapshotCheckout(attached.repositoryName, sourceHead, snapshot.head).pipe(inMeasure("workspace-checkpoint-checkout"));
						installedCheckpoint = restoredCheckpoint ?? void 0;
					} else {
						yield* checkout.cloneGenerationIntoSandbox({
							repositoryName: attached.repositoryName,
							repositoryId: attached.artifactsRepositoryId
						}).pipe(inPhase(agentPhases.clone));
						yield* checkout.ensureCommit(attached.repositoryName, sourceHead, {
							includeHistory: true,
							repositoryId: attached.artifactsRepositoryId,
							retryDeadlineAtMillis: yield* pinnedReadRetryDeadline
						});
						yield* checkout.checkoutCommit(sourceHead);
					}
					yield* store.verifiedHead.put(sourceHead);
				}
				yield* mark("cloneComplete");
				const materializationId = crypto.randomUUID();
				yield* sandbox.writeFile(MATERIALIZATION_MARKER, materializationId);
				yield* store.materializationId.put(materializationId);
				yield* store.containerInstanceId.put(identity.instanceId);
			}
			if (attached !== void 0) for (const commit of context.requiredCommits ?? []) yield* checkout.ensureCommit(attached.repositoryName, commit, {
				includeHistory: true,
				repositoryId: attached.artifactsRepositoryId,
				retryDeadlineAtMillis: yield* pinnedReadRetryDeadline
			});
			return {
				marker,
				reuse,
				head: sourceHead,
				baseHead: sourceHead,
				restored: !reuse && (storedMaterialization !== null || restoredCheckpoint !== null),
				resumeToken: reuse ? storedResumeToken : null,
				harnessVersion: reuse ? yield* store.harnessVersion.get : null,
				forkDisposition: "indeterminate",
				forkPollAttempts: null,
				persist: attached === void 0 ? Effect.succeed({
					head: null,
					changedPaths: []
				}) : checkout.persistGeneration({
					repositoryName: attached.repositoryName,
					repositoryId: attached.artifactsRepositoryId
				}, sourceHead)
			};
		});
		const prepareGenerationWorkspace = (generationSource) => Effect.gen(function* () {
			if (storedFork !== null) return yield* Effect.die("A generation-bound run cannot own a task repository fork");
			const sourceHead = generationSource.draft?.sourceSha ?? generationSource.acceptedSourceSha;
			const marker = yield* bootStep;
			const generationCheckout = generationCheckouts.forSource(generationSource);
			const checkpoint = yield* GenerationSourceCheckpoint;
			if (checkpoint === null) return yield* Effect.die("A generation-bound turn requires a durable GenerationSourceCheckpoint");
			yield* killStrayHarnessProcesses();
			const reuse = storedMaterialization !== null && marker === storedMaterialization && storedInstance === identity.instanceId && storedHead === sourceHead;
			let harnessVersion;
			let resumeToken = null;
			if (reuse) {
				harnessVersion = yield* store.harnessVersion.get;
				resumeToken = storedResumeToken;
			} else {
				yield* store.resumeToken.delete;
				if (restoredWorkspaceCheckpoint?.head !== sourceHead || marker !== restoredWorkspaceCheckpoint.materializationId) yield* generationCheckout.restore.pipe(inPhase(agentPhases.clone));
				else yield* checkout.checkoutCommit(sourceHead);
				yield* mark("cloneComplete");
				harnessVersion = null;
				yield* store.verifiedHead.put(sourceHead);
				const materializationId = crypto.randomUUID();
				yield* sandbox.writeFile(MATERIALIZATION_MARKER, materializationId).pipe(inMeasure("marker-write"));
				yield* store.materializationId.put(materializationId);
				yield* store.containerInstanceId.put(identity.instanceId);
			}
			yield* mark("sandboxResolved");
			return {
				marker,
				reuse,
				head: sourceHead,
				restored: !reuse && storedMaterialization !== null,
				resumeToken,
				harnessVersion,
				forkDisposition: "indeterminate",
				forkPollAttempts: null,
				baseHead: sourceHead,
				persist: generationCheckout.persist(({ head: preparedHead }) => checkpoint.prepared(preparedHead))
			};
		});
		const recoverWorkspace = Effect.gen(function* () {
			if (recovery === null) return yield* Effect.die("Missing workspace recovery receipt");
			const marker = yield* bootStep;
			if (marker !== recovery.materializationId || storedMaterialization !== recovery.materializationId || recovery.instanceId !== identity.instanceId || storedInstance !== identity.instanceId) return yield* HarnessTurnError.make({
				reason: "turnFailed",
				message: "The sandbox workspace was replaced during the active turn; its input was not replayed"
			});
			const generationSource = context.generationSource;
			const persist = repositoryFree ? Effect.succeed({
				head: null,
				changedPaths: []
			}) : attached !== void 0 ? checkout.persistGeneration({
				repositoryName: attached.repositoryName,
				repositoryId: attached.artifactsRepositoryId
			}, recovery.baseHead) : generationSource === void 0 ? persistRepository(recovery.baseHead) : generationCheckouts.forSource(generationSource).persist(Effect.fnUntraced(function* ({ head }) {
				const checkpoint = yield* GenerationSourceCheckpoint;
				if (checkpoint === null) return yield* Effect.die("Missing generation handoff owner");
				yield* checkpoint.prepared(head);
			}));
			return {
				marker,
				reuse: true,
				head: recovery.head,
				baseHead: recovery.baseHead,
				restored: false,
				resumeToken: recovery.resumeToken,
				harnessVersion: recovery.harnessVersion,
				forkDisposition: "indeterminate",
				forkPollAttempts: null,
				persist
			};
		});
		const prepared = recovery !== null ? yield* recoverWorkspace : repositoryFree || attached !== void 0 ? yield* prepareIndependentWorkspace : context.generationSource === void 0 ? yield* prepareRepositoryWorkspace : yield* prepareGenerationWorkspace(context.generationSource);
		const { marker, reuse, head, restored, resumeToken } = prepared;
		let harnessVersion = prepared.harnessVersion;
		let setupReport;
		yield* mark("repositoryReady");
		const prepareSetup = recovery === null && (seeded || attached !== void 0);
		let share = false;
		if (prepareSetup) {
			share = storedMaterialization === null && storedHead === null && storedResumeToken === null && storedSetup === null && context.initialCheckout === void 0 && (attached !== void 0 || !hadFork && generation !== null && (seed !== void 0 || (context.requiredCommits ?? []).every((commit) => commit === head)) && head === (plannedSource === void 0 ? yield* checkout.readGenerationMainHead(generation) : plannedSource.commitSha));
			yield* Effect.annotateCurrentSpan("workspace.checkpoint.shareEligible", share);
		}
		if (prepareSetup) setupReport = yield* prepareRepositorySetup({
			execution: {
				sessionId: context.sessionId,
				turnId: context.turnId,
				instanceId: identity.instanceId,
				...context.repositoryId === void 0 ? {} : { repositoryId: context.repositoryId }
			},
			sourceKey: attached?.repositoryName ?? (yield* requireRepositorySeed(seedRepository)),
			sourceHead: head,
			startPreview: context.startRepositoryPreview,
			installed: installedCheckpoint,
			inputs: installedCheckpoint === void 0 ? void 0 : restoredSetupInputs,
			snapshot: reuse ? void 0 : {
				repository,
				identity,
				share
			}
		}).pipe(inPhase(agentPhases.repositorySetup));
		yield* mark("repositoryPrepared");
		yield* Effect.annotateCurrentSpan({
			"workspace.readiness.turnToPreparedMillis": timings.repositoryPrepared,
			"workspace.readiness.repositorySetup": setupReport?.state ?? "not-required"
		});
		if (recovery === null && context.generationSource !== void 0 && !reuse) {
			const materializationId = yield* store.materializationId.get;
			if (materializationId === null) return yield* Effect.die("Missing prepared generation materialization");
			const setupFingerprint = yield* resourceHash(`${WORKSPACE_PREPARATION_VERSION}:site-source:${head}`);
			yield* sandbox.checkpoint({
				captureId: `site-source:${identity.instanceId}:${materializationId}:${setupFingerprint}`,
				instanceId: identity.instanceId,
				setupFingerprint,
				head,
				materializationId,
				snapshotAllowed: true
			}).pipe(Effect.flatMap((checkpoint) => checkpoint === null ? Effect.void : store.workspaceCheckpoint.put(checkpoint)), Effect.catchCause((cause) => cause.reasons.every((reason) => Cause.isFailReason(reason) && Predicate.isTagged(reason.error, "SandboxOperationError")) ? reportSanitizedCause(cause, { operation: "MessagePipeline.siteCapture" }) : Effect.failCause(cause)));
		}
		if (recovery === null && harnessVersion === null) {
			harnessVersion = harness.version;
			yield* store.harnessVersion.put(harnessVersion);
		}
		const model = context.model ?? config.defaultModel;
		const reasoningEffort = context.reasoningEffort ?? "medium";
		const fast = context.fast ?? true;
		const observed = {
			fork: prepared.forkDisposition === "indeterminate" ? "unknown" : prepared.forkDisposition,
			forkPollAttempts: prepared.forkPollAttempts,
			marker: storedMaterialization === null ? "no-stored-materialization" : marker === null ? "absent" : marker === storedMaterialization ? "matched" : "mismatched",
			workspace: reuse ? "reused-in-place" : restored ? "restored-clone" : "fresh-clone",
			harnessSession: resumeToken === null ? "newly-started" : "resumed"
		};
		if (lifecycle !== null) yield* Ref.update(lifecycle, (capture) => ({
			...capture,
			observed
		}));
		const materializationId = yield* store.materializationId.get;
		if (materializationId === null) return yield* Effect.die("Prepared workspace has no materialization identity");
		let checkpoint = recovery ?? {
			id: context.turnId,
			instanceId: identity.instanceId,
			materializationId,
			head,
			baseHead: prepared.baseHead,
			resumeToken,
			harnessVersion,
			startedAtMillis: startedAt,
			persisted: null
		};
		yield* store.pipelineTurn.put(checkpoint);
		yield* output.emit({
			type: "agent.ready",
			sessionId: context.sessionId,
			restored,
			materialization: reuse ? "reused" : restored ? "restored" : "fresh",
			head,
			harness: harness.name,
			harnessVersion: harnessVersion ?? "unknown",
			model,
			reasoningEffort,
			fast,
			containerSize,
			readiness: {
				startedAtMillis: startedAt,
				timings: { ...timings },
				containerReused,
				containerStartMillis,
				checkpointSource,
				checkpointRestore,
				...plannedSource === void 0 ? {} : { fork: (yield* Ref.get(forkState)).state },
				installation: recovery !== null && storedSetup?.prepared ? "in-place" : setupReport?.installation ?? "not-required",
				capture: setupReport?.checkpoint?.capture ?? "not-requested",
				publication: setupReport?.checkpoint?.publication ?? "not-requested"
			},
			lifecycle: {
				preTurn,
				observed
			}
		});
		const firstEventSeen = yield* Ref.make(false);
		const steering = yield* TurnSteering;
		const harnessTurn = checkForkBeforeModel.pipe(Effect.andThen(mark("harnessStarted")), Effect.andThen(harness.runTurn({
			turnId: context.turnId,
			recover: context.recover,
			prompt: context.text,
			resumeToken,
			model,
			reasoningEffort,
			fast,
			...context.serviceTier === void 0 ? {} : { serviceTier: context.serviceTier },
			deadlinePolicy,
			...context.restrictModelRequests === void 0 ? {} : { restrictModelRequests: context.restrictModelRequests },
			...context.sentryMcp !== true || config.sentryMcp === void 0 ? {} : { sentryMcp: config.sentryMcp },
			...config.commitCoauthorTrailer === void 0 ? {} : { commitCoauthorTrailer: config.commitCoauthorTrailer },
			...context.outputSchema === void 0 ? {} : { outputSchema: context.outputSchema },
			...context.deadlineAtMs === void 0 ? {} : { deadlineAtMs: context.deadlineAtMs },
			...steering === null ? {} : { onSteeringReady: steering.ready },
			onEvent: (event) => Effect.gen(function* () {
				if (!(yield* Ref.getAndSet(firstEventSeen, true))) yield* mark("firstHarnessEvent");
				yield* output.emit(event);
			})
		})), Effect.tapError((error) => Predicate.isObject(error) && Predicate.hasProperty(error, "_tag") && error._tag === "HarnessTurnError" && Predicate.hasProperty(error, "reason") && (error.reason === "freshSessionRequired" || error.reason === "deadlineExceeded") ? store.resumeToken.delete.pipe(Effect.asVoid) : Effect.void), inPhase(agentPhases.harness));
		const servicesStep = (context.generationSource === void 0 ? Effect.void : ensureWorkspaceServices(!reuse)).pipe(Effect.catchCause((cause) => Effect.logError("sandbox-agent workspace service start failed", cause)));
		const [turn] = yield* Effect.all([harnessTurn, servicesStep], { concurrency: "unbounded" });
		yield* mark("harnessExit");
		yield* store.resumeToken.put(turn.resumeToken);
		const persisted = checkpoint.persisted ?? (yield* prepared.persist);
		checkpoint = {
			...checkpoint,
			persisted
		};
		yield* store.pipelineTurn.put(checkpoint);
		yield* complete(persisted, turn);
	});
	yield* withSandboxLease(context.turnId, turnBody).pipe(Effect.provideService(SessionSandbox, sandbox));
}, Effect.scoped);
/** @deprecated Compatibility adapter for hosts migrating to runTurn and its inward ports. */
const runMessagePipeline = (context) => {
	const { lifecycle, onSteeringReady, onGenerationSourcePrepared, ...request } = context;
	return runTurn(request).pipe(Effect.provideService(TurnLifecycleCapture, lifecycle), Effect.provideService(TurnSteering, onSteeringReady === void 0 ? null : { ready: onSteeringReady }), Effect.provideService(GenerationSourceCheckpoint, onGenerationSourcePrepared === void 0 ? null : { prepared: onGenerationSourcePrepared }));
};
//#endregion
export { GenerationSourceCheckpoint, TurnLifecycleCapture, TurnSteering, describeFailure, finishLifecycle, runMessagePipeline, runTurn, summarizeRun };
