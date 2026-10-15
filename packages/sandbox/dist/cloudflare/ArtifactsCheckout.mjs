import { agentPhases } from "../Contract.mjs";
import { toJson } from "../Http.mjs";
import { SessionSandbox } from "../SessionSandbox.mjs";
import { REPO_DIR, WORKSPACE_DIR } from "../workspace-paths.mjs";
import { sanitizeCause, withFailureDiagnostic } from "../observability/FailureDiagnostic.mjs";
import { reportSanitizedCause } from "../observability/RuntimeDiagnostics.mjs";
import { HarnessTurnError } from "../harness/Harness.mjs";
import { RepositoryMutationFence, RepositoryMutationFenceFailure } from "../repository/RepositoryMutationFence.mjs";
import { transferRepositoryGit } from "../repository/RepositoryGitTransfer.mjs";
import { annotateSpanAttempts, inMeasure, inPhase, markPoint } from "../tracing/RunSpanCapture.mjs";
import { ArtifactsOperationTimeoutError, ForkNotReadyError, artifactsForkPolicyFromDeadlines, artifactsMainHead, awaitArtifactsForkReady, isForkProvisioning, requestArtifactsFork } from "./ArtifactsFork.mjs";
import { SandboxAgentConfig, requireRepositorySeed, sandboxAgentDeadlinePolicy } from "./Config.mjs";
import { resourceHash } from "./resource-names.mjs";
import { SandboxAgentArtifacts } from "./Bindings.mjs";
import { RepositoryPersistenceCheckpoint } from "./RepositoryPersistenceCheckpoint.mjs";
import { SessionStateStore } from "./SessionStateStore.mjs";
import { acknowledgeWorkspacePreparation, runWorkspacePreparation } from "./WorkspacePreparation.mjs";
import { assertRepositorySetupSecretsNotPersisted } from "./RepositorySetup.mjs";
import { runSandboxCommand } from "./SandboxCommand.mjs";
import { Cause, Clock, Context, Effect, Layer, Redacted, Schedule, Schema } from "effect";
import { FetchHttpClient } from "effect/http";
//#region src/cloudflare/ArtifactsCheckout.ts
/**
* Durable source state through Cloudflare Artifacts: fork the immutable seed,
* materialize a disposable sandbox workspace, persist committed work
* back, and verify every push through the binding before success is claimed.
*/
const MINIMUM_TOKEN_TTL_SECONDS = 300;
const AGENT_COMMIT_AUTHOR = "Sandbox Agent";
const AGENT_COMMIT_EMAIL = "sandbox-agent@localhost";
const GIT_OUTPUT_LIMIT_BYTES = 524288;
const CHANGED_PATHS_LIMIT_BYTES = 131072;
const GIT_READ_RETRY_COUNT = 2;
const GIT_TRANSPORT_DIR = `${WORKSPACE_DIR}/.kommunikasie-git-transport`;
const GIT_TRANSPORT_REPOSITORY = `${GIT_TRANSPORT_DIR}/repository.git`;
const isolatedGitTransportEnv = {
	GIT_CEILING_DIRECTORIES: GIT_TRANSPORT_DIR,
	GIT_CONFIG_GLOBAL: "/dev/null",
	GIT_CONFIG_NOSYSTEM: "1",
	GIT_CONFIG_SYSTEM: "/dev/null"
};
/** A sandboxed Git command exited unsuccessfully; arguments are never retained. */
var GitCommandError = class extends Schema.TaggedError()("GitCommandError", {
	command: Schema.String,
	exitCode: Schema.Int,
	stderr: Schema.String,
	status: Schema.optionalKey(Schema.Int.check(Schema.isBetween({
		minimum: 400,
		maximum: 599
	})))
}) {};
/** Persisting every changed path would exceed the lifecycle event's safe bound. */
var PersistedPathsLimitError = class extends Schema.TaggedError()("PersistedPathsLimitError", {
	pathCount: Schema.Int,
	observedBytes: Schema.Int,
	maxBytes: Schema.Int
}) {};
/** Artifacts read-back did not match the commit that Git reported as pushed. */
var PushVerificationError = class extends Schema.TaggedError()("PushVerificationError", {
	forkName: Schema.String,
	localHead: Schema.String,
	remoteHead: Schema.String
}) {};
/** The sandbox tried to replace main instead of advancing its accepted head. */
var NonFastForwardPushError = class extends Schema.TaggedError()("NonFastForwardPushError", {
	forkName: Schema.String,
	acceptedHead: Schema.String,
	localHead: Schema.String
}) {};
/** A staged generation handoff can no longer find its exact commit in the preserved workspace. */
var PreparedCommitUnavailableError = class extends Schema.TaggedError()("PreparedCommitUnavailableError", {
	repositoryName: Schema.String,
	preparedHead: Schema.String
}) {};
/** A mutable repository name no longer resolves to its authorized provider generation. */
var ArtifactsRepositoryGenerationMismatchError = class extends Schema.TaggedError()("ArtifactsRepositoryGenerationMismatchError", {
	repositoryName: Schema.String,
	expectedRepositoryId: Schema.NonEmptyString
}) {};
var RepositoryForkIntentError = class extends Schema.TaggedError()("RepositoryForkIntentError", { reason: Schema.Literals([
	"source-pin",
	"intent-mismatch",
	"target-mismatch"
]) }) {};
/** Release/fence ports cannot fail with a typed error; retain every original reason. */
const failuresAsDefects = (cause) => Cause.fromReasons(cause.reasons.map((reason) => Cause.isFailReason(reason) ? Cause.makeDieReason(reason.error) : reason));
/**
* Credentials remain in the native owner's outbound authority. Commands use
* an isolated transport with HTTPS, redirects and hooks explicitly bounded.
*/
const gitTransportEnv = (remote) => ({
	...isolatedGitTransportEnv,
	ALL_PROXY: "",
	GIT_ALLOW_PROTOCOL: "https",
	GIT_CONFIG_COUNT: "13",
	GIT_CONFIG_KEY_0: `http.${remote}.extraHeader`,
	GIT_CONFIG_VALUE_0: "",
	GIT_CONFIG_KEY_1: "core.hooksPath",
	GIT_CONFIG_VALUE_1: "/dev/null",
	GIT_CONFIG_KEY_2: "credential.helper",
	GIT_CONFIG_VALUE_2: "",
	GIT_CONFIG_KEY_3: `http.${remote}.proxy`,
	GIT_CONFIG_VALUE_3: "",
	GIT_CONFIG_KEY_4: `http.${remote}.sslVerify`,
	GIT_CONFIG_VALUE_4: "true",
	GIT_CONFIG_KEY_5: `http.${remote}.followRedirects`,
	GIT_CONFIG_VALUE_5: "false",
	GIT_CONFIG_KEY_6: `http.${remote}.curloptResolve`,
	GIT_CONFIG_VALUE_6: "",
	GIT_CONFIG_KEY_7: `url.${remote}.insteadOf`,
	GIT_CONFIG_VALUE_7: remote,
	GIT_CONFIG_KEY_8: "protocol.allow",
	GIT_CONFIG_VALUE_8: "never",
	GIT_CONFIG_KEY_9: "protocol.https.allow",
	GIT_CONFIG_VALUE_9: "always",
	GIT_CONFIG_KEY_10: "pack.window",
	GIT_CONFIG_VALUE_10: "0",
	GIT_CONFIG_KEY_11: "pack.depth",
	GIT_CONFIG_VALUE_11: "0",
	GIT_CONFIG_KEY_12: "pack.allowPackReuse",
	GIT_CONFIG_VALUE_12: "false",
	GIT_SSL_NO_VERIFY: "0",
	GIT_TERMINAL_PROMPT: "0",
	HTTPS_PROXY: "",
	HTTP_PROXY: "",
	NO_PROXY: "*",
	all_proxy: "",
	https_proxy: "",
	http_proxy: "",
	no_proxy: "*"
});
const sanitizeGitDiagnostic = (value) => value.replace(/Authorization:\s*Bearer\s+\S+/giu, "Authorization: Bearer [redacted]").replace(/https:\/\/[^\s/@]+@/giu, "https://[redacted]@");
const gitCommandError = (command, output) => {
	const text = output.stderr.trim() === "" ? output.stdout : output.stderr;
	const flags = [output.timedOut ? "process timed out" : null, output.truncated ? "output truncated" : null].filter((value) => value !== null);
	const diagnostic = `${sanitizeGitDiagnostic(text).trim()}${flags.length === 0 ? "" : `${text.trim() === "" ? "" : "\n"}${flags.join(", ")}`}`;
	const httpFailure = /The requested URL returned error: ([45][0-9]{2})(?:\s|$)/u.exec(text);
	return GitCommandError.make({
		command,
		exitCode: output.exitCode ?? -1,
		stderr: diagnostic.slice(-2048),
		...httpFailure === null ? {} : { status: Number(httpFailure[1]) }
	});
};
const statusAndHead = (output) => {
	let head = null;
	let dirty = false;
	for (const line of output.split("\n")) if (line.startsWith("# branch.oid ")) {
		const value = line.slice(13).trim();
		if (value !== "(initial)" && value !== "") head = value;
	} else if (line.trim() !== "" && !line.startsWith("# ")) dirty = true;
	return head === null ? null : {
		dirty,
		head
	};
};
/** Host-owned checkout root; never supplied by model-generated file paths. */
const ArtifactsCheckoutDirectory = Context.Reference("sandbox-agent/ArtifactsCheckoutDirectory", { defaultValue: () => REPO_DIR });
const reportCleanupFailure = (cause, operation) => {
	if (cause.reasons.every(Cause.isInterruptReason)) return Effect.void;
	return reportSanitizedCause(cause, { operation });
};
const clearRepositoryAuthorization = Effect.fn("ArtifactsCheckout.clearRepositoryAuthorization")(function* (grant) {
	yield* (yield* SessionSandbox).clearRepositoryAuthorization(grant).pipe(Effect.tapCause((cause) => reportCleanupFailure(cause, "ArtifactsCheckout.clearRepositoryAuthorization")), Effect.ignore);
});
const make = Effect.gen(function* () {
	const repositoryDirectory = yield* ArtifactsCheckoutDirectory;
	const artifacts = yield* SandboxAgentArtifacts;
	const config = yield* SandboxAgentConfig;
	const deadlines = sandboxAgentDeadlinePolicy(config);
	const forkPolicy = artifactsForkPolicyFromDeadlines(deadlines);
	const tokenTtlSeconds = Math.max(MINIMUM_TOKEN_TTL_SECONDS, Math.ceil((deadlines.gitCommandMillis + 2 * deadlines.artifactsOperationMillis) / 1e3));
	const remoteFor = (forkName) => `https://${config.artifactsHostname}/git/${config.artifactsNamespace}/${forkName}.git`;
	const boundedArtifacts = (operation) => (effect) => effect.pipe(Effect.timeoutOrElse({
		duration: deadlines.artifactsOperationMillis,
		orElse: () => ArtifactsOperationTimeoutError.make({
			operation,
			timeoutMillis: deadlines.artifactsOperationMillis
		})
	}), (bounded) => withFailureDiagnostic(bounded, { operation: `ArtifactsCheckout.${operation.replaceAll(":", ".")}` }));
	const verifyRepositoryMetadata = Effect.fnUntraced(function* (metadata, generation) {
		if (metadata.id !== generation.repositoryId || metadata.name !== generation.repositoryName || metadata.remote !== remoteFor(generation.repositoryName)) return yield* ArtifactsRepositoryGenerationMismatchError.make({
			repositoryName: generation.repositoryName,
			expectedRepositoryId: generation.repositoryId
		});
	});
	const verifyRepositoryGeneration = Effect.fn("ArtifactsCheckout.verifyRepositoryGeneration")(function* (repository, generation) {
		yield* verifyRepositoryMetadata(yield* repository.info().pipe(boundedArtifacts("verifyRepositoryGeneration")), generation);
	});
	const readPinnedRepository = Effect.fnUntraced(function* (generation, query, retryDeadlineAtMillis) {
		const operation = query._tag === "Commit" ? "readPinnedCommit" : "readSourcePin";
		const repository = yield* artifacts.get(generation.repositoryName).pipe(boundedArtifacts(query._tag === "Commit" ? "getCommitRepository" : "getSourcePin"));
		yield* verifyRepositoryMetadata(repository, generation);
		const read = Effect.gen(function* () {
			if (query._tag === "Commit") return {
				pin: (yield* repository.readCommit(query.commit).pipe(boundedArtifacts(operation))).hash,
				main: null
			};
			const [commits, main] = yield* Effect.all([repository.log({
				ref: query.branch,
				limit: 1
			}).pipe(boundedArtifacts(operation)), query.includeMain && query.branch !== "main" ? repository.log({
				ref: "main",
				limit: 1
			}).pipe(boundedArtifacts("readGenerationMainHead")) : Effect.succeed([])], { concurrency: "unbounded" });
			yield* verifyRepositoryGeneration(repository, generation);
			const pin = commits[0]?.hash ?? null;
			return {
				pin,
				main: query.branch === "main" ? pin : main[0]?.hash ?? null
			};
		});
		return yield* read.pipe(Effect.catchCause((original) => Effect.gen(function* () {
			if (retryDeadlineAtMillis === void 0 || original.reasons.length === 0 || !original.reasons.every((reason) => Cause.isFailReason(reason) && reason.error._tag === "ArtifactsOperationTimeoutError" && (reason.error.operation === operation || query._tag === "Heads" && reason.error.operation === "readGenerationMainHead"))) return yield* Effect.failCause(original);
			const remaining = Math.min(deadlines.artifactsOperationMillis, retryDeadlineAtMillis - (yield* Clock.currentTimeMillis));
			if (!(remaining > 0)) return yield* Effect.failCause(original);
			return yield* Effect.gen(function* () {
				yield* verifyRepositoryGeneration(repository, generation);
				const result = yield* read;
				if (query._tag === "Commit") yield* verifyRepositoryGeneration(repository, generation);
				return result;
			}).pipe(Effect.timeoutOrElse({
				duration: remaining,
				orElse: () => ArtifactsOperationTimeoutError.make({
					operation,
					timeoutMillis: remaining
				})
			}), (recovery) => withFailureDiagnostic(recovery, { operation: `ArtifactsCheckout.${operation}` }), Effect.catchCause((recovery) => Effect.failCause(Cause.fromReasons([...original.reasons, ...recovery.reasons]))));
		})));
	});
	const runGit = Effect.fn("ArtifactsCheckout.runGit")(function* (args, options) {
		const timeout = options?.deadlineAt === void 0 ? deadlines.gitCommandMillis : Math.max(1, options.deadlineAt - (yield* Clock.currentTimeMillis));
		const output = yield* options?.inspect ? (yield* SessionSandbox).inspect(["git", ...args], {
			cwd: options.cwd ?? repositoryDirectory,
			timeout,
			deadlineAt: options.deadlineAt,
			maxBytes: GIT_OUTPUT_LIMIT_BYTES
		}) : runSandboxCommand(["git", ...args], {
			cwd: options?.cwd ?? repositoryDirectory,
			env: options?.env,
			timeout,
			deadlineAt: options?.deadlineAt
		}, {
			timeout,
			maxBytes: GIT_OUTPUT_LIMIT_BYTES,
			operation: "ArtifactsCheckout.runGit"
		});
		if (output.exitCode !== 0 || output.timedOut || options?.requireCompleteOutput === true && output.truncated) return yield* gitCommandError(args[0] ?? "git", output);
		return output.stdout;
	});
	const resetGitTransportDirectory = Effect.fn("ArtifactsCheckout.resetGitTransportDirectory")(function* () {
		const sandbox = yield* SessionSandbox;
		const output = yield* (yield* sandbox.exec([
			"rm",
			"-rf",
			GIT_TRANSPORT_DIR
		], {
			cwd: WORKSPACE_DIR,
			timeout: deadlines.gitCommandMillis
		})).outputText({
			timeout: deadlines.gitCommandMillis,
			maxBytes: GIT_OUTPUT_LIMIT_BYTES
		});
		if (output.exitCode !== 0 || output.timedOut) {
			const diagnostic = sanitizeGitDiagnostic(output.stderr.trim() === "" ? output.stdout : output.stderr);
			return yield* GitCommandError.make({
				command: "git-transport-reset",
				exitCode: output.exitCode,
				stderr: diagnostic.slice(-2048)
			});
		}
		yield* sandbox.mkdir(GIT_TRANSPORT_DIR);
	});
	const withRepoToken = Effect.fn("ArtifactsCheckout.withRepoToken")(function* (forkName, scope, use, expectedRepositoryId) {
		const repo = yield* artifacts.get(forkName).pipe(boundedArtifacts("getTokenRepository"));
		const sandbox = yield* SessionSandbox;
		const remote = remoteFor(forkName);
		const generation = expectedRepositoryId === void 0 ? void 0 : {
			repositoryName: forkName,
			repositoryId: expectedRepositoryId
		};
		if (generation !== void 0) yield* verifyRepositoryGeneration(repo, generation);
		return yield* Effect.acquireUseRelease(repo.createToken(scope, tokenTtlSeconds).pipe(boundedArtifacts(`createToken:${scope}`), inMeasure(`git-token-mint-${scope}`)), (minted) => Effect.gen(function* () {
			if (generation !== void 0) yield* verifyRepositoryGeneration(repo, generation);
			const deadlineAt = (yield* Clock.currentTimeMillis) + deadlines.gitCommandMillis;
			return yield* Effect.acquireUseRelease(sandbox.setRepositoryAuthorization(remote, `Bearer ${minted.plaintext}`, deadlineAt, minted.id), () => Effect.gen(function* () {
				const result = yield* use(deadlineAt);
				if (generation !== void 0) yield* verifyRepositoryGeneration(repo, generation);
				return result;
			}), clearRepositoryAuthorization);
		}), (minted) => repo.revokeToken(minted.id).pipe(boundedArtifacts(`revokeToken:${scope}`), Effect.tapCause((cause) => reportCleanupFailure(cause, "ArtifactsCheckout.revokeToken")), Effect.ignore, inMeasure(`git-token-revoke-${scope}`)));
	});
	const boundGenerationMainHead = Effect.fn("ArtifactsCheckout.boundGenerationMainHead")(function* (repository, generation) {
		const commits = yield* repository.log({
			ref: "main",
			limit: 1
		}).pipe(boundedArtifacts("readGenerationMainHead"));
		yield* verifyRepositoryGeneration(repository, generation);
		return commits[0]?.hash ?? null;
	});
	const generationMainHead = Effect.fn("ArtifactsCheckout.generationMainHead")(function* (generation) {
		const repository = yield* artifacts.get(generation.repositoryName).pipe(boundedArtifacts("getHeadRepository"));
		yield* verifyRepositoryMetadata(repository, generation);
		return yield* boundGenerationMainHead(repository, generation);
	});
	const sourceGeneration = Effect.fn("ArtifactsCheckout.sourceGeneration")(function* (repositoryName) {
		const repository = yield* artifacts.get(repositoryName).pipe(boundedArtifacts("getSourceGeneration"));
		const generation = {
			repositoryName,
			repositoryId: (yield* repository.info().pipe(boundedArtifacts("readSourceGeneration"))).id
		};
		yield* verifyRepositoryGeneration(repository, generation);
		return generation;
	});
	const readSourceHeads = Effect.fnUntraced(function* (source, includeMain, retryDeadlineAtMillis) {
		const ref = source.artifactsRef ?? "refs/heads/main";
		if (ref.startsWith("refs/") && !ref.startsWith("refs/heads/")) return yield* RepositoryForkIntentError.make({ reason: "source-pin" });
		const heads = yield* readPinnedRepository(source, {
			_tag: "Heads",
			branch: ref.replace(/^refs\/heads\//u, ""),
			includeMain
		}, retryDeadlineAtMillis);
		if (heads.pin !== source.commitSha) return yield* RepositoryForkIntentError.make({ reason: "source-pin" });
		return {
			pin: source.commitSha,
			main: heads.main
		};
	});
	const readSourcePin = Effect.fn("ArtifactsCheckout.readSourcePin")(function* (source, retryDeadlineAtMillis) {
		return (yield* readSourceHeads(source, false, retryDeadlineAtMillis)).pin;
	});
	const retainFork = Effect.fn("ArtifactsCheckout.retainFork")(function* (forkName, source, taskDeadlineAtMillis) {
		const store = yield* SessionStateStore;
		const ref = source.artifactsRef ?? "refs/heads/main";
		const artifactsRef = ref.startsWith("refs/") ? ref : `refs/heads/${ref}`;
		const retained = yield* store.repositoryFork.get;
		if (retained !== null) {
			if (retained.forkName !== forkName || retained.source.repositoryName !== source.repositoryName || retained.source.repositoryId !== source.repositoryId || retained.source.commitSha !== source.commitSha || retained.source.artifactsRef !== artifactsRef) return yield* RepositoryForkIntentError.make({ reason: "intent-mismatch" });
			return retained;
		}
		const requestedAtMillis = yield* Clock.currentTimeMillis;
		const deadlineAtMillis = Math.min(taskDeadlineAtMillis ?? Infinity, requestedAtMillis + deadlines.forkReadyMillis);
		const { main: originalMainHead } = yield* readSourceHeads(source, true, deadlineAtMillis);
		const fingerprint = yield* resourceHash(yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Array(Schema.String)))([
			forkName,
			source.repositoryName,
			source.repositoryId,
			source.commitSha,
			artifactsRef
		]));
		const intent = {
			forkName,
			source: {
				...source,
				artifactsRef
			},
			originalMainHead,
			description: `sandbox-task-fork:${fingerprint}`,
			requestedAtMillis,
			deadlineAtMillis,
			repositoryId: null,
			initialized: false
		};
		yield* store.repositoryFork.put(intent);
		return intent;
	});
	const sameForkIntent = (left, right) => left.forkName === right.forkName && left.description === right.description && left.source.repositoryName === right.source.repositoryName && left.source.repositoryId === right.source.repositoryId && left.source.commitSha === right.source.commitSha && left.source.artifactsRef === right.source.artifactsRef && left.originalMainHead === right.originalMainHead && left.requestedAtMillis === right.requestedAtMillis && left.deadlineAtMillis === right.deadlineAtMillis;
	const plannedForkGeneration = Effect.fn("ArtifactsCheckout.plannedForkGeneration")(function* (intent) {
		const info = yield* artifacts.get(intent.forkName).pipe(boundedArtifacts("getPlannedFork"));
		if (info.name !== intent.forkName || info.remote !== remoteFor(intent.forkName) || info.description !== intent.description || info.defaultBranch !== "main" || info.source !== `artifacts:${config.artifactsNamespace}/${intent.source.repositoryName}` || info.readOnly || intent.repositoryId !== null && info.id !== intent.repositoryId) return yield* RepositoryForkIntentError.make({ reason: "target-mismatch" });
		return {
			repositoryName: intent.forkName,
			repositoryId: info.id
		};
	});
	/** Remote work only: this can run beside workspace preparation without touching its Git state. */
	const ensurePlannedFork = Effect.fn("ArtifactsCheckout.ensurePlannedFork")(function* (intent) {
		const store = yield* SessionStateStore;
		const retained = yield* store.repositoryFork.get;
		if (retained === null || !sameForkIntent(retained, intent)) return yield* RepositoryForkIntentError.make({ reason: "intent-mismatch" });
		if (retained.initialized) {
			if (retained.repositoryId === null) return yield* RepositoryForkIntentError.make({ reason: "target-mismatch" });
			return yield* plannedForkGeneration(retained);
		}
		const expired = () => ForkNotReadyError.make({
			forkName: retained.forkName,
			message: "The retained fork initialization deadline expired"
		});
		yield* readSourcePin(retained.source, retained.deadlineAtMillis);
		const existing = yield* plannedForkGeneration(retained).pipe(Effect.catchCause((cause) => cause.reasons.length > 0 && cause.reasons.every((reason) => Cause.isFailReason(reason) && reason.error._tag === "ArtifactsOperationError" && isForkProvisioning(reason.error)) ? Effect.succeed(null) : Effect.failCause(cause)), inMeasure("fork-lookup"));
		if (existing !== null && (yield* generationMainHead(existing)) === retained.source.commitSha) {
			yield* readSourcePin({
				...existing,
				commitSha: retained.source.commitSha,
				artifactsRef: retained.source.artifactsRef
			}, retained.deadlineAtMillis);
			const bound = {
				...retained,
				repositoryId: existing.repositoryId
			};
			yield* plannedForkGeneration(bound);
			yield* store.repositoryFork.put({
				...bound,
				initialized: true
			});
			return existing;
		}
		const remainingBeforeRequest = retained.deadlineAtMillis - (yield* Clock.currentTimeMillis);
		if (remainingBeforeRequest <= 0) return yield* expired();
		if (existing === null) {
			if (retained.repositoryId !== null) return yield* RepositoryForkIntentError.make({ reason: "target-mismatch" });
			yield* requestArtifactsFork({
				artifacts,
				seedRepository: retained.source.repositoryName,
				forkName: retained.forkName,
				policy: {
					...forkPolicy,
					operationTimeoutMillis: Math.min(forkPolicy.operationTimeoutMillis, remainingBeforeRequest)
				},
				description: retained.description,
				defaultBranchOnly: false
			}).pipe(inMeasure("fork-create-request"));
		}
		let pollAttempts = 0;
		yield* awaitArtifactsForkReady({
			artifacts,
			forkName: retained.forkName,
			policy: forkPolicy,
			readyRef: retained.source.artifactsRef.replace(/^refs\/heads\//u, ""),
			deadlineAtMillis: retained.deadlineAtMillis,
			onPollAttempt: () => {
				pollAttempts += 1;
			}
		}).pipe(Effect.onExit(() => annotateSpanAttempts(pollAttempts)), inMeasure("fork-poll"));
		const remaining = retained.deadlineAtMillis - (yield* Clock.currentTimeMillis);
		if (remaining <= 0) return yield* expired();
		return yield* Effect.gen(function* () {
			yield* readSourcePin(retained.source, retained.deadlineAtMillis);
			const generation = yield* plannedForkGeneration(retained);
			const bound = {
				...retained,
				repositoryId: generation.repositoryId
			};
			yield* store.repositoryFork.put(bound);
			yield* readSourcePin({
				...generation,
				commitSha: retained.source.commitSha,
				artifactsRef: retained.source.artifactsRef
			}, retained.deadlineAtMillis);
			const repository = yield* artifacts.get(generation.repositoryName).pipe(boundedArtifacts("getForkInitialization"));
			const head = yield* generationMainHead(generation);
			if (head !== retained.source.commitSha) {
				if (head !== retained.originalMainHead) return yield* RepositoryForkIntentError.make({ reason: "target-mismatch" });
				const fence = Effect.gen(function* () {
					const current = yield* store.repositoryFork.get;
					if (current === null || !sameForkIntent(current, retained) || current.initialized || current.repositoryId !== generation.repositoryId || (yield* Clock.currentTimeMillis) >= retained.deadlineAtMillis) return yield* RepositoryMutationFenceFailure.make({
						reason: "stopped",
						message: "The retained fork no longer authorizes initialization"
					});
					yield* plannedForkGeneration(bound).pipe(Effect.asVoid);
				}).pipe(Effect.catchCause((cause) => Effect.failCause(Cause.map(cause, (error) => error._tag === "RepositoryMutationFenceFailure" ? error : RepositoryMutationFenceFailure.make({
					reason: error._tag === "RepositoryForkIntentError" ? "stopped" : "unavailable",
					message: "The private fork initialization fence could not be verified",
					cause: error
				})))));
				yield* Effect.acquireUseRelease(repository.createToken("write", tokenTtlSeconds).pipe(boundedArtifacts("createToken:initialize")), (minted) => transferRepositoryGit({
					source: {
						url: remoteFor(retained.forkName),
						authorization: Redacted.make(`Bearer ${minted.plaintext}`)
					},
					target: {
						url: remoteFor(retained.forkName),
						authorization: Redacted.make(`Bearer ${minted.plaintext}`)
					},
					commitSha: retained.source.commitSha,
					branch: "main",
					requireEmptyTarget: false,
					...retained.originalMainHead === null ? {} : { expectedHead: retained.originalMainHead }
				}).pipe(Effect.provideService(RepositoryMutationFence, { check: fence }), Effect.provide(FetchHttpClient.layer)), (minted) => repository.revokeToken(minted.id).pipe(boundedArtifacts("revokeToken:initialize"), Effect.catchCause((cause) => Effect.failCause(failuresAsDefects(cause))))).pipe(inMeasure("fork-main-initialize"));
			}
			yield* verifyExpectedHead(retained.forkName, retained.source.commitSha, generation.repositoryId);
			yield* store.repositoryFork.put({
				...bound,
				initialized: true
			});
			return generation;
		}).pipe(Effect.timeoutOrElse({
			duration: remaining,
			orElse: expired
		}));
	});
	/** Read-through used after push to verify the durable branch head. */
	const mainHead = Effect.fn("ArtifactsCheckout.mainHead")(function* (forkName, expectedRepositoryId) {
		const head = expectedRepositoryId === void 0 ? yield* artifactsMainHead(artifacts, forkName, deadlines.artifactsOperationMillis) : yield* generationMainHead({
			repositoryName: forkName,
			repositoryId: expectedRepositoryId
		});
		if (head === null) return yield* ForkNotReadyError.make({
			forkName,
			message: "Fork main branch has no commits"
		});
		return head;
	});
	const verifyExpectedHead = Effect.fn("ArtifactsCheckout.verifyExpectedHead")(function* (forkName, expectedHead, expectedRepositoryId) {
		return yield* mainHead(forkName, expectedRepositoryId).pipe(Effect.flatMap((remoteHead) => remoteHead === expectedHead ? Effect.succeed(remoteHead) : PushVerificationError.make({
			forkName,
			localHead: expectedHead,
			remoteHead
		})), Effect.retry({
			times: 2,
			while: (error) => error._tag === "PushVerificationError" || error._tag === "ForkNotReadyError" || error._tag === "ArtifactsOperationTimeoutError" || error._tag === "ArtifactsOperationError",
			schedule: Schedule.exponential("200 millis").pipe(Schedule.jittered)
		}));
	});
	const verifyExpectedBoundGenerationHead = Effect.fn("ArtifactsCheckout.verifyExpectedBoundGenerationHead")(function* (repository, generation, expectedHead) {
		return yield* verifyRepositoryGeneration(repository, generation).pipe(Effect.andThen(boundGenerationMainHead(repository, generation)), Effect.flatMap((remoteHead) => Effect.gen(function* () {
			if (remoteHead === null) return yield* ForkNotReadyError.make({
				forkName: generation.repositoryName,
				message: "Fork main branch has no commits"
			});
			if (remoteHead !== expectedHead) return yield* PushVerificationError.make({
				forkName: generation.repositoryName,
				localHead: expectedHead,
				remoteHead
			});
			return remoteHead;
		})), Effect.retry({
			times: 2,
			while: (error) => error._tag === "PushVerificationError" || error._tag === "ForkNotReadyError" || error._tag === "ArtifactsOperationTimeoutError" || error._tag === "ArtifactsOperationError",
			schedule: Schedule.exponential("200 millis").pipe(Schedule.jittered)
		}));
	});
	const ensureFork = Effect.fn("ArtifactsCheckout.ensureFork")(function* (forkName, seed = config.seedRepository, policy = forkPolicy) {
		const seedRepository = yield* requireRepositorySeed(seed);
		const existingHead = yield* artifactsMainHead(artifacts, forkName, policy.operationTimeoutMillis).pipe(Effect.catchTags({
			ArtifactsOperationError: (error) => error.code === "NOT_FOUND" || error.code === "FORK_IN_PROGRESS" ? Effect.succeed(null) : Effect.fail(error),
			ArtifactsOperationTimeoutError: () => Effect.succeed(null)
		}), inMeasure("fork-lookup"));
		if (existingHead !== null) return {
			head: existingHead,
			disposition: "existing",
			pollAttempts: 0
		};
		const requested = yield* requestArtifactsFork({
			artifacts,
			seedRepository,
			forkName,
			policy
		}).pipe(inMeasure("fork-create-request"));
		let pollAttempts = 0;
		return {
			head: yield* awaitArtifactsForkReady({
				artifacts,
				forkName,
				policy,
				onPollAttempt: () => {
					pollAttempts += 1;
				}
			}).pipe(Effect.onExit(() => annotateSpanAttempts(pollAttempts)), inMeasure("fork-poll")),
			disposition: requested.disposition,
			pollAttempts
		};
	});
	const discardCloneReceipt = (scope) => Effect.flatMap(SessionStateStore, (store) => store.workspacePreparation(`clone:${scope}`).delete).pipe(Effect.asVoid);
	const cloneRepositoryIntoSandbox = Effect.fn("ArtifactsCheckout.cloneRepositoryIntoSandbox")(function* (repositoryName, expectedRepositoryId, scope = "workspace", ref = "main") {
		const remote = remoteFor(repositoryName);
		const generation = expectedRepositoryId === void 0 ? void 0 : {
			repositoryName,
			repositoryId: expectedRepositoryId
		};
		const repository = yield* artifacts.get(repositoryName).pipe(boundedArtifacts("getCloneRepository"));
		const sandbox = yield* SessionSandbox;
		if (generation !== void 0) yield* verifyRepositoryGeneration(repository, generation);
		const cloned = yield* Effect.gen(function* () {
			const retained = yield* (yield* SessionStateStore).workspacePreparation(`clone:${scope}`).get;
			if (retained !== null && (retained.repositoryToken?.repositoryName !== repositoryName || retained.repositoryToken.repositoryId !== expectedRepositoryId)) return yield* HarnessTurnError.make({
				reason: "turnFailed",
				message: "A different repository preparation still owns this workspace"
			});
			let repositoryToken = retained?.repositoryToken;
			let request;
			if (retained === null) {
				if (generation !== void 0) yield* verifyRepositoryGeneration(repository, generation);
				const minted = yield* repository.createToken("read", tokenTtlSeconds).pipe(boundedArtifacts("createToken:read"));
				if (generation !== void 0) yield* verifyRepositoryGeneration(repository, generation);
				const deadlineAt = (yield* Clock.currentTimeMillis) + deadlines.gitCommandMillis;
				const authorization = yield* sandbox.setRepositoryAuthorization(remote, `Bearer ${minted.plaintext}`, deadlineAt, minted.id);
				repositoryToken = {
					repositoryName,
					...expectedRepositoryId === void 0 ? {} : { repositoryId: expectedRepositoryId },
					tokenId: minted.id,
					authorization
				};
				request = {
					deadlineAt,
					command: [
						"sh",
						"-c",
						ref === "main" ? "set -eu; rm -rf -- \"$1\" \"$2\"; mkdir -p -- \"$2\"; cd \"$2\"; git clone --depth 1 --single-branch --branch main --no-tags \"$3\" \"$1\"; git -C \"$1\" config --local user.name \"$4\"; git -C \"$1\" config --local user.email \"$5\"; git -C \"$1\" rev-parse HEAD" : "set -eu; rm -rf -- \"$1\" \"$2\"; mkdir -p -- \"$2\"; cd \"$2\"; git clone --depth 1 --single-branch --branch \"$6\" --no-tags \"$3\" \"$1\"; git -C \"$1\" config --local user.name \"$4\"; git -C \"$1\" config --local user.email \"$5\"; git -C \"$1\" rev-parse HEAD",
						"workspace-clone",
						repositoryDirectory,
						GIT_TRANSPORT_DIR,
						remote,
						AGENT_COMMIT_AUTHOR,
						AGENT_COMMIT_EMAIL,
						...ref === "main" ? [] : [ref]
					],
					cwd: WORKSPACE_DIR,
					env: gitTransportEnv(remote),
					maxOutputBytes: GIT_OUTPUT_LIMIT_BYTES,
					repositoryToken
				};
			}
			const output = yield* runWorkspacePreparation(`clone:${scope}`, request);
			if (repositoryToken !== void 0) {
				yield* clearRepositoryAuthorization(repositoryToken.authorization);
				const { repositoryName, tokenId } = repositoryToken;
				yield* Effect.gen(function* () {
					yield* (yield* artifacts.get(repositoryName)).revokeToken(tokenId);
				}).pipe(boundedArtifacts("revokeToken:read"), Effect.tapCause((cause) => reportCleanupFailure(cause, "ArtifactsCheckout.revokeToken")), Effect.ignore);
			}
			yield* acknowledgeWorkspacePreparation(`clone:${scope}`);
			return output;
		}).pipe(Effect.flatMap((output) => {
			if (output.exitCode !== 0 || output.timedOut || output.truncated) return gitCommandError("clone", output);
			const head = output.stdout.trim();
			return /^[0-9a-f]{40}$/u.test(head) ? Effect.succeed({ head }) : GitCommandError.make({
				command: "clone",
				exitCode: 0,
				stderr: "Clone returned an invalid HEAD"
			});
		})).pipe(Effect.retry({
			times: GIT_READ_RETRY_COUNT,
			while: (error) => error._tag === "GitCommandError",
			schedule: Schedule.exponential("250 millis").pipe(Schedule.jittered)
		}));
		if (generation !== void 0) yield* verifyRepositoryGeneration(repository, generation);
		return cloned;
	});
	const cloneIntoSandbox = Effect.fn("ArtifactsCheckout.cloneIntoSandbox")(function* (forkName, scope = "workspace", bootstrap) {
		if (bootstrap === void 0) return yield* cloneRepositoryIntoSandbox(forkName, void 0, scope);
		const retained = yield* (yield* SessionStateStore).workspacePreparation(`clone:${scope}`).get;
		const seedCloneName = config.seedCloneRepositoryName(bootstrap.source.repositoryName);
		let cloneSource = bootstrap.source;
		if (retained !== null) {
			const token = retained.repositoryToken;
			if (token?.repositoryName === forkName && token.repositoryId === void 0) return yield* cloneRepositoryIntoSandbox(forkName, void 0, scope);
			if (token?.repositoryId === void 0 || token.repositoryName !== bootstrap.source.repositoryName && token.repositoryName !== seedCloneName || token.repositoryName === bootstrap.source.repositoryName && token.repositoryId !== bootstrap.source.repositoryId) return yield* HarnessTurnError.make({
				reason: "turnFailed",
				message: "A different repository preparation still owns this workspace"
			});
			cloneSource = {
				repositoryName: token.repositoryName,
				repositoryId: token.repositoryId
			};
		} else {
			if ((yield* generationMainHead(bootstrap.source)) !== bootstrap.forkHead) return yield* cloneRepositoryIntoSandbox(forkName, void 0, scope);
			if (seedCloneName !== bootstrap.source.repositoryName) cloneSource = yield* sourceGeneration(seedCloneName);
		}
		const cloned = yield* cloneRepositoryIntoSandbox(cloneSource.repositoryName, cloneSource.repositoryId, scope);
		if (cloned.head !== bootstrap.forkHead || (yield* generationMainHead(bootstrap.source)) !== bootstrap.forkHead) return yield* GitCommandError.make({
			command: "clone",
			exitCode: 0,
			stderr: "Bootstrap clone does not match the admitted fork head"
		});
		yield* retargetOrigin(forkName);
		return cloned;
	});
	const cloneGenerationIntoSandbox = Effect.fn("ArtifactsCheckout.cloneGenerationIntoSandbox")(function* (generation) {
		return yield* cloneRepositoryIntoSandbox(generation.repositoryName, generation.repositoryId);
	});
	const cloneSourceIntoSandbox = Effect.fn("ArtifactsCheckout.cloneSourceIntoSandbox")(function* (source, forkName, retryDeadlineAtMillis) {
		yield* readSourcePin(source, retryDeadlineAtMillis);
		const cloned = yield* cloneRepositoryIntoSandbox(source.repositoryName, source.repositoryId, "workspace", (source.artifactsRef ?? "main").replace(/^refs\/heads\//u, ""));
		if (cloned.head !== source.commitSha) return yield* RepositoryForkIntentError.make({ reason: "source-pin" });
		yield* readSourcePin(source, retryDeadlineAtMillis);
		yield* retargetOrigin(forkName);
		return cloned;
	});
	/** Point a seed clone at its writable session fork without persisting a
	* credential. The caller first proves both repositories have the same head. */
	const retargetOrigin = Effect.fn("ArtifactsCheckout.retargetOrigin")(function* (forkName) {
		yield* runGit([
			"remote",
			"set-url",
			"origin",
			remoteFor(forkName)
		]).pipe(inMeasure("git-remote-retarget"));
	});
	const checkoutCommit = Effect.fn("ArtifactsCheckout.checkoutCommit")(function* (commit) {
		yield* runGit([
			"checkout",
			"--detach",
			commit
		]);
		if ((yield* runGit(["rev-parse", "HEAD"], { requireCompleteOutput: true })).trim() !== commit) return yield* GitCommandError.make({
			command: "checkout",
			exitCode: 0,
			stderr: "the checkout does not match the pinned commit"
		});
	});
	const restoreSnapshotCheckout = Effect.fn("ArtifactsCheckout.restoreSnapshotCheckout")(function* (repositoryName, head, checkpointHead) {
		if ((yield* runGit(["rev-parse", "HEAD"], {
			requireCompleteOutput: true,
			inspect: true
		})).trim() !== checkpointHead) return yield* GitCommandError.make({
			command: "snapshot-head",
			exitCode: 0,
			stderr: "Snapshot source does not match its receipt"
		});
		yield* retargetOrigin(repositoryName);
		yield* Effect.annotateCurrentSpan("repository.git.checkout", head === checkpointHead ? "unchanged" : "updated");
		if (head !== checkpointHead) {
			yield* runGit([
				"reset",
				"--hard",
				head
			]);
			yield* checkoutCommit(head);
		}
	});
	const restorePreparedRepository = Effect.fn("ArtifactsCheckout.restorePreparedRepository")(function* (candidate, transportDirectory) {
		const output = yield* (yield* (yield* SessionSandbox).exec([
			"rm",
			"-rf",
			"--",
			repositoryDirectory
		], {
			cwd: WORKSPACE_DIR,
			timeout: deadlines.gitCommandMillis
		})).outputText({
			timeout: deadlines.gitCommandMillis,
			maxBytes: GIT_OUTPUT_LIMIT_BYTES
		});
		if (output.exitCode !== 0 || output.timedOut) return yield* gitCommandError("restore-repository-reset", output);
		const env = {
			...isolatedGitTransportEnv,
			GIT_ALLOW_PROTOCOL: "file"
		};
		yield* runGit(["init", repositoryDirectory], {
			cwd: WORKSPACE_DIR,
			env,
			requireCompleteOutput: true
		});
		for (const [key, value] of [
			["core.hooksPath", "/dev/null"],
			["user.name", AGENT_COMMIT_AUTHOR],
			["user.email", AGENT_COMMIT_EMAIL]
		]) yield* runGit([
			"config",
			"--local",
			key,
			value
		], {
			env,
			requireCompleteOutput: true
		});
		yield* runGit([
			"fetch",
			"--no-tags",
			"--update-shallow",
			transportDirectory,
			candidate.head
		], {
			env,
			requireCompleteOutput: true
		});
		yield* runGit([
			"checkout",
			"--detach",
			candidate.head
		], {
			env,
			requireCompleteOutput: true
		});
		if ((yield* runGit(["rev-parse", "HEAD"], {
			env,
			requireCompleteOutput: true
		})).trim() !== candidate.head) return yield* GitCommandError.make({
			command: "checkout",
			exitCode: 0,
			stderr: "the restored checkout does not match the prepared commit"
		});
	});
	const describePreparedCommit = Effect.fn("ArtifactsCheckout.describePreparedCommit")(function* (forkName, verifiedHead, head) {
		if ((yield* runGit(["rev-parse", `${head}^{commit}`], { requireCompleteOutput: true })).trim() !== head) return yield* GitCommandError.make({
			command: "rev-parse",
			exitCode: 0,
			stderr: "the prepared commit no longer resolves to its exact object id"
		});
		yield* runGit([
			"merge-base",
			"--is-ancestor",
			verifiedHead,
			head
		], { requireCompleteOutput: true }).pipe(Effect.catchTag("GitCommandError", (error) => error.exitCode === 1 ? Effect.fail(NonFastForwardPushError.make({
			forkName,
			acceptedHead: verifiedHead,
			localHead: head
		})) : Effect.fail(error)));
		const changedPaths = (yield* runGit([
			"diff",
			"--name-only",
			verifiedHead,
			head
		], { requireCompleteOutput: true })).split("\n").map((line) => line.trim()).filter((line) => line.length > 0);
		const encodedBytes = new TextEncoder().encode(toJson(changedPaths)).byteLength;
		if (encodedBytes > CHANGED_PATHS_LIMIT_BYTES) return yield* PersistedPathsLimitError.make({
			pathCount: changedPaths.length,
			observedBytes: encodedBytes,
			maxBytes: CHANGED_PATHS_LIMIT_BYTES
		});
		return {
			head,
			changedPaths
		};
	});
	const prepareGitTransport = Effect.fn("ArtifactsCheckout.prepareGitTransport")(function* (local) {
		yield* resetGitTransportDirectory();
		yield* runGit([
			"init",
			"--bare",
			GIT_TRANSPORT_REPOSITORY
		], {
			cwd: GIT_TRANSPORT_DIR,
			env: isolatedGitTransportEnv,
			requireCompleteOutput: true
		});
		yield* runGit([
			"fetch",
			"--no-tags",
			"--force",
			"--update-shallow",
			repositoryDirectory,
			local.head
		], {
			cwd: GIT_TRANSPORT_DIR,
			env: {
				...isolatedGitTransportEnv,
				GIT_DIR: GIT_TRANSPORT_REPOSITORY
			},
			requireCompleteOutput: true
		});
	});
	const pushPreparedRepository = Effect.fn("ArtifactsCheckout.pushPreparedRepository")(function* (forkName, local, generation, beforeGenerationPush) {
		const persisted = yield* Effect.scoped(Effect.gen(function* () {
			const repo = yield* artifacts.get(forkName).pipe(boundedArtifacts("getWriteTokenRepository"));
			if (generation !== void 0) {
				yield* verifyRepositoryGeneration(repo, generation);
				if (beforeGenerationPush !== void 0) yield* beforeGenerationPush(local);
			}
			const credential = yield* Effect.acquireRelease(Effect.gen(function* () {
				const minted = yield* repo.createToken("write", tokenTtlSeconds).pipe(boundedArtifacts("createToken:write"), inMeasure("git-token-mint-write"));
				return {
					repo,
					minted
				};
			}), ({ repo, minted }) => repo.revokeToken(minted.id).pipe(boundedArtifacts("revokeToken:write"), Effect.tapCause((cause) => reportCleanupFailure(cause, "ArtifactsCheckout.revokeToken")), Effect.ignore, inMeasure("git-token-revoke-write")));
			const remote = remoteFor(forkName);
			const sandbox = yield* SessionSandbox;
			if (generation !== void 0) yield* verifyRepositoryGeneration(credential.repo, generation);
			const deadlineAt = (yield* Clock.currentTimeMillis) + deadlines.gitCommandMillis;
			yield* Effect.acquireRelease(sandbox.setRepositoryAuthorization(remote, `Bearer ${credential.minted.plaintext}`, deadlineAt, credential.minted.id), clearRepositoryAuthorization);
			const verifyPushedHead = Effect.gen(function* () {
				if (generation === void 0) return yield* verifyExpectedHead(forkName, local.head);
				return yield* verifyExpectedBoundGenerationHead(credential.repo, generation, local.head);
			});
			yield* runGit([
				"push",
				"--no-thin",
				remote,
				"FETCH_HEAD:refs/heads/main"
			], {
				cwd: GIT_TRANSPORT_DIR,
				deadlineAt,
				env: {
					...gitTransportEnv(remote),
					GIT_DIR: GIT_TRANSPORT_REPOSITORY
				}
			}).pipe(inPhase(agentPhases.gitPush), Effect.catch((error) => verifyPushedHead.pipe(Effect.asVoid, Effect.catchTag("PushVerificationError", () => Effect.fail(error)))));
			yield* verifyPushedHead;
			return local;
		}));
		yield* verifyExpectedHead(forkName, persisted.head, generation?.repositoryId).pipe(inPhase(agentPhases.artifactsVerify));
		return persisted;
	});
	const prepareRepository = Effect.fn("ArtifactsCheckout.prepareRepository")(function* (forkName, verifiedHead, generation) {
		const statusOutput = yield* runGit([
			"status",
			"--porcelain=v2",
			"--branch",
			"--untracked-files=normal"
		], { requireCompleteOutput: true }).pipe(inPhase(agentPhases.gitStatus));
		const status = statusAndHead(statusOutput);
		if (status === null) return yield* GitCommandError.make({
			command: "status",
			exitCode: 0,
			stderr: "git status did not report branch.oid"
		});
		if (!status.dirty && status.head === verifiedHead) {
			if (generation !== void 0) yield* verifyExpectedHead(forkName, verifiedHead, generation.repositoryId);
			yield* markPoint("git-persist-skipped");
			return {
				changed: false,
				local: {
					head: status.head,
					changedPaths: []
				}
			};
		}
		if (status.dirty) yield* runGit(["add", "-A"], { requireCompleteOutput: true }).pipe(Effect.flatMap(() => runGit([
			"-c",
			`user.name=${AGENT_COMMIT_AUTHOR}`,
			"-c",
			`user.email=${AGENT_COMMIT_EMAIL}`,
			"commit",
			"-m",
			["sandbox-agent: persist harness turn", config.commitCoauthorTrailer].filter(Boolean).join("\n\n")
		], { requireCompleteOutput: true })), inPhase(agentPhases.gitCommit));
		const head = status.dirty ? (yield* runGit(["rev-parse", "HEAD"], { requireCompleteOutput: true })).trim() : status.head;
		return {
			changed: true,
			local: yield* describePreparedCommit(forkName, verifiedHead, head)
		};
	});
	const persist = Effect.fn("ArtifactsCheckout.persist")(function* (forkName, verifiedHead, sourceKey, repositoryId) {
		const generation = repositoryId === void 0 ? void 0 : {
			repositoryName: forkName,
			repositoryId
		};
		const prepared = yield* prepareRepository(forkName, verifiedHead, generation);
		if (!prepared.changed) {
			yield* (yield* RepositoryPersistenceCheckpoint).prepared(prepared.local, null);
			return prepared.local;
		}
		yield* assertRepositorySetupSecretsNotPersisted(sourceKey, verifiedHead, prepared.local.head).pipe(Effect.provideService(SandboxAgentConfig, config));
		yield* prepareGitTransport(prepared.local);
		yield* (yield* RepositoryPersistenceCheckpoint).prepared(prepared.local, GIT_TRANSPORT_REPOSITORY);
		return yield* pushPreparedRepository(forkName, prepared.local, generation);
	});
	const persistGeneration = Effect.fn("ArtifactsCheckout.persistGeneration")(function* (generation, verifiedHead, beforePush) {
		const prepared = yield* prepareRepository(generation.repositoryName, verifiedHead, generation);
		if (!prepared.changed) return prepared.local;
		yield* prepareGitTransport(prepared.local);
		return yield* pushPreparedRepository(generation.repositoryName, prepared.local, generation, beforePush);
	});
	const pushPreparedGeneration = Effect.fn("ArtifactsCheckout.pushPreparedGeneration")(function* (generation, verifiedHead, preparedHead) {
		yield* runGit([
			"cat-file",
			"-e",
			`${preparedHead}^{commit}`
		], { requireCompleteOutput: true }).pipe(Effect.catchTag("GitCommandError", (error) => error.exitCode === 128 ? Effect.fail(PreparedCommitUnavailableError.make({
			repositoryName: generation.repositoryName,
			preparedHead
		})) : Effect.fail(error)));
		const local = yield* describePreparedCommit(generation.repositoryName, verifiedHead, preparedHead);
		yield* prepareGitTransport(local);
		return yield* pushPreparedRepository(generation.repositoryName, local, generation);
	});
	const ensureCommit = Effect.fn("ArtifactsCheckout.ensureCommit")(function* (forkName, commit, options) {
		if (options?.repositoryId !== void 0) yield* readPinnedRepository({
			repositoryName: forkName,
			repositoryId: options.repositoryId
		}, {
			_tag: "Commit",
			commit
		}, options.retryDeadlineAtMillis);
		const { present, shallow } = yield* runGit([
			"rev-parse",
			"--is-shallow-repository",
			"--verify",
			`${commit}^{commit}`
		], {
			requireCompleteOutput: true,
			inspect: true
		}).pipe(Effect.map((output) => ({
			present: true,
			shallow: output.split("\n")[0] === "true"
		})), Effect.catchTag("GitCommandError", (error) => error.exitCode === 128 ? runGit(["rev-parse", "--is-shallow-repository"], {
			requireCompleteOutput: true,
			inspect: true
		}).pipe(Effect.map((output) => ({
			present: false,
			shallow: output.trim() === "true"
		}))) : Effect.fail(error)));
		const includeHistory = !shallow || options?.includeHistory === true;
		const cleanup = (yield* SessionSandbox).remove(GIT_TRANSPORT_DIR, {
			recursive: true,
			force: true
		});
		if (present && (!includeHistory || !shallow)) return yield* cleanup;
		const head = (yield* runGit(["rev-parse", "HEAD"], { requireCompleteOutput: true })).trim();
		const unshallow = includeHistory && shallow;
		const revisions = unshallow ? [head, commit] : [commit];
		yield* resetGitTransportDirectory();
		yield* runGit([
			"init",
			"--bare",
			GIT_TRANSPORT_REPOSITORY
		], {
			cwd: GIT_TRANSPORT_DIR,
			env: isolatedGitTransportEnv,
			requireCompleteOutput: true
		});
		if (!unshallow) {
			const sandbox = yield* SessionSandbox;
			yield* sandbox.writeFile(`${GIT_TRANSPORT_REPOSITORY}/objects/info/alternates`, `${repositoryDirectory}/.git/objects\n`);
			if (shallow) yield* sandbox.writeFile(`${GIT_TRANSPORT_REPOSITORY}/shallow`, yield* sandbox.readFileString(`${repositoryDirectory}/.git/shallow`));
		}
		yield* Effect.annotateCurrentSpan({
			"repository.git.fetch": unshallow ? "full-history" : "incremental",
			"repository.git.shallow": shallow
		});
		const remote = remoteFor(forkName);
		yield* withRepoToken(forkName, "read", (deadlineAt) => runGit([
			"fetch",
			...includeHistory ? [] : ["--depth", "1"],
			...!unshallow ? ["--negotiation-tip", head] : [],
			"--no-tags",
			remote,
			...revisions
		], {
			cwd: GIT_TRANSPORT_DIR,
			env: {
				...gitTransportEnv(remote),
				GIT_DIR: GIT_TRANSPORT_REPOSITORY
			},
			requireCompleteOutput: true,
			deadlineAt
		}), options?.repositoryId).pipe(Effect.retry({
			times: GIT_READ_RETRY_COUNT,
			while: (error) => error._tag === "GitCommandError" && error.status !== void 0 && [
				408,
				429,
				500,
				502,
				503,
				504
			].includes(error.status),
			schedule: Schedule.exponential("250 millis").pipe(Schedule.jittered, Schedule.tap(({ input }) => Effect.logWarning("Artifacts history read attempt failed", sanitizeCause(Cause.fail(input), { operation: "ArtifactsCheckout.ensureCommit" }))))
		}));
		yield* runGit([
			"fetch",
			"--no-tags",
			...unshallow ? ["--unshallow"] : ["--update-shallow"],
			GIT_TRANSPORT_REPOSITORY,
			...revisions
		], {
			env: {
				...isolatedGitTransportEnv,
				GIT_ALLOW_PROTOCOL: "file"
			},
			requireCompleteOutput: true
		});
		yield* runGit([
			"cat-file",
			"-e",
			`${commit}^{commit}`
		], { requireCompleteOutput: true });
		yield* cleanup;
	});
	const deleteFork = Effect.fn("ArtifactsCheckout.deleteFork")(function* (forkName) {
		return yield* artifacts.delete(forkName).pipe(boundedArtifacts("deleteFork"), Effect.catchTag("ArtifactsOperationError", (error) => error.code === "NOT_FOUND" ? Effect.succeed(false) : Effect.fail(error)));
	});
	return {
		ensureFork,
		retainFork,
		ensurePlannedFork,
		readSourcePin,
		cloneSourceIntoSandbox,
		deletePlannedFork: Effect.fn("ArtifactsCheckout.deletePlannedFork")(function* (intent) {
			return yield* Effect.gen(function* () {
				yield* plannedForkGeneration(intent);
				return yield* artifacts.delete(intent.forkName).pipe(boundedArtifacts("deletePlannedFork"));
			}).pipe(Effect.catchCause((cause) => cause.reasons.length > 0 && cause.reasons.every((reason) => Cause.isFailReason(reason) && reason.error._tag === "ArtifactsOperationError" && reason.error.code === "NOT_FOUND") ? Effect.succeed(false) : Effect.failCause(cause)));
		}),
		sourceGeneration,
		cloneIntoSandbox,
		discardCloneReceipt,
		cloneGenerationIntoSandbox,
		readGenerationMainHead: (generation) => mainHead(generation.repositoryName, generation.repositoryId),
		retargetOrigin,
		checkoutCommit,
		restoreSnapshotCheckout,
		restorePreparedRepository,
		persist,
		persistGeneration,
		pushPreparedGeneration,
		ensureCommit,
		deleteFork,
		remoteFor
	};
});
/**
* Internal service that owns the seed/fork Git lifecycle. Its methods mint
* short-lived repository tokens only around the Git process that consumes
* them and verify every claimed durable write through the Artifacts binding.
*/
var ArtifactsCheckout = class ArtifactsCheckout extends Context.Service()("sandbox-agent/ArtifactsCheckout") {
	static layer = Layer.effect(ArtifactsCheckout)(Effect.map(make, (service) => ArtifactsCheckout.of(service)));
};
//#endregion
export { ArtifactsCheckout, ArtifactsCheckoutDirectory, ArtifactsRepositoryGenerationMismatchError, ForkNotReadyError, GitCommandError, NonFastForwardPushError, PersistedPathsLimitError, PreparedCommitUnavailableError, PushVerificationError, RepositoryForkIntentError, isForkProvisioning };
