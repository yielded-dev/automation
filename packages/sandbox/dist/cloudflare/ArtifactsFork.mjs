import { withFailureDiagnostic } from "../observability/FailureDiagnostic.mjs";
import { reportSanitizedCause } from "../observability/RuntimeDiagnostics.mjs";
import { Cause, Clock, Effect, Predicate, Random, Schema } from "effect";
//#region src/cloudflare/ArtifactsFork.ts
/** One provider operation exceeded its bound and was interrupted. */
var ArtifactsOperationTimeoutError = class extends Schema.TaggedError()("ArtifactsOperationTimeoutError", {
	operation: Schema.String,
	timeoutMillis: Schema.Int
}) {};
/** The writable Artifacts fork did not become readable before its setup deadline. */
var ForkNotReadyError = class extends Schema.TaggedError()("ForkNotReadyError", {
	forkName: Schema.String,
	message: Schema.String,
	cause: Schema.optionalKey(Schema.Defect())
}) {};
const missingRepositoryMessage = /^ArtifactsError: Repository not found: [A-Za-z0-9][A-Za-z0-9._-]{0,62}\.?$/u;
/** True when a provider error describes a fork that is absent or not readable yet. */
const isForkProvisioning = (error) => {
	if (error.code === "NOT_FOUND" || error.numericCode === 10200 || error.code === "FORK_IN_PROGRESS" || error.numericCode === 10303) return true;
	return error.operation === "get" && Predicate.isObject(error.cause) && Predicate.hasProperty(error.cause, "message") && typeof error.cause.message === "string" && missingRepositoryMessage.test(error.cause.message);
};
const artifactsForkPolicyFromDeadlines = (deadlines) => ({
	operationTimeoutMillis: deadlines.artifactsOperationMillis,
	readyDeadlineMillis: deadlines.forkReadyMillis
});
const providerTimeout = (operation, timeoutMillis) => (effect) => effect.pipe(Effect.timeoutOrElse({
	duration: timeoutMillis,
	orElse: () => ArtifactsOperationTimeoutError.make({
		operation,
		timeoutMillis
	})
}), (bounded) => withFailureDiagnostic(bounded, { operation: `ArtifactsFork.${operation}` }));
/** Reads a branch (main by default); null means that ref has no readable commit yet. */
const artifactsMainHead = Effect.fn("ArtifactsFork.artifactsMainHead")(function* (artifacts, forkName, operationTimeoutMillis, ref = "main") {
	return yield* Effect.gen(function* () {
		return (yield* (yield* artifacts.get(forkName)).log({
			ref,
			limit: 1
		}))[0]?.hash ?? null;
	}).pipe(providerTimeout("mainHead", operationTimeoutMillis));
});
/**
* Requests a fork without waiting for materialization. A timed-out mutation is
* indeterminate and therefore adopted: the kernel's readiness loop reconciles
* the target instead of issuing an unsafe alternate mutation.
*/
const requestArtifactsFork = Effect.fn("ArtifactsFork.requestArtifactsFork")(function* (input) {
	return { disposition: yield* Effect.gen(function* () {
		yield* (yield* input.artifacts.get(input.seedRepository)).fork(input.forkName, {
			defaultBranchOnly: input.defaultBranchOnly ?? true,
			readOnly: false,
			...input.description === void 0 ? {} : { description: input.description }
		});
		return "created";
	}).pipe(providerTimeout("fork", input.policy.operationTimeoutMillis), Effect.catchCause((cause) => {
		if (!cause.reasons.every((reason) => Cause.isFailReason(reason) && (reason.error._tag === "ArtifactsOperationTimeoutError" || reason.error.code === "ALREADY_EXISTS" || reason.error.code === "FORK_IN_PROGRESS")) || cause.reasons.length === 0) return Effect.failCause(cause);
		return Effect.succeed(cause.reasons.every((reason) => Cause.isFailReason(reason) && reason.error._tag === "ArtifactsOperationError" && reason.error.code === "ALREADY_EXISTS") ? "existing" : "indeterminate");
	})) };
});
/** Waits for the fork's selected ref with bounded provider probes and jitter. */
const awaitArtifactsForkReady = Effect.fn("ArtifactsFork.awaitArtifactsForkReady")(function* (input) {
	const eagerPollMillis = input.policy.eagerPollMillis ?? 300;
	const eagerPollAttempts = input.policy.eagerPollAttempts ?? 20;
	const steadyPollMillis = input.policy.steadyPollMillis ?? 1e3;
	const deadlineAt = input.deadlineAtMillis ?? (yield* Clock.currentTimeMillis) + input.policy.readyDeadlineMillis;
	let lastFailure;
	for (let attempt = 0;; attempt += 1) {
		const beforeProbe = yield* Clock.currentTimeMillis;
		if (beforeProbe >= deadlineAt) break;
		input.onPollAttempt?.();
		const head = yield* artifactsMainHead(input.artifacts, input.forkName, Math.max(1, Math.min(input.policy.operationTimeoutMillis, deadlineAt - beforeProbe)), input.readyRef).pipe(Effect.catchCause((cause) => {
			if (cause.reasons.length === 0 || !cause.reasons.every((reason) => Cause.isFailReason(reason) && (reason.error._tag === "ArtifactsOperationTimeoutError" || isForkProvisioning(reason.error) || reason.error.code === "INTERNAL_ERROR"))) return Effect.failCause(cause);
			lastFailure = cause;
			if (cause.reasons.some((reason) => Cause.isFailReason(reason) && reason.error._tag === "ArtifactsOperationError" && reason.error.code === "INTERNAL_ERROR")) return reportSanitizedCause(cause, { operation: "ArtifactsFork.awaitArtifactsForkReady" }).pipe(Effect.as(null));
			return Effect.succeed(null);
		}));
		if (head !== null) return head;
		const now = yield* Clock.currentTimeMillis;
		if (now >= deadlineAt) break;
		const baseDelay = attempt < eagerPollAttempts ? eagerPollMillis : steadyPollMillis;
		const random = yield* Random.next;
		const jitteredDelay = Math.round(baseDelay * (.8 + random * .4));
		yield* Effect.sleep(Math.min(jitteredDelay, deadlineAt - now));
	}
	return yield* ForkNotReadyError.make({
		forkName: input.forkName,
		message: `Fork was not ready after ${input.policy.readyDeadlineMillis / 1e3} seconds`,
		...lastFailure === void 0 ? {} : { cause: lastFailure }
	});
});
/** Finds or provisions a fork and returns only after its `main` head is readable. */
const ensureArtifactsFork = Effect.fn("ArtifactsFork.ensureArtifactsFork")(function* (input) {
	const existingHead = yield* artifactsMainHead(input.artifacts, input.forkName, input.policy.operationTimeoutMillis).pipe(Effect.catchTags({
		ArtifactsOperationError: (error) => isForkProvisioning(error) ? Effect.succeed(null) : Effect.fail(error),
		ArtifactsOperationTimeoutError: () => Effect.succeed(null)
	}));
	if (existingHead !== null) return {
		head: existingHead,
		disposition: "existing",
		pollAttempts: 0
	};
	const requested = yield* requestArtifactsFork(input);
	let pollAttempts = 0;
	return {
		head: yield* awaitArtifactsForkReady({
			artifacts: input.artifacts,
			forkName: input.forkName,
			policy: input.policy,
			onPollAttempt: () => {
				pollAttempts += 1;
				input.onPollAttempt?.();
			}
		}),
		disposition: requested.disposition,
		pollAttempts
	};
});
//#endregion
export { ArtifactsOperationTimeoutError, ForkNotReadyError, artifactsForkPolicyFromDeadlines, artifactsMainHead, awaitArtifactsForkReady, ensureArtifactsFork, isForkProvisioning, requestArtifactsFork };
