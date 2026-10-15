import { RepairSourceLimit, RepairSourceReadError } from "../domain/Investigation.mjs";
import "../domain/TriageTrace.mjs";
import { Cause, Context, Crypto, Effect, Exit, Match, Option, Result, Schema } from "effect";
import { AiError } from "effect/ai";
//#region src/server/TriageJournal.ts
var TriageJournal = class extends Context.Service()("@reve/repairs/TriageJournal") {};
const ContextError = Schema.fromJsonString(Schema.Struct({ detail: Schema.Struct({ error_type: Schema.Literal("max_tokens_exceeded") }) }));
const triageAiFailure = (error) => {
	const reason = error.reason;
	const http = "http" in reason ? reason.http : void 0;
	const body = http?.body;
	return {
		failure: Match.value(reason).pipe(Match.tag("RateLimitError", () => "rate_limit"), Match.tag("AuthenticationError", () => "authentication"), Match.tag("InvalidRequestError", () => typeof body === "string" && Result.isSuccess(Schema.decodeResult(ContextError)(body)) ? "context_limit" : "provider_failure"), Match.tag("InternalProviderError", "QuotaExhaustedError", "ContentPolicyError", () => "provider_failure"), Match.tag("NetworkError", (reason) => reason.reason === "TransportError" ? "provider_failure" : "internal_failure"), Match.tag("InvalidOutputError", "StructuredOutputError", "ToolNotFoundError", "ToolParameterValidationError", () => "invalid_output"), Match.tag("UnsupportedSchemaError", "UnknownError", "InvalidToolResultError", "ToolResultEncodingError", "ToolConfigurationError", "ToolkitRequiredError", "InvalidUserInputError", () => "internal_failure"), Match.exhaustive),
		...http?.response?.status === void 0 ? {} : { httpStatus: http.response.status }
	};
};
const triageTraceFailure = (cause) => {
	if (Cause.hasInterruptsOnly(cause)) return { failure: "interrupted" };
	if (cause.reasons.some(Cause.isDieReason)) return { failure: "internal_failure" };
	if (cause.reasons.filter(Cause.isFailReason).length !== 1) return { failure: "internal_failure" };
	const error = Cause.findErrorOption(cause);
	const timeout = Schema.Struct({
		_tag: Schema.Literals([
			"RepairInvestigationFailure",
			"RepairTriageFailure",
			"RepairPreparationFailure"
		]),
		reason: Schema.Literals(["provider_timeout", "deadline"])
	});
	if (Option.isSome(error) && Schema.is(timeout)(error.value)) return { failure: "timeout" };
	if (Option.isSome(error)) {
		if (Schema.is(RepairSourceLimit)(error.value)) return { failure: "source_limit" };
		if (Schema.is(RepairSourceReadError)(error.value)) return { failure: error.value.reason };
	}
	return Option.isSome(error) && AiError.isAiError(error.value) ? triageAiFailure(error.value) : { failure: "internal_failure" };
};
const triageFailureReason = (cause) => Match.value(triageTraceFailure(cause).failure).pipe(Match.when("source_limit", () => "investigation_input_limit"), Match.when("source_unavailable", () => "source_unavailable"), Match.when("internal_failure", () => "runtime_unavailable"), Match.when("invalid_output", () => "invalid_output"), Match.when("timeout", () => "deadline"), Match.when("interrupted", () => "interrupted"), Match.whenOr("context_limit", "rate_limit", "authentication", "provider_failure", () => "provider_failure"), Match.exhaustive);
const traceTriageStep = (metadata, work, output = () => void 0) => Effect.gen(function* () {
	const journal = yield* TriageJournal;
	const operationId = yield* (yield* Crypto.Crypto).randomUUIDv4();
	yield* journal.write({
		...metadata,
		operationId,
		status: "started"
	});
	return yield* work.pipe(Effect.onExit((exit) => {
		if (Exit.isSuccess(exit)) {
			const value = output(exit.value);
			return journal.write({
				...metadata,
				operationId,
				status: "completed",
				...value === void 0 ? {} : { output: value }
			});
		}
		return journal.write({
			...metadata,
			operationId,
			status: "failed",
			...triageTraceFailure(exit.cause)
		});
	}));
});
//#endregion
export { TriageJournal, traceTriageStep, triageAiFailure, triageFailureReason, triageTraceFailure };
