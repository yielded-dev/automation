import {
  Crypto,
  Cause,
  Context,
  Effect,
  Exit,
  Match,
  Option,
  PlatformError,
  Result,
  Schema,
} from "effect";
import { AiError } from "effect/ai";

import { RepairSourceLimit, RepairSourceReadError } from "../domain/Investigation";
import { TriageFailureKind, type TriageTraceData } from "../domain/TriageTrace";

export class TriageJournal extends Context.Service<
  TriageJournal,
  { readonly write: (event: TriageTraceData) => Effect.Effect<void> }
>()("@reve/repairs/TriageJournal") {}

const ContextError = Schema.fromJsonString(
  Schema.Struct({ detail: Schema.Struct({ error_type: Schema.Literal("max_tokens_exceeded") }) }),
);

export const triageAiFailure = (error: AiError.AiError) => {
  const reason = error.reason;
  const http = "http" in reason ? reason.http : undefined;
  const body = http?.body;
  const failure: typeof TriageFailureKind.Type = Match.value(reason).pipe(
    Match.tag("RateLimitError", () => "rate_limit" as const),
    Match.tag("AuthenticationError", () => "authentication" as const),
    Match.tag("InvalidRequestError", () =>
      typeof body === "string" && Result.isSuccess(Schema.decodeResult(ContextError)(body))
        ? ("context_limit" as const)
        : ("provider_failure" as const),
    ),
    Match.tag(
      "InternalProviderError",
      "QuotaExhaustedError",
      "ContentPolicyError",
      () => "provider_failure" as const,
    ),
    Match.tag("NetworkError", (reason) =>
      reason.reason === "TransportError"
        ? ("provider_failure" as const)
        : ("internal_failure" as const),
    ),
    Match.tag(
      "InvalidOutputError",
      "StructuredOutputError",
      "ToolNotFoundError",
      "ToolParameterValidationError",
      () => "invalid_output" as const,
    ),
    Match.tag(
      "UnsupportedSchemaError",
      "UnknownError",
      "InvalidToolResultError",
      "ToolResultEncodingError",
      "ToolConfigurationError",
      "ToolkitRequiredError",
      "InvalidUserInputError",
      () => "internal_failure" as const,
    ),
    Match.exhaustive,
  );

  return {
    failure,
    ...(http?.response?.status === undefined ? {} : { httpStatus: http.response.status }),
  };
};

export const triageTraceFailure = (cause: Cause.Cause<unknown>) => {
  if (Cause.hasInterruptsOnly(cause)) return { failure: "interrupted" as const };
  if (cause.reasons.some(Cause.isDieReason)) return { failure: "internal_failure" as const };
  if (cause.reasons.filter(Cause.isFailReason).length !== 1)
    return { failure: "internal_failure" as const };
  const error = Cause.findErrorOption(cause);
  const timeout = Schema.Struct({
    _tag: Schema.Literals([
      "RepairInvestigationFailure",
      "RepairTriageFailure",
      "RepairPreparationFailure",
    ]),
    reason: Schema.Literals(["provider_timeout", "deadline"]),
  });

  if (Option.isSome(error) && Schema.is(timeout)(error.value))
    return { failure: "timeout" as const };

  if (Option.isSome(error)) {
    if (Schema.is(RepairSourceLimit)(error.value)) return { failure: "source_limit" as const };
    if (Schema.is(RepairSourceReadError)(error.value)) return { failure: error.value.reason };
  }

  return Option.isSome(error) && AiError.isAiError(error.value)
    ? triageAiFailure(error.value)
    : { failure: "internal_failure" as const };
};

export const triageFailureReason = (cause: Cause.Cause<unknown>) =>
  Match.value(triageTraceFailure(cause).failure).pipe(
    Match.when("source_limit", () => "investigation_input_limit" as const),
    Match.when("source_unavailable", () => "source_unavailable" as const),
    Match.when("internal_failure", () => "runtime_unavailable" as const),
    Match.when("invalid_output", () => "invalid_output" as const),
    Match.when("timeout", () => "deadline" as const),
    Match.when("interrupted", () => "interrupted" as const),
    Match.whenOr(
      "context_limit",
      "rate_limit",
      "authentication",
      "provider_failure",
      () => "provider_failure" as const,
    ),
    Match.exhaustive,
  );

export const traceTriageStep = <A, E, R>(
  metadata: Omit<TriageTraceData, "operationId" | "status" | "output" | "failure" | "httpStatus">,
  work: Effect.Effect<A, E, R>,
  output: (value: A) => TriageTraceData["output"] = () => undefined,
): Effect.Effect<A, E | PlatformError.PlatformError, R | TriageJournal | Crypto.Crypto> =>
  Effect.gen(function* () {
    const journal = yield* TriageJournal;
    const operationId = yield* (yield* Crypto.Crypto).randomUUIDv4();

    yield* journal.write({ ...metadata, operationId, status: "started" });

    return yield* work.pipe(
      Effect.onExit((exit) => {
        if (Exit.isSuccess(exit)) {
          const value = output(exit.value);

          return journal.write({
            ...metadata,
            operationId,
            status: "completed",
            ...(value === undefined ? {} : { output: value }),
          });
        }

        return journal.write({
          ...metadata,
          operationId,
          status: "failed",
          ...triageTraceFailure(exit.cause),
        });
      }),
    );
  });
