import { DateTime, Effect, Option, Schema } from "effect";
import type { HttpClientResponse } from "effect/http";

export class GithubRateLimited extends Schema.TaggedError<GithubRateLimited>()(
  "GithubRateLimited",
  { retryAfterMs: Schema.Natural },
) {}

type ResponseTiming = Pick<HttpClientResponse.HttpClientResponse, "status" | "headers">;

const seconds = (value: string | undefined) =>
  value === undefined || value.trim() === ""
    ? undefined
    : Option.getOrUndefined(Schema.decodeOption(Schema.FiniteFromString)(value));

/** Provider cooldown only; the action owner retains its deadline and schedules any retry. */
export const githubRetryAfterMs = (
  response: ResponseTiming,
  nowMillis: number,
): number | undefined => {
  // Retain conservative handling of 403: secondary limits may omit timing headers.
  if (response.status !== 403 && response.status !== 429) return undefined;

  const retryAfter = response.headers["retry-after"];
  const retrySeconds = seconds(retryAfter);
  const retryDate = retrySeconds === undefined ? DateTime.make(retryAfter ?? "") : Option.none();
  const retryAt =
    retrySeconds !== undefined
      ? retrySeconds >= 0
        ? nowMillis + retrySeconds * 1_000
        : undefined
      : Option.isSome(retryDate)
        ? DateTime.toEpochMillis(retryDate.value)
        : undefined;
  const remaining = seconds(response.headers["x-ratelimit-remaining"]);
  const reset = seconds(response.headers["x-ratelimit-reset"]);
  const resetAt =
    remaining === 0 && reset !== undefined && reset >= 0 ? reset * 1_000 + 1_000 : undefined;
  // GitHub's one-minute fallback applies only without a usable Retry-After or exhausted reset.
  // https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api#exceeding-the-rate-limit
  const resumeAt =
    retryAt === undefined && resetAt === undefined
      ? nowMillis + 60_000
      : Math.max(retryAt ?? 0, resetAt ?? 0);

  return Math.min(Number.MAX_SAFE_INTEGER, Math.max(0, Math.ceil(resumeAt - nowMillis)));
};

export const checkGithubRateLimit = Effect.fnUntraced(function* (
  response: ResponseTiming,
): Effect.fn.Return<void, GithubRateLimited> {
  if (response.status !== 403 && response.status !== 429) return;
  const delay = githubRetryAfterMs(response, DateTime.toEpochMillis(yield* DateTime.now));
  if (delay !== undefined) return yield* GithubRateLimited.make({ retryAfterMs: delay });
});
