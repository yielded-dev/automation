import { DateTime, Effect, Option, Schema } from "effect";
//#region src/server/GithubRateLimit.ts
var GithubRateLimited = class extends Schema.TaggedError()("GithubRateLimited", { retryAfterMs: Schema.Natural }) {};
const seconds = (value) => value === void 0 || value.trim() === "" ? void 0 : Option.getOrUndefined(Schema.decodeOption(Schema.FiniteFromString)(value));
/** Provider cooldown only; the action owner retains its deadline and schedules any retry. */
const githubRetryAfterMs = (response, nowMillis) => {
	if (response.status !== 403 && response.status !== 429) return void 0;
	const retryAfter = response.headers["retry-after"];
	const retrySeconds = seconds(retryAfter);
	const retryDate = retrySeconds === void 0 ? DateTime.make(retryAfter ?? "") : Option.none();
	const retryAt = retrySeconds !== void 0 ? retrySeconds >= 0 ? nowMillis + retrySeconds * 1e3 : void 0 : Option.isSome(retryDate) ? DateTime.toEpochMillis(retryDate.value) : void 0;
	const remaining = seconds(response.headers["x-ratelimit-remaining"]);
	const reset = seconds(response.headers["x-ratelimit-reset"]);
	const resetAt = remaining === 0 && reset !== void 0 && reset >= 0 ? reset * 1e3 + 1e3 : void 0;
	const resumeAt = retryAt === void 0 && resetAt === void 0 ? nowMillis + 6e4 : Math.max(retryAt ?? 0, resetAt ?? 0);
	return Math.min(Number.MAX_SAFE_INTEGER, Math.max(0, Math.ceil(resumeAt - nowMillis)));
};
const checkGithubRateLimit = Effect.fnUntraced(function* (response) {
	if (response.status !== 403 && response.status !== 429) return;
	const delay = githubRetryAfterMs(response, DateTime.toEpochMillis(yield* DateTime.now));
	if (delay !== void 0) return yield* GithubRateLimited.make({ retryAfterMs: delay });
});
//#endregion
export { GithubRateLimited, checkGithubRateLimit, githubRetryAfterMs };
