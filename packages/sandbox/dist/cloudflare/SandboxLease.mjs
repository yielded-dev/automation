import { SessionSandbox } from "../SessionSandbox.mjs";
import { sanitizeCause } from "../observability/FailureDiagnostic.mjs";
import { reportSanitizedCause } from "../observability/RuntimeDiagnostics.mjs";
import { Cause, Effect } from "effect";
//#region src/cloudflare/SandboxLease.ts
const withSandboxLease = Effect.fn("SandboxLease.withLease")(function* (id, effect) {
	const sandbox = yield* SessionSandbox;
	const renew = Effect.gen(function* () {
		yield* Effect.sleep("1 minute");
		let retryDelayMillis = 1e3;
		while (true) {
			if (yield* sandbox.lease(id, true, true).pipe(Effect.as(true), Effect.catchCause((cause) => {
				if (cause.reasons.length === 0 || !cause.reasons.every((reason) => Cause.isFailReason(reason) && reason.error.code !== "LEASE_LOST")) return Effect.failCause(cause);
				return (retryDelayMillis === 1e3 ? reportSanitizedCause(cause, { operation: "SandboxLease.renew" }) : Effect.logWarning("Sandbox lease renewal failed; retrying", sanitizeCause(cause, { operation: "SandboxLease.renew" }))).pipe(Effect.as(false));
			}))) return;
			yield* Effect.sleep(retryDelayMillis);
			retryDelayMillis = Math.min(retryDelayMillis * 2, 1e4);
		}
	}).pipe(Effect.forever);
	return yield* Effect.acquireUseRelease(sandbox.lease(id, true), () => Effect.raceFirst(effect, renew), () => sandbox.lease(id, false));
});
//#endregion
export { withSandboxLease };
