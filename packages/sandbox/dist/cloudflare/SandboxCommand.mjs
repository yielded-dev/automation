import { SessionSandbox } from "../SessionSandbox.mjs";
import { sanitizeCause } from "../observability/FailureDiagnostic.mjs";
import { Cause, Clock, Effect, Option, Schedule } from "effect";
//#region src/cloudflare/SandboxCommand.ts
/** Observe the accepted process again on disconnect; never replay its command. */
const runSandboxCommand = Effect.fnUntraced(function* (command, options, outputOptions) {
	const sandbox = yield* SessionSandbox;
	const handle = yield* sandbox.exec(command, options);
	const { timeout, maxBytes, operation } = outputOptions;
	const observationDeadline = (yield* Clock.currentTimeMillis) + timeout;
	return yield* handle.outputText({
		timeout,
		maxBytes
	}).pipe(Effect.catchTag("SandboxOperationError", (error) => Effect.gen(function* () {
		const remaining = observationDeadline - (yield* Clock.currentTimeMillis);
		if (remaining <= 0) return yield* error;
		yield* Effect.logWarning("Sandbox command observation disconnected", sanitizeCause(Cause.fail(error), { operation }));
		return yield* Effect.gen(function* () {
			const process = yield* sandbox.getProcess(handle.id);
			if (Option.isNone(process)) return yield* error;
			return yield* process.value.outputText({
				timeout: Math.max(1, observationDeadline - (yield* Clock.currentTimeMillis)),
				maxBytes
			});
		}).pipe(Effect.retry({
			times: 1,
			while: (failure) => failure._tag === "SandboxOperationError",
			schedule: Schedule.exponential("250 millis")
		}), Effect.timeoutOrElse({
			duration: remaining,
			orElse: () => Effect.fail(error)
		}));
	})));
});
//#endregion
export { runSandboxCommand };
