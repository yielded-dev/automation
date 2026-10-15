import { Cause, Effect } from "effect";

import { reportSanitizedCause } from "../observability/RuntimeDiagnostics";
import { sanitizeCause } from "../observability/FailureDiagnostic";

import { SessionSandbox } from "../SessionSandbox";

export const withSandboxLease = Effect.fn("SandboxLease.withLease")(function* <A, E, R>(
  id: string,
  effect: Effect.Effect<A, E, R>,
) {
  const sandbox = yield* SessionSandbox;
  const renew = Effect.gen(function* () {
    yield* Effect.sleep("1 minute");
    let retryDelayMillis = 1_000;

    while (true) {
      const renewed = yield* sandbox.lease(id, true, true).pipe(
        Effect.as(true),
        Effect.catchCause((cause) => {
          if (
            cause.reasons.length === 0 ||
            !cause.reasons.every(
              (reason) => Cause.isFailReason(reason) && reason.error.code !== "LEASE_LOST",
            )
          )
            return Effect.failCause(cause);

          // An unavailable renewal does not prove owner loss. Report the first
          // failure in this retry episode without exposing private RPC details.
          return (
            retryDelayMillis === 1_000
              ? reportSanitizedCause(cause, { operation: "SandboxLease.renew" })
              : Effect.logWarning(
                  "Sandbox lease renewal failed; retrying",
                  sanitizeCause(cause, { operation: "SandboxLease.renew" }),
                )
          ).pipe(Effect.as(false));
        }),
      );

      if (renewed) return;
      yield* Effect.sleep(retryDelayMillis);
      retryDelayMillis = Math.min(retryDelayMillis * 2, 10_000);
    }
  }).pipe(Effect.forever);

  return yield* Effect.acquireUseRelease(
    sandbox.lease(id, true),
    () => Effect.raceFirst(effect, renew),
    () => sandbox.lease(id, false),
  );
});
