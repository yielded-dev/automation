import { Cause, Clock, Effect, Option, Schedule } from "effect";

import { sanitizeCause } from "../observability/FailureDiagnostic";

import { SessionSandbox } from "../SessionSandbox";
import type { ExecOptions, SandboxCommand } from "./NativeSandbox";

/** Observe the accepted process again on disconnect; never replay its command. */
export const runSandboxCommand = Effect.fnUntraced(function* (
  command: SandboxCommand,
  options: ExecOptions,
  outputOptions: {
    readonly timeout: number;
    readonly maxBytes: number;
    readonly operation: string;
  },
) {
  const sandbox = yield* SessionSandbox;
  const handle = yield* sandbox.exec(command, options);
  const { timeout, maxBytes, operation } = outputOptions;
  const observationDeadline = (yield* Clock.currentTimeMillis) + timeout;

  return yield* handle.outputText({ timeout, maxBytes }).pipe(
    Effect.catchTag("SandboxOperationError", (error) =>
      Effect.gen(function* () {
        const remaining = observationDeadline - (yield* Clock.currentTimeMillis);

        if (remaining <= 0) return yield* error;
        yield* Effect.logWarning(
          "Sandbox command observation disconnected",
          sanitizeCause(Cause.fail(error), { operation }),
        );

        return yield* Effect.gen(function* () {
          const process = yield* sandbox.getProcess(handle.id);

          if (Option.isNone(process)) return yield* error;

          return yield* process.value.outputText({
            timeout: Math.max(1, observationDeadline - (yield* Clock.currentTimeMillis)),
            maxBytes,
          });
        }).pipe(
          Effect.retry({
            times: 1,
            while: (failure) => failure._tag === "SandboxOperationError",
            schedule: Schedule.exponential("250 millis"),
          }),
          Effect.timeoutOrElse({ duration: remaining, orElse: () => Effect.fail(error) }),
        );
      }),
    ),
  );
});
