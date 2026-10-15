import { Cause, Context, Effect } from "effect";
import { sanitizeCause, type FailureDiagnosticOptions } from "./FailureDiagnostic";

/** Host reporting boundary. The host owns SDK installation, scope and duplicate suppression. */
export interface RuntimeDiagnostics {
  readonly reportCause: (
    cause: Cause.Cause<unknown>,
    options: FailureDiagnosticOptions,
  ) => Effect.Effect<void>;
  readonly scope: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
}

/** Default diagnostics contain no provider content; hosts can supply their existing reporter. */
export const RuntimeDiagnostics = Context.Reference<RuntimeDiagnostics>(
  "@reve/repairs/RuntimeDiagnostics",
  {
    defaultValue: () => ({
      reportCause: (cause, options) =>
        Cause.hasInterruptsOnly(cause)
          ? Effect.void
          : Effect.logError("Sandbox operation failed", sanitizeCause(cause, options)),
      scope: (effect) => effect,
    }),
  },
);

export const reportSanitizedCause = <E>(cause: Cause.Cause<E>, options: FailureDiagnosticOptions) =>
  Effect.flatMap(RuntimeDiagnostics, (diagnostics) => diagnostics.reportCause(cause, options));

export const withErrorReportingScope = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> =>
  Effect.flatMap(RuntimeDiagnostics, (diagnostics) => diagnostics.scope(effect));
