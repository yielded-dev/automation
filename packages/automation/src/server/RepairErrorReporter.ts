import { Cause, Context, Effect } from "effect";

/** Hosts install a content-free diagnostic sink in every runtime using the engine. */
export class RepairErrorReporter extends Context.Service<
  RepairErrorReporter,
  {
    readonly report: (cause: Cause.Cause<unknown>, operation: string) => Effect.Effect<void>;
  }
>()("@reve/repairs/RepairErrorReporter") {}

export const reportSanitizedCause = (
  cause: Cause.Cause<unknown>,
  options: { readonly operation: string },
) => Effect.flatMap(RepairErrorReporter, (reporter) => reporter.report(cause, options.operation));
