import { sanitizeCause } from "./FailureDiagnostic.mjs";
import { Cause, Context, Effect } from "effect";
//#region src/observability/RuntimeDiagnostics.ts
/** Default diagnostics contain no provider content; hosts can supply their existing reporter. */
const RuntimeDiagnostics = Context.Reference("@reve/repairs/RuntimeDiagnostics", { defaultValue: () => ({
	reportCause: (cause, options) => Cause.hasInterruptsOnly(cause) ? Effect.void : Effect.logError("Sandbox operation failed", sanitizeCause(cause, options)),
	scope: (effect) => effect
}) });
const reportSanitizedCause = (cause, options) => Effect.flatMap(RuntimeDiagnostics, (diagnostics) => diagnostics.reportCause(cause, options));
const withErrorReportingScope = (effect) => Effect.flatMap(RuntimeDiagnostics, (diagnostics) => diagnostics.scope(effect));
//#endregion
export { RuntimeDiagnostics, reportSanitizedCause, withErrorReportingScope };
