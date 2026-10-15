import { Context, Effect } from "effect";
//#region src/server/RepairErrorReporter.ts
/** Hosts install a content-free diagnostic sink in every runtime using the engine. */
var RepairErrorReporter = class extends Context.Service()("@reve/repairs/RepairErrorReporter") {};
const reportSanitizedCause = (cause, options) => Effect.flatMap(RepairErrorReporter, (reporter) => reporter.report(cause, options.operation));
//#endregion
export { RepairErrorReporter, reportSanitizedCause };
