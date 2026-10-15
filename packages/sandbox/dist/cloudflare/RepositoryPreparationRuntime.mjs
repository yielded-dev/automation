import { reportSanitizedCause, withErrorReportingScope } from "../observability/RuntimeDiagnostics.mjs";
import { RepositoryPreparationGate, RepositoryPreparationObject, executeRepositoryPreparation, runRepositoryPreparationAlarms, scheduleRepositoryPreparation } from "./RepositoryPreparation.mjs";
import { makeRepositoryWorkspaceLayer } from "./TurnLayer.mjs";
import { Effect, Layer } from "effect";
//#region src/cloudflare/RepositoryPreparationRuntime.ts
/** Composition shared by a dedicated workspace owner and existing repair-task namespaces. */
const repositoryPreparationHandlers = (sourcesLayer) => ({
	rpc: {
		requestRepositoryPreparation: (repository) => scheduleRepositoryPreparation(repository).pipe(Effect.provide(sourcesLayer), Effect.scoped, withErrorReportingScope),
		executeRepositoryPreparation: (input) => executeRepositoryPreparation(input).pipe(Effect.provide(sourcesLayer), Effect.scoped, Effect.catchCause((cause) => reportSanitizedCause(cause, { operation: "RepositoryPreparation.execute" }).pipe(Effect.as({ status: "unavailable" }))), withErrorReportingScope)
	},
	alarms: runRepositoryPreparationAlarms.pipe(Effect.provide(sourcesLayer), Effect.scoped, withErrorReportingScope)
});
/** Model-free preparation on host-owned resources; no repair records or coding provider required. */
const makeRepositoryPreparationDurableObject = (options) => {
	const runtime = Layer.mergeAll(makeRepositoryWorkspaceLayer(options), RepositoryPreparationGate.layer, RepositoryPreparationObject.layer({ binding: options.coordinatorBinding })).pipe(Layer.provideMerge(options.diagnosticsLayer));
	const handlers = repositoryPreparationHandlers(options.sourcesLayer);
	return RepositoryPreparationObject.make(runtime, handlers);
};
//#endregion
export { makeRepositoryPreparationDurableObject, repositoryPreparationHandlers };
