import { ConflictError, ServiceUnavailableError } from "../domain/errors.mjs";
import { RepairErrorReporter } from "./RepairErrorReporter.mjs";
import { RepairExecutionContext } from "./RepairExecutionContext.mjs";
import { publicationPushMessage } from "./PublicationPushFailure.mjs";
import { PublicationError, abandonPublication, publishRepair, reconcilePublication } from "./Publication.mjs";
import { inspectRepairExecution, reconcileRepairExecution, reportRepairExecution, retireRepairExecution, startRepairExecution } from "./RepairExecution.mjs";
import { readRunEvents } from "./RunOutput.mjs";
import { Cause, Context, Effect, Layer, Schema } from "effect";
//#region src/server/RunClient.ts
const make = Effect.gen(function* () {
	const context = yield* Effect.context();
	const reporter = yield* RepairErrorReporter;
	const call = (runId, effect) => effect.pipe(Effect.provideService(RepairExecutionContext, runId), Effect.provide(context), Effect.catchCause((cause) => {
		if (cause.reasons.every((reason) => Cause.isFailReason(reason) && Schema.is(ConflictError)(reason.error))) return Effect.failCause(Cause.map(cause, () => ConflictError.make({ message: "Repair execution rejected the operation" })));
		return reporter.report(cause, "RepairExecutionClient").pipe(Effect.andThen(Effect.failCause(Cause.map(cause, (error) => ServiceUnavailableError.make({ message: Schema.is(PublicationError)(error) && error.reason !== void 0 ? publicationPushMessage(error.reason, error.diagnostic) : "Repair execution is unavailable" })))));
	}));
	return {
		start: (input) => call(input.runId, startRepairExecution(input)),
		inspect: (runId) => call(runId, inspectRepairExecution),
		report: (runId) => call(runId, reportRepairExecution),
		events: (runId, after = 0, limit = 1e3) => call(runId, readRunEvents(after, limit)),
		retire: (launch, reason) => call(launch.runId, retireRepairExecution(launch, reason)),
		reconcilePublication: (runId) => call(runId, reconcilePublication),
		abandonPublication: (runId, requestId) => call(runId, abandonPublication(requestId).pipe(Effect.as({ accepted: true }))),
		publish: (runId, input) => call(runId, reconcileRepairExecution.pipe(Effect.andThen(publishRepair(input))))
	};
});
/** Application policy and receipts in PG; execution is exclusively the shared SandboxTask service. */
var RunClient = class extends Context.Service()("repair-tool/RunClient") {
	static layer = Layer.effect(this, make);
};
//#endregion
export { RunClient };
