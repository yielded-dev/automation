import { ConflictError, ServiceUnavailableError } from "../domain/errors.mjs";
import { RepairSandboxTaskObject } from "./RepairSandboxTaskObject.mjs";
import { RepairSandboxTaskStore } from "./RepairSandboxTaskStore.mjs";
import { RepairSandboxTaskControl } from "./RepairSandboxTaskControl.mjs";
import "./RepairSandboxTaskNotifications.mjs";
import { RepairSandboxAlarms } from "./RepairSandboxAlarms.mjs";
import { RepairSandboxPreparation } from "./RepairSandboxPreparation.mjs";
import { deliverRepairSandboxTaskSettlement } from "./RepairSandboxTaskDelivery.mjs";
import { cancelRepairSandboxTask, cleanupRepairSandboxTask, createRepairSandboxTask, eventsRepairSandboxTask, forwardRepairSandboxTask, inspectRepairSandboxTask, nativeRepairSandboxTask, readRepairContinuation, runRepairSandboxTask, snapshotRepairSandboxTask } from "./RepairSandboxTaskExecution.mjs";
import { Cause, Effect, Layer, Schema } from "effect";
import { DurableObjectSqlite, DurableObjectState } from "effect-cf";
import { makeRepositoryWorkspaceLayer, makeSandboxAgentTurnLayer } from "@yielded/sandbox/cloudflare/TurnLayer";
import { OutboundAuthority } from "@yielded/sandbox/cloudflare/OutboundAuthority";
import { RepositoryPreparationGate, RepositoryPreparationObject, RepositoryPreparationSources, isRepositoryPreparationCoordinator, isRepositoryPreparationOwner } from "@yielded/sandbox/cloudflare/RepositoryPreparation";
import { repositoryPreparationHandlers } from "@yielded/sandbox/cloudflare/RepositoryPreparationRuntime";
import { reportSanitizedCause, withErrorReportingScope } from "@yielded/sandbox/observability/RuntimeDiagnostics";
//#region src/cloudflare/RepairSandboxTaskDurableObject.ts
const outcome = (operation, effect) => effect.pipe(Effect.map((value) => ({
	_tag: "Success",
	value
})), Effect.catchCause((cause) => {
	if (Cause.hasInterruptsOnly(cause)) return Effect.failCause(Cause.map(cause, () => ServiceUnavailableError.make({ message: "Repair sandbox execution is unavailable" })));
	if (cause.reasons.length === 1) {
		const reason = cause.reasons[0];
		if (Cause.isFailReason(reason) && Schema.is(ConflictError)(reason.error)) return Effect.succeed(reason.error);
	}
	const unavailable = ServiceUnavailableError.make({ message: "Repair sandbox execution is unavailable" });
	return reportSanitizedCause(cause, { operation }).pipe(Effect.andThen(cause.reasons.every(Cause.isFailReason) ? Effect.succeed(unavailable) : Effect.failCause(Cause.map(cause, () => unavailable))));
}), withErrorReportingScope);
/** Accepted work and native recovery run locally on the host's own Cloudflare resources. */
const makeRepairSandboxTaskDurableObject = (options) => {
	const taskRuntime = Layer.mergeAll(makeSandboxAgentTurnLayer(options), RepairSandboxTaskStore.layer, RepairSandboxTaskControl.layer, RepairSandboxTaskObject.layer({ binding: options.coordinatorBinding }), RepositoryPreparationObject.layer({ binding: options.coordinatorBinding }), RepositoryPreparationGate.layer, options.preparationLayer ?? RepairSandboxPreparation.noopLayer, options.outboundAuthorityLayer ?? OutboundAuthority.noneLayer, options.taskNotificationsLayer ?? Layer.empty).pipe(Layer.provideMerge(DurableObjectSqlite.layer()), Layer.tapCause((cause) => reportSanitizedCause(cause, { operation: "RepairSandboxTask.initialize" }).pipe(withErrorReportingScope)), Layer.provideMerge(options.diagnosticsLayer));
	const preparationRuntime = Layer.mergeAll(makeRepositoryWorkspaceLayer(options), RepositoryPreparationObject.layer({ binding: options.coordinatorBinding }), RepositoryPreparationGate.layer).pipe(Layer.provideMerge(options.diagnosticsLayer));
	const runtime = Layer.unwrap(Effect.map(DurableObjectState.DurableObjectState, (state) => isRepositoryPreparationOwner(state.id.name) ? preparationRuntime : taskRuntime));
	const preparation = repositoryPreparationHandlers(options.repositoryPreparationLayer ?? RepositoryPreparationSources.unconfiguredLayer);
	const taskAlarms = RepairSandboxAlarms.handlers({
		run: () => runRepairSandboxTask(options.turnPolicy).pipe(Effect.provide(taskRuntime), withErrorReportingScope),
		settled: ({ payload }) => deliverRepairSandboxTaskSettlement(payload).pipe(Effect.provide(taskRuntime)),
		cleanup: () => cleanupRepairSandboxTask.pipe(Effect.provide(taskRuntime), Effect.tapCause((cause) => reportSanitizedCause(cause, { operation: "RepairSandboxTask.cleanup" })), withErrorReportingScope)
	});
	return RepairSandboxTaskObject.make(runtime, {
		rpc: {
			...preparation.rpc,
			create: (request) => outcome("RepairSandboxTask.create", createRepairSandboxTask(request).pipe(Effect.provide(taskRuntime))),
			inspect: (id) => outcome("RepairSandboxTask.inspect", inspectRepairSandboxTask(id).pipe(Effect.provide(taskRuntime))),
			snapshot: (id) => outcome("RepairSandboxTask.snapshot", snapshotRepairSandboxTask(id).pipe(Effect.provide(taskRuntime))),
			events: (id, after, limit) => outcome("RepairSandboxTask.events", eventsRepairSandboxTask(id, after, limit).pipe(Effect.provide(taskRuntime))),
			cancel: (id, commandId, request) => outcome("RepairSandboxTask.cancel", cancelRepairSandboxTask(id, commandId, request).pipe(Effect.provide(taskRuntime))),
			native: (id, request) => outcome("RepairSandboxTask.native", nativeRepairSandboxTask(id, request).pipe(Effect.provide(taskRuntime))),
			continuation: (request) => outcome("RepairSandboxTask.continuation", readRepairContinuation(request).pipe(Effect.provide(taskRuntime))),
			forward: (id, purpose, operationId, request) => forwardRepairSandboxTask(id, purpose, operationId, new Request(request)).pipe(Effect.provide(taskRuntime), Effect.catchCause((cause) => {
				if (Cause.hasInterruptsOnly(cause)) return Effect.failCause(Cause.map(cause, () => ServiceUnavailableError.make({ message: "Repair sandbox execution is unavailable" })));
				const reason = cause.reasons[0];
				if (cause.reasons.length === 1 && reason !== void 0 && Cause.isFailReason(reason) && Schema.is(ConflictError)(reason.error)) return Effect.succeed(new Response(null, { status: 409 }));
				return reportSanitizedCause(cause, { operation: "RepairSandboxTask.forward" }).pipe(Effect.andThen(cause.reasons.every(Cause.isFailReason) ? Effect.succeed(new Response(null, { status: 503 })) : Effect.failCause(Cause.map(cause, () => ServiceUnavailableError.make({ message: "Repair sandbox execution is unavailable" })))));
			}), withErrorReportingScope)
		},
		alarms: {
			...taskAlarms,
			run: Effect.gen(function* () {
				const state = yield* DurableObjectState.DurableObjectState;
				if (isRepositoryPreparationCoordinator(state.id.name)) return yield* preparation.alarms;
				return yield* taskAlarms.run;
			})
		}
	});
};
//#endregion
export { makeRepairSandboxTaskDurableObject };
