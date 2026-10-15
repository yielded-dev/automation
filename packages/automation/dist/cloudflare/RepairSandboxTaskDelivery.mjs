import { ConflictError } from "../domain/errors.mjs";
import { RepairSandboxTaskStore } from "./RepairSandboxTaskStore.mjs";
import { RepairSandboxTaskNotificationError, RepairSandboxTaskNotifications } from "./RepairSandboxTaskNotifications.mjs";
import { RepairSandboxAlarms } from "./RepairSandboxAlarms.mjs";
import { preserveRepairSandboxFailure } from "./RepairSandboxTaskError.mjs";
import { DateTime, Effect, Option } from "effect";
//#region src/cloudflare/RepairSandboxTaskDelivery.ts
const isTerminal = (state) => state.status === "succeeded" || state.status === "failed" || state.status === "cancelled";
/** The caller commits this schedule in the same transaction as the first terminal transition. */
const repairSandboxTaskSettlement = Effect.fnUntraced(function* (previous, current) {
	if (current.request.execution === "commands" || isTerminal(previous) || !isTerminal(current)) return void 0;
	if (Option.isNone(yield* Effect.serviceOption(RepairSandboxTaskNotifications))) return void 0;
	return {
		tag: "settled",
		id: "host",
		payload: { taskId: current.request.id },
		runAt: yield* DateTime.now,
		progress: current.latestSequence + 1
	};
});
const deliverRepairSandboxTaskSettlement = Effect.fnUntraced(function* (event) {
	const state = yield* (yield* RepairSandboxTaskStore).required;
	if (state.request.id !== event.taskId || state.request.execution === "commands" || !isTerminal(state)) return yield* ConflictError.make({ message: "The repair terminal state is not committed" });
	const alarms = yield* RepairSandboxAlarms;
	yield* alarms.scheduleAlarm({
		tag: "settled",
		id: "host",
		payload: event,
		runAt: yield* DateTime.now
	});
	const notifications = yield* Effect.serviceOption(RepairSandboxTaskNotifications);
	if (Option.isNone(notifications)) return yield* RepairSandboxTaskNotificationError.make({ cause: /* @__PURE__ */ new Error("The host task notification adapter is unavailable") });
	yield* notifications.value.settled(event);
	yield* alarms.cancelAlarm({
		tag: "settled",
		id: "host"
	});
}, preserveRepairSandboxFailure);
//#endregion
export { deliverRepairSandboxTaskSettlement, repairSandboxTaskSettlement };
