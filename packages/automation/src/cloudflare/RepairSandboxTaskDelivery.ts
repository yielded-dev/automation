import { DateTime, Effect, Option } from "effect";
import { ConflictError } from "../domain/errors";
import { RepairSandboxAlarms } from "./RepairSandboxAlarms";
import { RepairSandboxTaskStore, type RepairSandboxState } from "./RepairSandboxTaskStore";
import { preserveRepairSandboxFailure } from "./RepairSandboxTaskError";
import {
  RepairSandboxTaskNotificationError,
  RepairSandboxTaskNotifications,
  type RepairSandboxTaskSettled,
} from "./RepairSandboxTaskNotifications";

const isTerminal = (state: RepairSandboxState) =>
  state.status === "succeeded" || state.status === "failed" || state.status === "cancelled";

/** The caller commits this schedule in the same transaction as the first terminal transition. */
export const repairSandboxTaskSettlement = Effect.fnUntraced(function* (
  previous: RepairSandboxState,
  current: RepairSandboxState,
) {
  if (current.request.execution === "commands" || isTerminal(previous) || !isTerminal(current))
    return undefined;
  if (Option.isNone(yield* Effect.serviceOption(RepairSandboxTaskNotifications))) return undefined;
  return {
    tag: "settled" as const,
    id: "host",
    payload: { taskId: current.request.id },
    runAt: yield* DateTime.now,
    // A terminal transition advances beyond the run's execution progress, even if it was parked.
    progress: current.latestSequence + 1,
  };
});

export const deliverRepairSandboxTaskSettlement = Effect.fnUntraced(function* (
  event: RepairSandboxTaskSettled,
) {
  const state = yield* (yield* RepairSandboxTaskStore).required;
  if (
    state.request.id !== event.taskId ||
    state.request.execution === "commands" ||
    !isTerminal(state)
  )
    return yield* ConflictError.make({ message: "The repair terminal state is not committed" });

  const alarms = yield* RepairSandboxAlarms;
  // Pre-arm before the host side effect: a lost reply or isolate must retain delivery work.
  yield* alarms.scheduleAlarm({
    tag: "settled",
    id: "host",
    payload: event,
    runAt: yield* DateTime.now,
  });
  const notifications = yield* Effect.serviceOption(RepairSandboxTaskNotifications);
  if (Option.isNone(notifications))
    return yield* RepairSandboxTaskNotificationError.make({
      cause: new Error("The host task notification adapter is unavailable"),
    });
  yield* notifications.value.settled(event);
  yield* alarms.cancelAlarm({ tag: "settled", id: "host" });
}, preserveRepairSandboxFailure);
