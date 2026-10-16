import { Effect } from "effect";
import { ConflictError } from "../domain/errors";
import { readRunState, updateRunState } from "./RepairExecutionAuthority";
import { RepairSandboxTaskClient } from "./RepairSandboxTaskClient";

/** Native setup may close an accepted execution before any model receipt exists. */
export const reconcileRepairNativeStatus = Effect.gen(function* () {
  const state = yield* readRunState;
  if (state === undefined)
    return yield* ConflictError.make({ message: "Repair execution has not been admitted" });
  if (state.retirement !== "active" || !["pending", "running"].includes(state.outcome))
    return state;
  const native = yield* (yield* RepairSandboxTaskClient).inspect(state.runId);
  if (native === null) return state;
  if (native.id !== state.runId)
    return yield* ConflictError.make({ message: "The native status belongs to a different run" });
  if (native.status !== "failed") return state;
  return yield* updateRunState((current) =>
    current.stopReason === null &&
    current.retirement === "active" &&
    ["pending", "running"].includes(current.outcome)
      ? { ...current, outcome: "failed" }
      : current,
  );
});
