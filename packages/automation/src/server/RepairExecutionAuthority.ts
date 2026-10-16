import { DateTime, Effect } from "effect";

import { ConflictError } from "../domain/errors";
import { RunState } from "../domain/RepairExecution";
import { RepairAuthorization } from "./RepairAuthorization";
import { RepairExecutionRepository } from "./database/RepairExecutionRepository";

import { RepairExecutionContext } from "./RepairExecutionContext";
export { RunLaunch, RunState } from "../domain/RepairExecution";
export const readRunState = Effect.gen(function* () {
  const records = yield* RepairExecutionRepository;
  const runId = yield* RepairExecutionContext;
  return (yield* records.read(runId))?.state;
});
export const updateRunState = (change: (state: RunState) => RunState) =>
  Effect.gen(function* () {
    const records = yield* RepairExecutionRepository;
    return yield* records.transition(yield* RepairExecutionContext, change);
  });
export const isRunStopped = Effect.map(
  readRunState,
  (state) => state === undefined || state.stopReason !== null || state.retirement !== "active",
);

const authorize = Effect.fnUntraced(function* (state: RunState | undefined, publication: boolean) {
  if (
    state === undefined ||
    state.stopReason !== null ||
    state.retirement !== "active" ||
    state.origin.deadlineAtMs <= DateTime.toEpochMillis(yield* DateTime.now)
  )
    return false;

  return yield* (yield* RepairAuthorization).authorize(state, publication);
});

// Pause lets accepted execution finish, but fences each new publication write.
export const authorizeRunState = (state: RunState) => authorize(state, false);
export const authorizeRun = Effect.flatMap(readRunState, (state) => authorize(state, false));
export const authorizePublication = Effect.flatMap(readRunState, (state) => authorize(state, true));

export const requireLiveRun = Effect.gen(function* () {
  const state = yield* readRunState;

  if (!state || !(yield* authorizeRun) || state.outcome !== "completed")
    return yield* ConflictError.make({
      message: "The repair run is not ready or its authority has expired",
    });

  return state;
});
