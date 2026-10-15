import { ConflictError } from "../domain/errors.mjs";
import { RunLaunch, RunState } from "../domain/RepairExecution.mjs";
import { RepairAuthorization } from "./RepairAuthorization.mjs";
import { RepairExecutionRepository } from "./database/RepairExecutionRepository.mjs";
import { RepairExecutionContext } from "./RepairExecutionContext.mjs";
import { DateTime, Effect } from "effect";
//#region src/server/RepairExecutionAuthority.ts
const readRunState = Effect.gen(function* () {
	const records = yield* RepairExecutionRepository;
	const runId = yield* RepairExecutionContext;
	return (yield* records.read(runId))?.state;
});
const updateRunState = (change) => Effect.gen(function* () {
	return yield* (yield* RepairExecutionRepository).transition(yield* RepairExecutionContext, change);
});
const isRunStopped = Effect.map(readRunState, (state) => state === void 0 || state.stopReason !== null || state.retirement !== "active");
const authorize = Effect.fnUntraced(function* (state, publication) {
	if (state === void 0 || state.stopReason !== null || state.retirement !== "active" || state.origin.deadlineAtMs <= DateTime.toEpochMillis(yield* DateTime.now)) return false;
	return yield* (yield* RepairAuthorization).authorize(state, publication);
});
const authorizeRunState = (state) => authorize(state, false);
const authorizeRun = Effect.flatMap(readRunState, (state) => authorize(state, false));
const authorizePublication = Effect.flatMap(readRunState, (state) => authorize(state, true));
const requireLiveRun = Effect.gen(function* () {
	const state = yield* readRunState;
	if (!state || !(yield* authorizeRun) || state.outcome !== "completed") return yield* ConflictError.make({ message: "The repair run is not ready or its authority has expired" });
	return state;
});
//#endregion
export { RunLaunch, RunState, authorizePublication, authorizeRun, authorizeRunState, isRunStopped, readRunState, requireLiveRun, updateRunState };
