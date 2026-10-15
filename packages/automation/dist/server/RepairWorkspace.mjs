import { ConflictError } from "../domain/errors.mjs";
import { repairIdentity } from "../domain/launch.mjs";
import { RepairSandboxTaskClient } from "./RepairSandboxTaskClient.mjs";
import { RepairExecutionContext } from "./RepairExecutionContext.mjs";
import { readRunState } from "./RepairExecutionAuthority.mjs";
import { remoteRepairSandbox } from "./RemoteSandbox.mjs";
import { Effect, Schema } from "effect";
//#region src/server/RepairWorkspace.ts
const repairExecutionSnapshot = Effect.gen(function* () {
	const runId = yield* RepairExecutionContext;
	return yield* (yield* RepairSandboxTaskClient).snapshot(runId);
});
const repairHelperSandbox = (purpose, options = {}) => Effect.gen(function* () {
	const state = yield* readRunState;
	if (state === void 0) return yield* ConflictError.make({ message: "The repair execution is unavailable" });
	return yield* remoteRepairSandbox(state.runId, purpose, state.origin.deadlineAtMs, options);
});
const repairNativeRequest = (purpose, action) => Effect.gen(function* () {
	return yield* (yield* RepairSandboxTaskClient).native(yield* RepairExecutionContext, {
		purpose,
		action,
		operationId: crypto.randomUUID()
	}, Schema.Json);
});
const ensureRepairCommit = (head) => repairNativeRequest("export", {
	_tag: "EnsureCommit",
	head
}).pipe(Effect.asVoid);
const prepareRepairHelperRepository = Effect.fn("RepairExecution.prepareHelperRepository")(function* (head, scope, expectedInstanceId) {
	const runId = yield* RepairExecutionContext;
	yield* (yield* RepairSandboxTaskClient).native(runId, {
		purpose: "export",
		expectedInstanceId,
		operationId: repairIdentity("helper-repository", runId, "export", head, scope, expectedInstanceId),
		action: {
			_tag: "PrepareRepository",
			head,
			scope,
			resetReceipt: false
		}
	}, Schema.Json);
});
//#endregion
export { ensureRepairCommit, prepareRepairHelperRepository, repairExecutionSnapshot, repairHelperSandbox };
