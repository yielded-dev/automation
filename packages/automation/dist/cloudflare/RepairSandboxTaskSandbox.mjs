import { Effect } from "effect";
import { SandboxAgentConfig } from "@yielded/sandbox/cloudflare/Config";
import { Sandboxes } from "@yielded/sandbox/cloudflare/Bindings";
import { sessionResourceNames } from "@yielded/sandbox/cloudflare/resource-names";
//#region src/cloudflare/RepairSandboxTaskSandbox.ts
const repairSandboxTaskSessionId = (taskId, purpose) => {
	switch (purpose) {
		case "turn": return taskId;
		case "validation": return `${taskId}:repair-validation`;
		case "export": return `${taskId}:repository-export`;
		case "source": return `${taskId}:source-preparation`;
	}
};
/** One namespace and deterministic helper names, including exact-boot recovery. */
const repairSandboxTaskSandbox = Effect.fn("repairSandboxTaskSandbox")(function* (taskId, purpose, options = {}) {
	const config = yield* SandboxAgentConfig;
	const names = yield* sessionResourceNames(repairSandboxTaskSessionId(taskId, purpose), config.sandboxNamespace);
	return yield* Sandboxes.get(names.sandboxName, options);
});
//#endregion
export { repairSandboxTaskSandbox, repairSandboxTaskSessionId };
