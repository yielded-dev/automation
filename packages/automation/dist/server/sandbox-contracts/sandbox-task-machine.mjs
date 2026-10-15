import { RepairModelSettings } from "../../domain/proof.mjs";
import { GitObjectId } from "./git.mjs";
import { CommandId, SandboxTaskId } from "./ids.mjs";
import { SandboxTaskAttachedRepository, SandboxTaskSeedRepository } from "./sandbox-task-admission.mjs";
import { Schema } from "effect";
//#region src/server/sandbox-contracts/sandbox-task-machine.ts
const Uuid = Schema.String.check(Schema.isUUID());
const Hash = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
/** Repair lineage is frozen by its authenticated application, never inferred from model output. */
const SandboxTaskRepairAuthority = Schema.Struct({
	repairId: Uuid,
	attemptId: Uuid,
	repositoryId: Schema.NonEmptyString,
	evidenceHash: Hash,
	proofPolicyHash: Hash
});
const SandboxTaskMachineCreate = Schema.Struct({
	id: SandboxTaskId,
	commandId: CommandId,
	title: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
	prompt: Schema.NonEmptyString.check(Schema.isMaxLength(1e6)),
	modelSettings: RepairModelSettings,
	execution: Schema.optionalKey(Schema.Literals(["agent", "commands"])),
	source: Schema.Union([
		SandboxTaskAttachedRepository,
		SandboxTaskSeedRepository,
		Schema.TaggedStruct("None", {})
	]),
	repair: Schema.optionalKey(SandboxTaskRepairAuthority),
	deadlineAtMillis: Schema.Natural,
	containerSize: Schema.optionalKey(Schema.Literals(["standard", "big"])),
	outputSchema: Schema.Record(Schema.String, Schema.Json),
	requiredCommits: Schema.Array(GitObjectId),
	continuation: Schema.optionalKey(Schema.Struct({
		previousTaskId: SandboxTaskId,
		checkoutHead: GitObjectId
	}))
});
//#endregion
export { SandboxTaskMachineCreate, SandboxTaskRepairAuthority };
