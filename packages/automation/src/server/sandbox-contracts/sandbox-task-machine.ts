import { Schema } from "effect";

import { RepairModelSettings as TaskModelSettings } from "../../domain/proof";
import { GitObjectId } from "./git";
import { CommandId, SandboxTaskId } from "./ids";
import { SandboxTaskAttachedRepository, SandboxTaskSeedRepository } from "./sandbox-task-admission";

const Uuid = Schema.String.check(Schema.isUUID());
const Hash = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));

/** Repair lineage is frozen by its authenticated application, never inferred from model output. */
export const SandboxTaskRepairAuthority = Schema.Struct({
  repairId: Uuid,
  attemptId: Uuid,
  repositoryId: Schema.NonEmptyString,
  evidenceHash: Hash,
  proofPolicyHash: Hash,
});
export const SandboxTaskMachineCreate = Schema.Struct({
  id: SandboxTaskId,
  commandId: CommandId,
  title: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  prompt: Schema.NonEmptyString.check(Schema.isMaxLength(1_000_000)),
  modelSettings: TaskModelSettings,
  execution: Schema.optionalKey(Schema.Literals(["agent", "commands"])),
  source: Schema.Union([
    SandboxTaskAttachedRepository,
    SandboxTaskSeedRepository,
    Schema.TaggedStruct("None", {}),
  ]),
  repair: Schema.optionalKey(SandboxTaskRepairAuthority),
  deadlineAtMillis: Schema.Natural,
  containerSize: Schema.optionalKey(Schema.Literals(["standard", "big"])),
  outputSchema: Schema.Record(Schema.String, Schema.Json),
  requiredCommits: Schema.Array(GitObjectId),
  continuation: Schema.optionalKey(
    Schema.Struct({ previousTaskId: SandboxTaskId, checkoutHead: GitObjectId }),
  ),
});
export type SandboxTaskMachineCreate = typeof SandboxTaskMachineCreate.Type;
