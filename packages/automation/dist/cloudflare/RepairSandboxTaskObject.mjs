import { ConflictError, ServiceUnavailableError } from "../domain/errors.mjs";
import { SandboxTaskLaunchReceipt } from "../server/sandbox-contracts/sandbox-task-admission.mjs";
import { SandboxTaskMachineCreate } from "../server/sandbox-contracts/sandbox-task-machine.mjs";
import { SandboxTaskDetail } from "../server/sandbox-contracts/sandbox-task.mjs";
import { SandboxTaskEventList } from "../server/sandbox-contracts/sandbox-task-event.mjs";
import { SandboxTaskExecutionSnapshot, SandboxTaskNativeRequest } from "../server/sandbox-contracts/sandbox-task-native.mjs";
import { Schema } from "effect";
import { DurableObject } from "effect-cf";
import { repositoryPreparationMethods } from "@yielded/sandbox/cloudflare/RepositoryPreparation";
import { WorkspaceCheckpoint } from "@yielded/sandbox/cloudflare/NativeSandbox";
import { RepositorySetupReport } from "@yielded/sandbox/model/RepositorySetup";
//#region src/cloudflare/RepairSandboxTaskObject.ts
const result = (schema) => Schema.Union([
	Schema.TaggedStruct("Success", { value: schema }),
	ConflictError,
	ServiceUnavailableError
]);
const ContinuationCheckpoint = Schema.NullOr(Schema.Struct({
	checkpoint: WorkspaceCheckpoint,
	report: RepositorySetupReport
}));
/** Accessible only through the host's private binding; the public repair API owns caller authorization. */
var RepairSandboxTaskObject = class extends DurableObject.Tag()("RepairSandboxTaskObject", {
	...repositoryPreparationMethods,
	create: DurableObject.method({
		args: [SandboxTaskMachineCreate],
		success: result(SandboxTaskLaunchReceipt)
	}),
	inspect: DurableObject.method({
		args: [Schema.String],
		success: result(Schema.NullOr(SandboxTaskDetail))
	}),
	snapshot: DurableObject.method({
		args: [Schema.String],
		success: result(SandboxTaskExecutionSnapshot)
	}),
	events: DurableObject.method({
		args: [
			Schema.String,
			Schema.Natural,
			Schema.Int.check(Schema.isBetween({
				minimum: 1,
				maximum: 1e3
			}))
		],
		success: result(SandboxTaskEventList)
	}),
	cancel: DurableObject.method({
		args: [
			Schema.String,
			Schema.String,
			Schema.NullOr(SandboxTaskMachineCreate)
		],
		success: result(Schema.Struct({
			id: Schema.String,
			commandId: Schema.String,
			cancelled: Schema.Literal(true)
		}))
	}),
	native: DurableObject.method({
		args: [Schema.String, SandboxTaskNativeRequest],
		success: result(Schema.Json)
	}),
	continuation: DurableObject.method({
		args: [SandboxTaskMachineCreate],
		success: result(ContinuationCheckpoint)
	}),
	forward: DurableObject.method({
		args: [
			Schema.String,
			SandboxTaskNativeRequest.fields.purpose,
			Schema.String,
			DurableObject.native(Schema.declare((input) => input instanceof Request))
		],
		success: DurableObject.native(Schema.instanceOf(Response))
	})
}) {};
const repairSandboxTaskObjectName = (id) => `cloud-task:${id}`;
//#endregion
export { ContinuationCheckpoint, RepairSandboxTaskObject, repairSandboxTaskObjectName };
