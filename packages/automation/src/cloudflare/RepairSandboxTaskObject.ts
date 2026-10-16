import { Schema } from "effect";
import { DurableObject } from "effect-cf";
import { ConflictError, ServiceUnavailableError } from "../domain/errors";
import { SandboxTaskMachineCreate } from "../server/sandbox-contracts/sandbox-task-machine";
import { SandboxTaskLaunchReceipt } from "../server/sandbox-contracts/sandbox-task-admission";
import { SandboxTaskDetail } from "../server/sandbox-contracts/sandbox-task";
import { SandboxTaskEventList } from "../server/sandbox-contracts/sandbox-task-event";
import {
  SandboxTaskNativeRequest,
  SandboxTaskExecutionSnapshot,
} from "../server/sandbox-contracts/sandbox-task-native";
import { WorkspaceCheckpoint } from "@yielded/sandbox/cloudflare/NativeSandbox";
import { RepositorySetupReport } from "@yielded/sandbox/model/RepositorySetup";
import { repositoryPreparationMethods } from "@yielded/sandbox/cloudflare/RepositoryPreparation";

const result = <S extends Schema.Top>(schema: S) =>
  Schema.Union([
    Schema.TaggedStruct("Success", { value: schema }),
    ConflictError,
    ServiceUnavailableError,
  ]);
export const ContinuationCheckpoint = Schema.NullOr(
  Schema.Struct({ checkpoint: WorkspaceCheckpoint, report: RepositorySetupReport }),
);

/** Accessible only through the host's private binding; the public repair API owns caller authorization. */
export class RepairSandboxTaskObject extends DurableObject.Tag<RepairSandboxTaskObject>()(
  "RepairSandboxTaskObject",
  {
    ...repositoryPreparationMethods,
    create: DurableObject.method({
      args: [SandboxTaskMachineCreate],
      success: result(SandboxTaskLaunchReceipt),
    }),
    inspect: DurableObject.method({
      args: [Schema.String],
      success: result(Schema.NullOr(SandboxTaskDetail)),
    }),
    snapshot: DurableObject.method({
      args: [Schema.String],
      success: result(SandboxTaskExecutionSnapshot),
    }),
    events: DurableObject.method({
      args: [
        Schema.String,
        Schema.Natural,
        Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1000 })),
      ],
      success: result(SandboxTaskEventList),
    }),
    cancel: DurableObject.method({
      args: [Schema.String, Schema.String, Schema.NullOr(SandboxTaskMachineCreate)],
      success: result(
        Schema.Struct({
          id: Schema.String,
          commandId: Schema.String,
          cancelled: Schema.Literal(true),
        }),
      ),
    }),
    native: DurableObject.method({
      args: [Schema.String, SandboxTaskNativeRequest],
      success: result(Schema.Json),
    }),
    continuation: DurableObject.method({
      args: [SandboxTaskMachineCreate],
      success: result(ContinuationCheckpoint),
    }),
    forward: DurableObject.method({
      args: [
        Schema.String,
        SandboxTaskNativeRequest.fields.purpose,
        Schema.String,
        DurableObject.native(Schema.declare<Request>((input) => input instanceof Request)),
      ],
      success: DurableObject.native(Schema.instanceOf(Response)),
    }),
  },
) {}

export const repairSandboxTaskObjectName = (id: string) => `cloud-task:${id}`;
