import { Context, Effect, Schema } from "effect";
import type { SandboxTaskDetail } from "./sandbox-contracts/sandbox-task";
import type { SandboxTaskLaunchReceipt } from "./sandbox-contracts/sandbox-task-admission";
import type { SandboxTaskEvent } from "./sandbox-contracts/sandbox-task-event";
import type { SandboxTaskMachineCreate } from "./sandbox-contracts/sandbox-task-machine";
import type {
  SandboxTaskExecutionSnapshot,
  SandboxTaskNativeRequest,
} from "./sandbox-contracts/sandbox-task-native";

export class RepairSandboxTaskTransportError extends Schema.TaggedError<RepairSandboxTaskTransportError>()(
  "RepairSandboxTaskTransportError",
  {
    operation: Schema.String,
    status: Schema.optionalKey(Schema.Int),
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/** The cancellation fence is durable, but native evidence or leases may still own compute.
 * Only callers requesting allowDraining may receive this pending result.
 */
export type RepairSandboxTaskCancellation =
  | { readonly id: string; readonly commandId: string; readonly cancelled: true }
  | {
      readonly _tag: "SandboxTaskCancellationDraining";
      readonly id: string;
      readonly commandId: string;
    };

/** The host connects its existing sandbox runtime, locally or over authenticated transport. */
export class RepairSandboxTaskClient extends Context.Service<
  RepairSandboxTaskClient,
  {
    readonly create: (
      input: SandboxTaskMachineCreate,
    ) => Effect.Effect<SandboxTaskLaunchReceipt, RepairSandboxTaskTransportError>;
    readonly inspect: (
      id: string,
    ) => Effect.Effect<SandboxTaskDetail | null, RepairSandboxTaskTransportError>;
    readonly snapshot: (
      id: string,
    ) => Effect.Effect<SandboxTaskExecutionSnapshot, RepairSandboxTaskTransportError>;
    readonly events: (
      id: string,
      after: number,
      limit?: number,
    ) => Effect.Effect<ReadonlyArray<SandboxTaskEvent>, RepairSandboxTaskTransportError>;
    readonly cancel: (
      id: string,
      commandId: string,
      admission?: SandboxTaskMachineCreate,
      options?: { readonly allowDraining: true },
    ) => Effect.Effect<RepairSandboxTaskCancellation, RepairSandboxTaskTransportError>;
    readonly native: <A, I>(
      id: string,
      request: SandboxTaskNativeRequest,
      schema: Schema.Codec<A, I>,
    ) => Effect.Effect<A, RepairSandboxTaskTransportError>;
    readonly forward: (
      id: string,
      purpose: SandboxTaskNativeRequest["purpose"],
      operationId: string,
      nativeToken: string,
      original: Request,
    ) => Effect.Effect<Response, RepairSandboxTaskTransportError>;
  }
>()("kommunikasie/RepairSandboxTaskClient") {}
