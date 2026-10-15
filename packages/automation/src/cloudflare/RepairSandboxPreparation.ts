import { Context, Effect, Layer, Schema } from "effect";
import type { SandboxTaskMachineCreate } from "../server/sandbox-contracts/sandbox-task-machine";

export class RepairSandboxPreparationError extends Schema.TaggedError<RepairSandboxPreparationError>()(
  "RepairSandboxPreparationError",
  { cause: Schema.Defect() },
) {}

/** Join host-owned preparation before the first workspace initialization, never during receipt replay. */
export class RepairSandboxPreparation extends Context.Service<
  RepairSandboxPreparation,
  {
    readonly prepare: (
      request: SandboxTaskMachineCreate,
    ) => Effect.Effect<void, RepairSandboxPreparationError>;
  }
>()("@reve/repairs/RepairSandboxPreparation") {
  static readonly noopLayer = Layer.succeed(this, { prepare: () => Effect.void });
}
