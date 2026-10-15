import { Context, Effect } from "effect";
import type { RepairModelSettings, RepairModelUsage } from "../domain/proof";
import type { RunState } from "../domain/RepairExecution";
import type { ConflictError } from "../domain/errors";
import type { RepairHostError } from "./RepairHostError";

/** The host owns supported model capabilities and provider accounting. */
export class RepairModel extends Context.Service<
  RepairModel,
  {
    readonly prepare: (
      settings: typeof RepairModelSettings.Type,
    ) => Effect.Effect<typeof RepairModelSettings.Type, RepairHostError | ConflictError>;
    readonly usage: (
      state: RunState,
    ) => Effect.Effect<RepairModelUsage | undefined, RepairHostError>;
  }
>()("@reve/repairs/RepairModel") {}
