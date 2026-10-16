import { Context, Effect } from "effect";
import type { RunState } from "../domain/RepairExecution";
import type { RepairHostError } from "./RepairHostError";

/** Product consent and repository policy remain authoritative in the host. */
export class RepairAuthorization extends Context.Service<
  RepairAuthorization,
  {
    readonly authorize: (
      state: RunState,
      publication: boolean,
    ) => Effect.Effect<boolean, RepairHostError>;
  }
>()("@reve/repairs/RepairAuthorization") {}
