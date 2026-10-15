import { Context, Effect, Layer, Ref, Semaphore, type Fiber } from "effect";
import type { ConflictError } from "../domain/errors";
import type { RepairSandboxTaskError } from "./RepairSandboxTaskError";

export class RepairSandboxTaskControl extends Context.Service<
  RepairSandboxTaskControl,
  {
    readonly lifecycle: Semaphore.Semaphore;
    readonly active: Ref.Ref<Fiber.Fiber<void, RepairSandboxTaskError | ConflictError> | undefined>;
  }
>()("@reve/repairs/RepairSandboxTaskControl") {
  static readonly layer = Layer.effect(
    this,
    Effect.gen(function* () {
      return {
        lifecycle: yield* Semaphore.make(1),
        active: yield* Ref.make<
          Fiber.Fiber<void, RepairSandboxTaskError | ConflictError> | undefined
        >(undefined),
      };
    }),
  );
}
