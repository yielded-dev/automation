import { Cause, Effect, Schema } from "effect";
import { ConflictError } from "../domain/errors";

export class RepairSandboxTaskError extends Schema.TaggedError<RepairSandboxTaskError>()(
  "RepairSandboxTaskError",
  {
    cause: Schema.Defect(),
  },
) {}

export const preserveRepairSandboxFailure = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.catchCause((cause) =>
      Effect.failCause(
        Cause.map(cause, (error) =>
          Schema.is(ConflictError)(error) || Schema.is(RepairSandboxTaskError)(error)
            ? error
            : RepairSandboxTaskError.make({ cause: error }),
        ),
      ),
    ),
  );
