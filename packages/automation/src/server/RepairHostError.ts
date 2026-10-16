import { Cause, Effect, Schema } from "effect";

/** Original host failures stay attached until the public transport boundary reports them. */
export class RepairHostError extends Schema.TaggedError<RepairHostError>()("RepairHostError", {
  operation: Schema.String,
  cause: Schema.Defect(),
}) {}

export const mapRepairHostError =
  (operation: string) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.catchCause((cause) =>
        Effect.failCause(
          Cause.map(cause, (error) => RepairHostError.make({ operation, cause: error })),
        ),
      ),
    );
