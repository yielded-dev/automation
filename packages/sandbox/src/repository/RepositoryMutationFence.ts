import { Context, Effect, Schema } from "effect";

export class RepositoryMutationFenceFailure extends Schema.TaggedError<RepositoryMutationFenceFailure>()(
  "RepositoryMutationFenceFailure",
  {
    reason: Schema.Literals(["stopped", "unavailable"]),
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/** The operation owner rechecks its durable authority immediately before a remote mutation. */
export class RepositoryMutationFence extends Context.Service<
  RepositoryMutationFence,
  { readonly check: Effect.Effect<void, RepositoryMutationFenceFailure> }
>()("kommunikasie/RepositoryMutationFence") {}
