import { Context, Effect, Schema } from "effect";

/** A wake-up for an accepted repair run, not authority to publish it. */
export const RepairSandboxTaskSettled = Schema.Struct({
  /** The native task identity is the accepted Repairs runId. */
  taskId: Schema.String.check(Schema.isUUID()),
});
export type RepairSandboxTaskSettled = typeof RepairSandboxTaskSettled.Type;

export class RepairSandboxTaskNotificationError extends Schema.TaggedError<RepairSandboxTaskNotificationError>()(
  "RepairSandboxTaskNotificationError",
  { cause: Schema.Defect() },
) {}

/** Hosts durably accept this identity, then re-read their authority through RunClient. */
export class RepairSandboxTaskNotifications extends Context.Service<
  RepairSandboxTaskNotifications,
  {
    /** At-least-once delivery after terminal commit; accept duplicates of the same taskId. */
    readonly settled: (
      event: RepairSandboxTaskSettled,
    ) => Effect.Effect<void, RepairSandboxTaskNotificationError>;
  }
>()("@reve/repairs/RepairSandboxTaskNotifications") {}
