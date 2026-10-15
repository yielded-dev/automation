import { Schema } from "effect";

import { RepairOrigin, RepairRuntimeInspection } from "./proof";

/** Selected execution output used to continue an incident's next investigation. */
export const RepairRunReport = Schema.Struct({
  runId: RepairRuntimeInspection.fields.runId,
  origin: RepairOrigin,
  outcome: RepairRuntimeInspection.fields.outcome,
  stopReason: RepairRuntimeInspection.fields.stopReason,
  message: Schema.NullOr(
    Schema.Struct({
      text: Schema.String.check(Schema.isMaxLength(131_072)),
      phase: Schema.NullOr(Schema.Literals(["final_answer", "commentary"])),
      occurredAt: Schema.Natural,
    }),
  ),
  outputTruncated: Schema.Boolean,
});

export const RunEvent = Schema.Struct({
  sequence: Schema.Natural,
  occurredAt: Schema.Natural,
  body: Schema.String.check(Schema.isMaxLength(131_072)),
  bytes: Schema.Natural.check(Schema.isLessThanOrEqualTo(131_072)),
});
export type RunEvent = typeof RunEvent.Type;
export const RunEvents = Schema.Array(RunEvent).check(Schema.isMaxLength(2000));
export const RunOutputSnapshot = Schema.Struct({ events: RunEvents, bytes: Schema.Natural });
export type RunOutputSnapshot = typeof RunOutputSnapshot.Type;
