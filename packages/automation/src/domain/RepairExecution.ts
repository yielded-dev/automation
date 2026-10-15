import { Schema } from "effect";

import { RepairOrigin, RepairRuntimeInspection } from "./proof";
import { RepairLaunchIntent } from "./launch";

export const RunLaunch = Schema.Struct({
  runId: Schema.String.check(Schema.isUUID()),
  origin: RepairOrigin,
  launch: RepairLaunchIntent,
});
export type RunLaunch = typeof RunLaunch.Type;
export const RunState = Schema.Struct({
  ...RunLaunch.fields,
  outcome: RepairRuntimeInspection.fields.outcome,
  stopReason: RepairRuntimeInspection.fields.stopReason,
  retirement: RepairRuntimeInspection.fields.retirement,
  retiredAtMs: Schema.NullOr(Schema.Natural),
});
export type RunState = typeof RunState.Type;
