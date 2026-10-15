import { RepairOrigin, RepairRuntimeInspection } from "./proof.mjs";
import { RepairLaunchIntent } from "./launch.mjs";
import { Schema } from "effect";
//#region src/domain/RepairExecution.ts
const RunLaunch = Schema.Struct({
	runId: Schema.String.check(Schema.isUUID()),
	origin: RepairOrigin,
	launch: RepairLaunchIntent
});
const RunState = Schema.Struct({
	...RunLaunch.fields,
	outcome: RepairRuntimeInspection.fields.outcome,
	stopReason: RepairRuntimeInspection.fields.stopReason,
	retirement: RepairRuntimeInspection.fields.retirement,
	retiredAtMs: Schema.NullOr(Schema.Natural)
});
//#endregion
export { RunLaunch, RunState };
