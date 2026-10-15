import { RepairOrigin, RepairRuntimeInspection } from "./proof.mjs";
import { Schema } from "effect";
//#region src/domain/RunOutput.ts
/** Selected execution output used to continue an incident's next investigation. */
const RepairRunReport = Schema.Struct({
	runId: RepairRuntimeInspection.fields.runId,
	origin: RepairOrigin,
	outcome: RepairRuntimeInspection.fields.outcome,
	stopReason: RepairRuntimeInspection.fields.stopReason,
	message: Schema.NullOr(Schema.Struct({
		text: Schema.String.check(Schema.isMaxLength(131072)),
		phase: Schema.NullOr(Schema.Literals(["final_answer", "commentary"])),
		occurredAt: Schema.Natural
	})),
	outputTruncated: Schema.Boolean
});
const RunEvent = Schema.Struct({
	sequence: Schema.Natural,
	occurredAt: Schema.Natural,
	body: Schema.String.check(Schema.isMaxLength(131072)),
	bytes: Schema.Natural.check(Schema.isLessThanOrEqualTo(131072))
});
const RunEvents = Schema.Array(RunEvent).check(Schema.isMaxLength(2e3));
const RunOutputSnapshot = Schema.Struct({
	events: RunEvents,
	bytes: Schema.Natural
});
//#endregion
export { RepairRunReport, RunEvent, RunEvents, RunOutputSnapshot };
