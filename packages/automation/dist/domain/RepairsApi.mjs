import { ConflictError, ServiceUnavailableError } from "./errors.mjs";
import { RepairPublicationReceipt, RepairPublicationRequest, RepairRuntimeInspection } from "./proof.mjs";
import { RunLaunch } from "./RepairExecution.mjs";
import { RepairsAuthentication } from "./RepairsAuthentication.mjs";
import { RepairRunReport, RunEvents } from "./RunOutput.mjs";
import { Schema } from "effect";
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiSchema } from "effect/http-api";
//#region src/domain/RepairsApi.ts
const RunId = RepairRuntimeInspection.fields.runId;
const errors = [ConflictError.pipe(HttpApiSchema.status(409)), ServiceUnavailableError.pipe(HttpApiSchema.status(503))];
const RetireRun = Schema.Struct({
	launch: RunLaunch,
	reason: RepairRuntimeInspection.fields.stopReason
});
const RepairsRunsApi = HttpApiGroup.make("runs").add(HttpApiEndpoint.post("start", "/runs/start", {
	payload: RunLaunch,
	success: Schema.Struct({ accepted: Schema.Boolean }),
	error: errors
}), HttpApiEndpoint.get("inspect", "/runs/:runId", {
	params: { runId: RunId },
	success: RepairRuntimeInspection,
	error: errors
}), HttpApiEndpoint.get("report", "/runs/:runId/report", {
	params: { runId: RunId },
	success: RepairRunReport,
	error: errors
}), HttpApiEndpoint.get("events", "/runs/:runId/events", {
	params: { runId: RunId },
	query: {
		after: Schema.optionalKey(Schema.NumberFromString.pipe(Schema.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0)))),
		limit: Schema.optionalKey(Schema.NumberFromString.pipe(Schema.check(Schema.isInt(), Schema.isBetween({
			minimum: 1,
			maximum: 1e3
		}))))
	},
	success: RunEvents,
	error: errors
}), HttpApiEndpoint.post("publish", "/runs/:runId/publish", {
	params: { runId: RunId },
	payload: RepairPublicationRequest,
	success: RepairPublicationReceipt,
	error: errors
}), HttpApiEndpoint.post("retire", "/runs/retire", {
	payload: RetireRun,
	success: RepairRuntimeInspection,
	error: errors
})).middleware(RepairsAuthentication);
const RepairsApi = HttpApi.make("repairs").add(RepairsRunsApi).prefix("/repairs");
//#endregion
export { RepairsApi, RepairsRunsApi, RetireRun };
