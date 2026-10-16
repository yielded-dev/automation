import { Schema } from "effect";
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiSchema } from "effect/http-api";

import { ConflictError, ServiceUnavailableError } from "./errors";
import {
  RepairPublicationReceipt,
  RepairPublicationRequest,
  RepairRuntimeInspection,
} from "./proof";
import { RunLaunch } from "./RepairExecution";
import { RepairsAuthentication } from "./RepairsAuthentication";
import { RepairRunReport, RunEvents } from "./RunOutput";

const RunId = RepairRuntimeInspection.fields.runId;
const errors = [
  ConflictError.pipe(HttpApiSchema.status(409)),
  ServiceUnavailableError.pipe(HttpApiSchema.status(503)),
] as const;

export const RetireRun = Schema.Struct({
  launch: RunLaunch,
  reason: RepairRuntimeInspection.fields.stopReason,
});

export const RepairsRunsApi = HttpApiGroup.make("runs")
  .add(
    HttpApiEndpoint.post("start", "/runs/start", {
      payload: RunLaunch,
      success: Schema.Struct({ accepted: Schema.Boolean }),
      error: errors,
    }),
    HttpApiEndpoint.get("inspect", "/runs/:runId", {
      params: { runId: RunId },
      success: RepairRuntimeInspection,
      error: errors,
    }),
    HttpApiEndpoint.get("report", "/runs/:runId/report", {
      params: { runId: RunId },
      success: RepairRunReport,
      error: errors,
    }),
    HttpApiEndpoint.get("events", "/runs/:runId/events", {
      params: { runId: RunId },
      query: {
        after: Schema.optionalKey(
          Schema.NumberFromString.pipe(
            Schema.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0)),
          ),
        ),
        limit: Schema.optionalKey(
          Schema.NumberFromString.pipe(
            Schema.check(Schema.isInt(), Schema.isBetween({ minimum: 1, maximum: 1000 })),
          ),
        ),
      },
      success: RunEvents,
      error: errors,
    }),
    HttpApiEndpoint.post("publish", "/runs/:runId/publish", {
      params: { runId: RunId },
      payload: RepairPublicationRequest,
      success: RepairPublicationReceipt,
      error: errors,
    }),
    HttpApiEndpoint.post("retire", "/runs/retire", {
      payload: RetireRun,
      success: RepairRuntimeInspection,
      error: errors,
    }),
  )
  .middleware(RepairsAuthentication);

export const RepairsApi = HttpApi.make("repairs").add(RepairsRunsApi).prefix("/repairs");
