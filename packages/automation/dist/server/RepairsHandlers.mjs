import { RepairsApi } from "../domain/RepairsApi.mjs";
import { RunClient } from "./RunClient.mjs";
import { Effect } from "effect";
import { HttpApiBuilder } from "effect/http-api";
//#region src/server/RepairsHandlers.ts
/** Host authentication and engine dependencies are provided at the route family boundary. */
const makeRepairsHandlers = (api = RepairsApi) => HttpApiBuilder.group(api, "runs", Effect.fnUntraced(function* (handlers) {
	const runs = yield* RunClient;
	return handlers.handle("start", ({ payload }) => runs.start(payload)).handle("inspect", ({ params }) => runs.inspect(params.runId)).handle("report", ({ params }) => runs.report(params.runId)).handle("events", ({ params, query }) => runs.events(params.runId, query.after, query.limit)).handle("publish", ({ params, payload }) => runs.publish(params.runId, payload)).handle("retire", ({ payload }) => runs.retire(payload.launch, payload.reason));
}));
const RepairsHandlers = makeRepairsHandlers();
//#endregion
export { RepairsHandlers, makeRepairsHandlers };
