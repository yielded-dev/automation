import { RepairsApi } from "./domain/RepairsApi.mjs";
import { Context, Effect, Layer } from "effect";
import { HttpApiClient } from "effect/http-api";
import { Atom, AtomHttpApi } from "effect/reactivity";
//#region src/client.ts
/** Provide the host HTTP client and RepairsAuthentication client middleware to this layer. */
var RepairsClient = class RepairsClient extends Context.Service()("@reve/repairs/RepairsClient") {
	static layer = (options) => Layer.effect(RepairsClient, HttpApiClient.make(RepairsApi, options));
};
/** Construct once at the host composition root, using its existing Reactivity runtime. */
const makeRepairsAtoms = (options) => {
	class Client extends AtomHttpApi.Service()("@reve/repairs/RepairsAtoms", {
		api: RepairsApi,
		...options
	}) {}
	const runKeys = (runId) => [`repairs-run:${runId}`];
	const inspect = Atom.family((runId) => Client.query("runs", "inspect", {
		params: { runId },
		reactivityKeys: runKeys(runId)
	}));
	const report = Atom.family((runId) => Client.query("runs", "report", {
		params: { runId },
		reactivityKeys: runKeys(runId)
	}));
	const eventPages = Atom.family((key) => {
		const [runId = "", after = "0", limit = "1000"] = key.split(":");
		return Client.query("runs", "events", {
			params: { runId },
			query: {
				after: Number(after),
				limit: Number(limit)
			},
			reactivityKeys: runKeys(runId)
		});
	});
	const events = (runId, after = 0, limit = 1e3) => eventPages(`${runId}:${after}:${limit}`);
	const startMutation = Client.mutation("runs", "start");
	const publishMutation = Client.mutation("runs", "publish");
	const retireMutation = Client.mutation("runs", "retire");
	return {
		inspect,
		report,
		events,
		start: Atom.fn()(Effect.fnUntraced(function* (launch, get) {
			return yield* get.setResult(startMutation, {
				payload: launch,
				reactivityKeys: runKeys(launch.runId)
			});
		})),
		publish: Atom.family((runId) => Atom.fn()(Effect.fnUntraced(function* (request, get) {
			return yield* get.setResult(publishMutation, {
				params: { runId },
				payload: request,
				reactivityKeys: runKeys(runId)
			});
		}))),
		retire: Atom.fn()(Effect.fnUntraced(function* (request, get) {
			return yield* get.setResult(retireMutation, {
				payload: request,
				reactivityKeys: runKeys(request.launch.runId)
			});
		}))
	};
};
//#endregion
export { RepairsClient, makeRepairsAtoms };
