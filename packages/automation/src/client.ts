import { Context, Effect, Layer } from "effect";
import type { HttpClient } from "effect/http";
import { HttpApiClient, type HttpApiMiddleware } from "effect/http-api";
import { Atom, AtomHttpApi } from "effect/reactivity";

import { RepairsApi, type RetireRun } from "./domain/RepairsApi";
import type { RunLaunch } from "./domain/RepairExecution";
import type { RepairPublicationRequest } from "./domain/proof";
import type { RepairsAuthentication } from "./domain/RepairsAuthentication";

/** Provide the host HTTP client and RepairsAuthentication client middleware to this layer. */
export class RepairsClient extends Context.Service<
  RepairsClient,
  HttpApiClient.ForApi<typeof RepairsApi>
>()("@reve/repairs/RepairsClient") {
  static readonly layer = (options?: Parameters<typeof HttpApiClient.make>[1]) =>
    Layer.effect(RepairsClient, HttpApiClient.make(RepairsApi, options));
}

/** Construct once at the host composition root, using its existing Reactivity runtime. */
export const makeRepairsAtoms = (options: {
  readonly runtime: Atom.RuntimeFactory;
  readonly httpClient: Layer.Layer<
    HttpClient.HttpClient | HttpApiMiddleware.ForClient<RepairsAuthentication>
  >;
  readonly baseUrl?: string | URL;
}) => {
  class Client extends AtomHttpApi.Service<Client>()("@reve/repairs/RepairsAtoms", {
    api: RepairsApi,
    ...options,
  }) {}

  const runKeys = (runId: string) => [`repairs-run:${runId}`];
  const inspect = Atom.family((runId: string) =>
    Client.query("runs", "inspect", {
      params: { runId },
      reactivityKeys: runKeys(runId),
    }),
  );
  const report = Atom.family((runId: string) =>
    Client.query("runs", "report", {
      params: { runId },
      reactivityKeys: runKeys(runId),
    }),
  );
  const eventPages = Atom.family((key: string) => {
    const [runId = "", after = "0", limit = "1000"] = key.split(":");
    return Client.query("runs", "events", {
      params: { runId },
      query: { after: Number(after), limit: Number(limit) },
      reactivityKeys: runKeys(runId),
    });
  });
  const events = (runId: string, after = 0, limit = 1000) =>
    eventPages(`${runId}:${after}:${limit}`);
  const startMutation = Client.mutation("runs", "start");
  const publishMutation = Client.mutation("runs", "publish");
  const retireMutation = Client.mutation("runs", "retire");

  const start = Atom.fn<RunLaunch>()(
    Effect.fnUntraced(function* (launch, get) {
      return yield* get.setResult(startMutation, {
        payload: launch,
        reactivityKeys: runKeys(launch.runId),
      });
    }),
  );
  const publish = Atom.family((runId: string) =>
    Atom.fn<RepairPublicationRequest>()(
      Effect.fnUntraced(function* (request, get) {
        return yield* get.setResult(publishMutation, {
          params: { runId },
          payload: request,
          reactivityKeys: runKeys(runId),
        });
      }),
    ),
  );
  const retire = Atom.fn<typeof RetireRun.Type>()(
    Effect.fnUntraced(function* (request, get) {
      return yield* get.setResult(retireMutation, {
        payload: request,
        reactivityKeys: runKeys(request.launch.runId),
      });
    }),
  );

  return { inspect, report, events, start, publish, retire };
};
