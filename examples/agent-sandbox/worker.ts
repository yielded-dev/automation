import { webCryptoLayer } from "@yielded/sandbox/cloudflare/Bindings";
import { Clock, Context, Effect, Layer, Redacted, Schema, Semaphore } from "effect";
import { Artifacts, DurableObject, DurableObjectState, WorkerEnvironment } from "effect-cf";
import {
  AgentEventSink,
  CodexHarnessLive,
  NativeSandboxNamespace,
  OutboundAuthority,
  RepositoryInstallAuthorization,
  SandboxAgentArtifacts,
  SandboxAgentConfig,
  Sandboxes,
  SessionStateStore,
  WorkspaceBaselineRepository,
  makeNativeSandbox,
  makeSandboxAgentContainer,
  makeSandboxAgentOutbound,
  resourceHash,
  runTurn,
  sandboxAgentTurnLayer,
  sandboxNameFor,
} from "@yielded/sandbox/cloudflare";
import { fixtureArtifacts } from "../../test/fixtures/artifacts.worker";
import { CodingError, CodingRequest, CodingResult } from "./protocol.ts";

interface Environment {
  OPENAI_API_KEY: string;
  SOURCE: string;
  SESSION_ID: string;
  MODEL: string;
  ARTIFACTS: Fetcher;
}
const container = {
  namespaceBinding: "SANDBOX",
  allowedHosts: () => ["fixture.invalid"],
  openAiApiKey: (env: Environment) => Redacted.make(env.OPENAI_API_KEY),
};
export const Sandbox = makeSandboxAgentContainer(container);
export const SandboxOutbound = makeSandboxAgentOutbound(container);

const Source = Schema.Struct({
  repositoryName: Schema.String,
  repositoryId: Schema.String,
  commitSha: Schema.String,
});
const Accepted = Schema.Struct({
  instructions: CodingRequest.fields.instructions,
  source: Source,
  startedAtMillis: Schema.Finite,
  deadlineAtMs: Schema.Finite,
});
class Gate extends Context.Service<Gate, Semaphore.Semaphore>()("example/CodingGate") {}
const host = Layer.mergeAll(
  Layer.effect(Gate, Semaphore.make(1)),
  Layer.effect(
    SandboxAgentArtifacts,
    Effect.gen(function* () {
      const env = (yield* WorkerEnvironment) as Environment;
      return Artifacts.makeClient({ binding: "ARTIFACTS" })(fixtureArtifacts(env.ARTIFACTS));
    }),
  ),
  Layer.effect(
    SandboxAgentConfig,
    Effect.gen(function* () {
      const env = (yield* WorkerEnvironment) as Environment;
      return yield* SandboxAgentConfig.make({
        repositorySetupPath: ".fixture-setup.json",
        defaultModel: env.MODEL,
        artifactsNamespace: "fixture",
        artifactsHostname: "fixture.invalid",
      });
    }),
  ),
  NativeSandboxNamespace.layer({ binding: "SANDBOX" }),
  Sandboxes.layer({ binding: "SANDBOX" }),
  RepositoryInstallAuthorization.emptyLayer,
  WorkspaceBaselineRepository.noopLayer,
  OutboundAuthority.noneLayer,
  CodexHarnessLive,
  Layer.succeed(AgentEventSink, {
    emit: (event) => Effect.sync(() => console.log("CODING_EVENT " + JSON.stringify(event))),
  }),
);
const services = sandboxAgentTurnLayer.pipe(
  Layer.provideMerge(host),
  Layer.provideMerge(webCryptoLayer),
);

const code = Effect.fn("CodingSession.code")(function* (input: unknown) {
  const request = yield* Schema.decodeUnknownEffect(CodingRequest)(input);
  const state = yield* DurableObjectState.DurableObjectState;
  const env = (yield* WorkerEnvironment) as Environment;
  const previous = yield* state.storage.kv.get("accepted");
  const startedAtMillis = yield* Clock.currentTimeMillis;
  const accepted =
    previous === undefined
      ? {
          ...request,
          source: yield* Schema.decodeEffect(Schema.fromJsonString(Source))(env.SOURCE),
          startedAtMillis,
          deadlineAtMs: startedAtMillis + 240_000,
        }
      : yield* Schema.decodeUnknownEffect(Accepted)(previous);
  if (accepted.instructions !== request.instructions)
    return yield* CodingError.make({
      message: "This task already has different accepted instructions",
    });
  if (previous === undefined) yield* state.storage.kv.put("accepted", accepted);
  yield* runTurn({
    sessionId: env.SESSION_ID,
    turnId: env.SESSION_ID + ":coding",
    text: accepted.instructions,
    startedAtMillis: accepted.startedAtMillis,
    deadlineAtMs: accepted.deadlineAtMs,
    recover: previous !== undefined,
    restrictModelRequests: true,
    reasoningEffort: "low",
    repository: {
      _tag: "SeedRepository",
      repositoryName: accepted.source.repositoryName,
      artifactsRepositoryId: accepted.source.repositoryId,
      commitSha: accepted.source.commitSha,
    },
    initialCheckout: accepted.source.commitSha,
  });
  const store = yield* SessionStateStore;
  const saved = yield* store.pipelineTurn.get;
  return yield* Schema.decodeUnknownEffect(CodingResult)({
    head: saved?.persisted?.head,
    forkName: yield* store.forkName.get,
    changedPaths: saved?.persisted?.changedPaths,
  });
});
const stop = Effect.gen(function* () {
  const env = (yield* WorkerEnvironment) as Environment;
  const sandbox = yield* makeNativeSandbox(sandboxNameFor(yield* resourceHash(env.SESSION_ID)));
  const snapshot = (yield* sandbox.describe).checkpoint?.snapshotId ?? null;
  yield* sandbox.destroy;
  return { snapshot };
});
export const CodingSession = DurableObject.make(services, {
  rpc: {
    code: (input: unknown) => Effect.flatMap(Gate, (gate) => code(input).pipe(gate.withPermits(1))),
    stop: () => stop,
  },
});
export default {
  async fetch(
    request: Request,
    env: Environment & {
      CODING: DurableObjectNamespace<InstanceType<typeof CodingSession>>;
    },
  ) {
    const session = env.CODING.getByName(env.SESSION_ID);
    if (new URL(request.url).pathname === "/stop") return Response.json(await session.stop());
    return Response.json(await session.code(await request.json()));
  },
};
