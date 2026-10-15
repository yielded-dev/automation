import { Effect, Layer, Schema, Tracer } from "effect";
import { DurableObject, DurableObjectSqlite } from "effect-cf";
import { fixtureArtifacts } from "../../../../../test/fixtures/artifacts.worker";
import { makeRepairSandboxTaskDurableObject } from "../../../src/cloudflare/RepairSandboxTaskDurableObject";
import {
  RepairSandboxState,
  RepairSandboxTaskStore,
} from "../../../src/cloudflare/RepairSandboxTaskStore";
import { repairSandboxTaskObjectName } from "../../../src/cloudflare/RepairSandboxTaskObject";
import { SandboxAgentConfig } from "@yielded/sandbox/cloudflare/Config";

// With an explicit legacy repository, the runner resolves these imports to the pinned release.
const NativeRun = makeRepairSandboxTaskDurableObject({
  bindings: { artifacts: "ARTIFACTS", sandbox: "RUNS" },
  coordinatorBinding: "RUNS",
  configLayer: SandboxAgentConfig.layer({
    repositorySetupPath: ".fixture-setup.json",
    defaultModel: "unused",
    artifactsNamespace: "fixture",
    artifactsHostname: "fixture.invalid",
  }),
  diagnosticsLayer: Layer.succeed(
    Tracer.Tracer,
    Tracer.make({
      ...Tracer.nativeTracer,
      span(options) {
        const span = Tracer.nativeTracer.span(options);
        const end = span.end.bind(span);
        span.end = (time, exit) => {
          end(time, exit);
          if (options.name === "RepairSandboxTask.inspect")
            console.log(
              "INSPECT_SERVER " + JSON.stringify({ name: options.name, exit: exit._tag }),
            );
        };
        return span;
      },
    }),
  ),
  turnPolicy: { restrictModelRequests: true, sentryMcp: false, startRepositoryPreview: false },
});

interface Env {
  RUNS: DurableObjectNamespace<RepairRun>;
  FIXTURE_ARTIFACTS: Fetcher;
}

export class RepairRun extends NativeRun {
  constructor(state: DurableObjectState, env: Env) {
    super(state, { ...env, ARTIFACTS: fixtureArtifacts(env.FIXTURE_ARTIFACTS) });
  }

  seed(input: unknown) {
    return this[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const saved = yield* Schema.decodeUnknownEffect(RepairSandboxState)(input);
        const records = yield* RepairSandboxTaskStore;
        yield* records.accept(saved.request, saved.fingerprint, saved.createdAtMillis);
        yield* records.update(() => saved);
        const retained = yield* records.required;
        return {
          id: retained.request.id,
          status: retained.status,
          receipt: retained.launchReceipt !== null,
        };
      }).pipe(
        Effect.provide(
          RepairSandboxTaskStore.layer.pipe(Layer.provide(DurableObjectSqlite.layer())),
        ),
      ),
    );
  }
}

// Observe the generated RPC after encoding without changing its result or trace envelope.
Object.defineProperty(RepairRun.prototype, "inspect", {
  value: async function (this: RepairRun, ...args: Parameters<RepairRun["inspect"]>) {
    const result = await NativeRun.prototype.inspect.apply(this, args);
    console.log("INSPECT_REPLY " + JSON.stringify(result));
    return result;
  },
});

export default {
  async fetch(request: Request, env: Env) {
    const input: unknown = await request.json();
    const saved = Schema.decodeUnknownSync(RepairSandboxState)(input);
    return Response.json(
      await env.RUNS.getByName(repairSandboxTaskObjectName(saved.request.id)).seed(input),
    );
  },
};
