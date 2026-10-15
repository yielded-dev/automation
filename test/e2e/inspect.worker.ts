import { Cause, Effect, Layer, Schema } from "effect";
import { WorkerEnvironment } from "effect-cf";
import { RepairSandboxTaskClientLive } from "../../packages/automation/src/cloudflare/RepairSandboxTaskClientLive";
import { RepairSandboxTaskClient } from "@yielded/automation/server/RepairSandboxTaskClient";

const Input = Schema.Struct({ id: Schema.String });

export default {
  async fetch(request: Request, env: { SERVER: Fetcher; COMPLETION: Fetcher }) {
    if (new URL(request.url).pathname.startsWith("/completion/"))
      return env.COMPLETION.fetch(request);
    if (new URL(request.url).pathname === "/seed") return env.SERVER.fetch(request);
    const input: unknown = await request.json();
    const exit = await Effect.runPromiseExit(
      Effect.gen(function* () {
        const { id } = yield* Schema.decodeUnknownEffect(Input)(input);
        return yield* (yield* RepairSandboxTaskClient).inspect(id);
      }).pipe(
        Effect.provide(
          RepairSandboxTaskClientLive({ binding: "RUNS", locationHint: "apac-se" }).pipe(
            Layer.provide(Layer.succeed(WorkerEnvironment, env)),
          ),
        ),
      ),
    );
    return Response.json(
      exit._tag === "Success"
        ? { exit: exit._tag, value: exit.value }
        : { exit: exit._tag, failure: Cause.pretty(exit.cause) },
    );
  },
};
