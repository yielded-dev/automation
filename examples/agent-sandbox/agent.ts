import { Agent, AgentRuntime } from "@yielded/agent";
import { Context, Effect, Schema } from "effect";
import { Tool, Toolkit } from "effect/ai";
import { CodingError, CodingRequest, CodingResult } from "./protocol.ts";

export { CodingError, CodingRequest, CodingResult };

export class CodingTask extends Context.Service<
  CodingTask,
  {
    run(instructions: string): Effect.Effect<typeof CodingResult.Type, CodingError>;
  }
>()("example/CodingTask") {}

const tools = Toolkit.make(
  Tool.make("start_coding", {
    description:
      "Ask the coding agent to change the repository authorized by the host and save its result.",
    parameters: CodingRequest,
    success: CodingResult,
    failure: CodingError,
    dependencies: [CodingTask],
  }),
);
const handlers = tools.toLayer({
  start_coding: Effect.fnUntraced(function* ({ instructions }) {
    return yield* (yield* CodingTask).run(instructions);
  }),
});
const assistant = Agent.make("coding-assistant", {
  input: Schema.String,
  output: Schema.Struct({ summary: Schema.String, head: Schema.String }),
  instructions:
    "Help with the requested code change. Use start_coding for changes. Report the saved commit returned by that tool; never invent results.",
  toolkit: tools,
  policy: { maxTurns: 4, maxToolCalls: 2, toolConcurrency: 1, maxDuration: "5 minutes" },
});

export const runAssistant = Effect.fn("CodingAssistant.run")(function* (request: string) {
  return yield* AgentRuntime.run(assistant, request).pipe(Effect.provide(handlers));
});
