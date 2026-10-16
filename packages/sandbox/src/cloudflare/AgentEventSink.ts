import { Context, Data, type Effect } from "effect";

import type { AgentEvent } from "../Contract";

export class AgentEventSinkError extends Data.TaggedError("AgentEventSinkError")<{
  readonly cause: unknown;
}> {}

/** The host owns delivery and durable retention of the pipeline's output. */
export class AgentEventSink extends Context.Service<
  AgentEventSink,
  { readonly emit: (event: AgentEvent) => Effect.Effect<void, AgentEventSinkError> }
>()("kommunikasie/AgentEventSink") {}
