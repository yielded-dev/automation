import { Context, Data } from "effect";
//#region src/cloudflare/AgentEventSink.ts
var AgentEventSinkError = class extends Data.TaggedError("AgentEventSinkError") {};
/** The host owns delivery and durable retention of the pipeline's output. */
var AgentEventSink = class extends Context.Service()("kommunikasie/AgentEventSink") {};
//#endregion
export { AgentEventSink, AgentEventSinkError };
