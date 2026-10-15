import { AgentEventSink } from "../cloudflare/AgentEventSink.mjs";
import { makeRunSpanCapture } from "./RunSpanCapture.mjs";
import { Effect, Queue, Stream, Tracer } from "effect";
//#region src/tracing/PhaseEvents.ts
/** Drain phase boundaries before returning the pipeline outcome to its owner. */
const withPhaseEvents = (startedAtMillis) => (effect) => Effect.gen(function* () {
	const sink = yield* AgentEventSink;
	const queue = yield* Effect.acquireRelease(Queue.unbounded(), Queue.shutdown);
	const capture = makeRunSpanCapture({
		startedAtMillis,
		delegate: yield* Effect.tracer,
		maxSpans: 0,
		onPhase: (phase, status, at, outcome) => {
			Queue.offerUnsafe(queue, {
				type: "agent.phase",
				phase,
				status,
				at,
				...outcome === void 0 ? {} : { outcome }
			});
		}
	});
	const [exit] = yield* Effect.all([effect.pipe(Effect.provideService(Tracer.Tracer, capture.tracer), Effect.exit, Effect.ensuring(Queue.end(queue))), Stream.fromQueue(queue).pipe(Stream.runForEach(sink.emit))], { concurrency: "unbounded" });
	return yield* exit;
}).pipe(Effect.scoped);
//#endregion
export { withPhaseEvents };
