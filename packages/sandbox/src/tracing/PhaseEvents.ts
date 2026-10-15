import { Effect, Queue, Stream, Tracer, type Cause } from "effect";
import type { AgentPhaseEvent } from "../Contract";
import { AgentEventSink } from "../cloudflare/AgentEventSink";
import { makeRunSpanCapture } from "./RunSpanCapture";

/** Drain phase boundaries before returning the pipeline outcome to its owner. */
export const withPhaseEvents =
  (startedAtMillis: number) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.gen(function* () {
      const sink = yield* AgentEventSink;
      const queue = yield* Effect.acquireRelease(
        Queue.unbounded<AgentPhaseEvent, Cause.Done>(),
        Queue.shutdown,
      );
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
            ...(outcome === undefined ? {} : { outcome }),
          });
        },
      });
      const [exit] = yield* Effect.all(
        [
          effect.pipe(
            Effect.provideService(Tracer.Tracer, capture.tracer),
            Effect.exit,
            Effect.ensuring(Queue.end(queue)),
          ),
          Stream.fromQueue(queue).pipe(Stream.runForEach(sink.emit)),
        ],
        { concurrency: "unbounded" },
      );
      return yield* exit;
    }).pipe(Effect.scoped);
