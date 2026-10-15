import { Context, Effect, Exit, Predicate, Tracer } from "effect";

import type { SpanRecord } from "../RunRecord";

/**
 * Run-record span capture on Effect's native tracing. Code instruments with
 * ordinary `Effect.withSpan` through the combinators below; a per-request
 * capture tracer materializes the annotated spans into `SpanRecord`s for the
 * archived run document and mirrors phase spans as live `agent.phase` events.
 * Every started span also records its end and a typed outcome, on failure and
 * interruption too; overlapping work stays overlapping spans instead of being
 * summed twice. Offsets are milliseconds on the server clock since the
 * request was accepted.
 */

/**
 * Span annotation deciding what the capture tracer records: `untracked`
 * spans (the default, including every `Effect.fn` span) pass through,
 * `measure` spans enter the run record, and `phase` spans additionally
 * mirror live start/end events for timeline rendering.
 */
export const RunSpanKind = Context.Reference<"untracked" | "measure" | "phase">(
  "sandbox-agent/tracing/RunSpanKind",
  { defaultValue: () => "untracked" },
);

/** Span attribute carrying a retry count into `SpanRecord.attempts`. */
export const SPAN_ATTEMPTS_ATTRIBUTE = "sandboxAgent.attempts";

const measureAnnotations = Context.make(RunSpanKind, "measure");
const phaseAnnotations = Context.make(RunSpanKind, "phase");

/** Records a span around `effect` in the run record only. */
export const inMeasure =
  (name: string) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    Effect.withSpan(
      name,
      { annotations: measureAnnotations },
      { captureStackTrace: false },
    )(effect);

/** Records a span around `effect` and mirrors it as live `agent.phase` events. */
export const inPhase =
  (name: string) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    Effect.withSpan(name, { annotations: phaseAnnotations }, { captureStackTrace: false })(effect);

/** Records a zero-length marker span (e.g. first stdout byte). */
export const markPoint = (name: string): Effect.Effect<void> => inMeasure(name)(Effect.void);

/** Stores a retry count on the enclosing measured span. */
export const annotateSpanAttempts = (attempts: number): Effect.Effect<void> =>
  Effect.annotateCurrentSpan(SPAN_ATTEMPTS_ATTRIBUTE, attempts);

interface CapturedRecord {
  readonly name: string;
  readonly startAt: number;
  endAt: number | null;
  outcome: "ok" | "failed" | "unknown";
  attempts?: number;
}

/** Live phase mirror; synchronous because the tracer contract is synchronous. */
export type OnPhase = (
  phase: string,
  status: "start" | "end",
  atMillis: number,
  outcome?: "ok" | "failed",
) => void;

export interface RunSpanCapture {
  /** Install for the request with `Effect.provideService(Tracer.Tracer, tracer)`. */
  readonly tracer: Tracer.Tracer;
  /** Complete, unsampled records observed so far; a still-open span has
   * `endAt: null` and outcome `unknown`. */
  readonly snapshot: Effect.Effect<ReadonlyArray<SpanRecord>>;
  /** Measured spans omitted because the retention bound was reached. */
  readonly omittedSpans: Effect.Effect<number>;
}

/** Creates the capture tracer scoped to one message request. */
export const makeRunSpanCapture = (options: {
  readonly startedAtMillis: number;
  readonly onPhase?: OnPhase;
  readonly maxSpans?: number;
  /** Preserve the host tracer's span identity, export, and async context. */
  readonly delegate?: Tracer.Tracer;
}): RunSpanCapture => {
  const records: Array<CapturedRecord> = [];
  let omittedSpans = 0;
  const onPhase: OnPhase = options.onPhase ?? (() => undefined);
  const offsetOf = (nanos: bigint): number => Number(nanos / 1_000_000n) - options.startedAtMillis;
  const delegate = options.delegate;

  const tracer = Tracer.make({
    ...(delegate?.context === undefined ? {} : { context: delegate.context }),
    span(spanOptions) {
      const span = delegate?.span(spanOptions) ?? new Tracer.NativeSpan(spanOptions);
      const kind = Context.get(spanOptions.annotations, RunSpanKind);

      if (kind === "untracked") return span;
      const record: CapturedRecord = {
        name: spanOptions.name,
        startAt: offsetOf(spanOptions.startTime),
        endAt: null,
        outcome: "unknown",
      };

      if (options.maxSpans === undefined || records.length < options.maxSpans) records.push(record);
      else omittedSpans++;
      if (kind === "phase") onPhase(record.name, "start", record.startAt);

      const end = span.end.bind(span);
      let ended = false;

      // Keep the delegate's actual span: native tracers use its identity when
      // restoring async context. Capture never depends on its sampling decision.
      span.end = (endTime, exit) => {
        if (ended) return;
        ended = true;
        record.endAt = offsetOf(endTime);
        record.outcome = Exit.isSuccess(exit) ? "ok" : "failed";
        const attempts = span.attributes.get(SPAN_ATTEMPTS_ATTRIBUTE);

        if (Predicate.isNumber(attempts) && Number.isSafeInteger(attempts))
          record.attempts = attempts;
        if (kind === "phase") onPhase(record.name, "end", record.endAt, record.outcome);
        end(endTime, exit);
      };

      return span;
    },
  });

  const snapshot: Effect.Effect<ReadonlyArray<SpanRecord>> = Effect.sync(() =>
    records.map((record) => ({
      name: record.name,
      startAt: record.startAt,
      endAt: record.endAt,
      outcome: record.outcome,
      ...(record.attempts === undefined ? {} : { attempts: record.attempts }),
    })),
  );

  return { tracer, snapshot, omittedSpans: Effect.sync(() => omittedSpans) };
};
