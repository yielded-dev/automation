import { Context, Effect, Exit, Predicate, Tracer } from "effect";
//#region src/tracing/RunSpanCapture.ts
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
const RunSpanKind = Context.Reference("sandbox-agent/tracing/RunSpanKind", { defaultValue: () => "untracked" });
/** Span attribute carrying a retry count into `SpanRecord.attempts`. */
const SPAN_ATTEMPTS_ATTRIBUTE = "sandboxAgent.attempts";
const measureAnnotations = Context.make(RunSpanKind, "measure");
const phaseAnnotations = Context.make(RunSpanKind, "phase");
/** Records a span around `effect` in the run record only. */
const inMeasure = (name) => (effect) => Effect.withSpan(name, { annotations: measureAnnotations }, { captureStackTrace: false })(effect);
/** Records a span around `effect` and mirrors it as live `agent.phase` events. */
const inPhase = (name) => (effect) => Effect.withSpan(name, { annotations: phaseAnnotations }, { captureStackTrace: false })(effect);
/** Records a zero-length marker span (e.g. first stdout byte). */
const markPoint = (name) => inMeasure(name)(Effect.void);
/** Stores a retry count on the enclosing measured span. */
const annotateSpanAttempts = (attempts) => Effect.annotateCurrentSpan(SPAN_ATTEMPTS_ATTRIBUTE, attempts);
/** Creates the capture tracer scoped to one message request. */
const makeRunSpanCapture = (options) => {
	const records = [];
	let omittedSpans = 0;
	const onPhase = options.onPhase ?? (() => void 0);
	const offsetOf = (nanos) => Number(nanos / 1000000n) - options.startedAtMillis;
	const delegate = options.delegate;
	return {
		tracer: Tracer.make({
			...delegate?.context === void 0 ? {} : { context: delegate.context },
			span(spanOptions) {
				const span = delegate?.span(spanOptions) ?? new Tracer.NativeSpan(spanOptions);
				const kind = Context.get(spanOptions.annotations, RunSpanKind);
				if (kind === "untracked") return span;
				const record = {
					name: spanOptions.name,
					startAt: offsetOf(spanOptions.startTime),
					endAt: null,
					outcome: "unknown"
				};
				if (options.maxSpans === void 0 || records.length < options.maxSpans) records.push(record);
				else omittedSpans++;
				if (kind === "phase") onPhase(record.name, "start", record.startAt);
				const end = span.end.bind(span);
				let ended = false;
				span.end = (endTime, exit) => {
					if (ended) return;
					ended = true;
					record.endAt = offsetOf(endTime);
					record.outcome = Exit.isSuccess(exit) ? "ok" : "failed";
					const attempts = span.attributes.get(SPAN_ATTEMPTS_ATTRIBUTE);
					if (Predicate.isNumber(attempts) && Number.isSafeInteger(attempts)) record.attempts = attempts;
					if (kind === "phase") onPhase(record.name, "end", record.endAt, record.outcome);
					end(endTime, exit);
				};
				return span;
			}
		}),
		snapshot: Effect.sync(() => records.map((record) => ({
			name: record.name,
			startAt: record.startAt,
			endAt: record.endAt,
			outcome: record.outcome,
			...record.attempts === void 0 ? {} : { attempts: record.attempts }
		}))),
		omittedSpans: Effect.sync(() => omittedSpans)
	};
};
//#endregion
export { RunSpanKind, SPAN_ATTEMPTS_ATTRIBUTE, annotateSpanAttempts, inMeasure, inPhase, makeRunSpanCapture, markPoint };
