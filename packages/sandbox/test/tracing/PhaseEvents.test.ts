import { Cause, Deferred, Effect, Exit, Schema } from "effect";
import { expect, it } from "vite-plus/test";
import { AgentEvent, agentPhases } from "../../src/Contract";
import { AgentEventSink, AgentEventSinkError } from "../../src/cloudflare/AgentEventSink";
import { inPhase } from "../../src/tracing/RunSpanCapture";
import { withPhaseEvents } from "../../src/tracing/PhaseEvents";

it("delivers live setup start, then a failed end before the failure event", async () => {
  const events: AgentEvent[] = [];
  const original = { _tag: "SetupFailure" };
  const exit = await Effect.runPromise(
    Effect.gen(function* () {
      const observedStart = yield* Deferred.make<void>();
      const sink = AgentEventSink.of({
        emit: (event) =>
          Effect.gen(function* () {
            events.push(Schema.decodeSync(AgentEvent)(event));
            if (event.type === "agent.phase" && event.status === "start")
              yield* Deferred.succeed(observedStart, undefined);
          }),
      });
      return yield* Deferred.await(observedStart).pipe(
        Effect.andThen(Effect.fail(original)),
        inPhase(agentPhases.repositorySetup),
        withPhaseEvents(Date.now()),
        Effect.tapCause(() =>
          sink.emit({
            type: "agent.failed",
            reason: "repository-setup-failed",
            message: "start-1 exited with code 1",
          }),
        ),
        Effect.provideService(AgentEventSink, sink),
        Effect.exit,
      );
    }),
  );
  expect(events.map((event) => event.type)).toEqual(["agent.phase", "agent.phase", "agent.failed"]);
  expect(events[0]).toMatchObject({ phase: "repository-setup", status: "start" });
  expect(events[0]).not.toHaveProperty("outcome");
  expect(events[1]).toMatchObject({ phase: "repository-setup", status: "end", outcome: "failed" });
  expect(Exit.isFailure(exit) && Cause.hasFails(exit.cause)).toBe(true);
  if (Exit.isFailure(exit))
    expect(exit.cause.reasons).toContainEqual(Cause.makeFailReason(original));
});

it("drains successful end boundaries and retains the return value", async () => {
  const events: AgentEvent[] = [];
  expect(
    await Effect.runPromise(
      Effect.succeed(42).pipe(
        inPhase(agentPhases.repositorySetup),
        withPhaseEvents(Date.now()),
        Effect.provideService(AgentEventSink, {
          emit: (event) =>
            Effect.sync(() => {
              events.push(event);
            }),
        }),
      ),
    ),
  ).toBe(42);
  expect(events[1]).toMatchObject({ status: "end", outcome: "ok" });
});

it("interrupts execution on failed phase delivery and runs cleanup", async () => {
  let cleaned = false;
  const failure = new AgentEventSinkError({ cause: "unavailable storage" });
  const exit = await Effect.runPromise(
    Effect.never.pipe(
      Effect.ensuring(
        Effect.sync(() => {
          cleaned = true;
        }),
      ),
      inPhase(agentPhases.repositorySetup),
      withPhaseEvents(Date.now()),
      Effect.provideService(AgentEventSink, { emit: () => Effect.fail(failure) }),
      Effect.exit,
    ),
  );
  expect(cleaned).toBe(true);
  expect(
    Exit.isFailure(exit) &&
      exit.cause.reasons.some((reason) => Cause.isFailReason(reason) && reason.error === failure),
  ).toBe(true);
});
