import {
  InvestigationCheckpoint,
  InvestigationWorkflowError,
} from "@yielded/automation/domain/InvestigationWorkflow";
import {
  InvestigationWorkflow,
  runInvestigation,
} from "@yielded/automation/server/InvestigationWorkflow";
import { Cause, Context, Deferred, Effect, Exit, Fiber, Schema } from "effect";
import { expect, it } from "vite-plus/test";

const decodeCheckpoint = Schema.decodeUnknownSync(InvestigationCheckpoint);

const fixture = () => {
  let saved: InvestigationCheckpoint | null = null;
  const checkpoints: InvestigationCheckpoint[] = [];
  const calls: string[] = [];
  const record = <A>(call: string, value: A) =>
    Effect.sync(() => {
      calls.push(call);
      return value;
    });
  const read = (kind: string, data: Schema.JsonObject, query: Schema.JsonObject) =>
    record(`${kind}:${query.id}`, { ...data, [`${kind}:${query.id}`]: true });
  const workflow = InvestigationWorkflow.of({
    authorize: record("authorize", undefined),
    collect: record("collect", { evidence: true }),
    planSearch: (data) => record("planSearch", { data, queries: [] }),
    search: ({ data, query }) => read("search", data, query),
    planDiagnostics: ({ data, round }) => record(`planDiagnostics:${round}`, { data, queries: [] }),
    readDiagnostic: ({ data, query }) => read("diagnostic", data, query),
    planLookup: (data) => record("planLookup", { data, queries: [] }),
    lookup: ({ data, query }) => read("lookup", data, query),
    synthesize: (data) => record("synthesize", { report: data }),
    checkpoint: (checkpoint) =>
      Effect.sync(() => {
        // Model a storage round trip through the public schema, not an in-memory actor snapshot.
        saved = decodeCheckpoint(JSON.parse(JSON.stringify(checkpoint)));
        checkpoints.push(saved);
        calls.push(`checkpoint:${saved.phase}:${saved.round}:${saved.cursor}`);
      }),
  });
  return { workflow, checkpoints, calls, record, read, saved: () => saved };
};

const Provider = Context.Reference<string>("test/InvestigationWorkflow/provider", {
  defaultValue: () => "none",
});

// Regression: a failure late in an investigation replayed already-successful source reads.
it("resumes each lookup and synthesis from durable evidence without replaying expensive work", async () => {
  const host = fixture();
  const providerCause = Cause.annotate(
    Cause.combine(Cause.fail({ provider: "unavailable" }), Cause.die({ cleanup: "failed" })),
    Context.make(Provider, "primary"),
  );
  const error = InvestigationWorkflowError.make({ code: "provider", cause: providerCause });
  const synthesisDefect = { cleanup: "synthesis" };
  const synthesisCause = Cause.annotate(
    Cause.combine(
      Cause.combine(Cause.fail(error), Cause.die(synthesisDefect)),
      Cause.interrupt(73),
    ),
    Context.make(Provider, "synthesis"),
  );
  let failLookup = true;
  let failSynthesis = true;
  const workflow = InvestigationWorkflow.of({
    ...host.workflow,
    planSearch: (data) =>
      host.record("planSearch", {
        data,
        // Search may also contain host-selected attachments and initial diagnostic reads.
        queries: Array.from({ length: 8 }, (_, id) => ({ id })),
      }),
    planDiagnostics: ({ data, round }) =>
      host.record(`planDiagnostics:${round}`, {
        data,
        queries: round === 2 ? [] : [{ id: `${round}:a` }, { id: `${round}:b` }],
        retry: round === 2,
      }),
    planLookup: (data) =>
      host.record("planLookup", { data, queries: [{ id: "one" }, { id: "two" }] }),
    lookup: ({ data, query }) =>
      host
        .read("lookup", data, query)
        .pipe(
          Effect.flatMap((result) =>
            query.id === "two" && failLookup ? Effect.fail(error) : Effect.succeed(result),
          ),
        ),
    synthesize: (data) =>
      host
        .record("synthesize", { report: data })
        .pipe(
          Effect.flatMap((result) =>
            failSynthesis ? Effect.failCause(synthesisCause) : Effect.succeed(result),
          ),
        ),
  });
  const observed = new Set<string>();
  const deliver = () =>
    Effect.runPromiseExit(
      runInvestigation(host.saved(), {
        inspect: (event) => {
          if ("snapshot" in event && "value" in event.snapshot)
            observed.add(String(event.snapshot.value));
        },
      }).pipe(Effect.provideService(InvestigationWorkflow, workflow)),
    );

  const first = await deliver();
  expect(Exit.isFailure(first)).toBe(true);
  if (Exit.isFailure(first)) {
    const reason = first.cause.reasons[0];
    expect(reason && Cause.isFailReason(reason) && reason.error).toBe(error);
    expect(error.cause).toBe(providerCause);
  }
  expect(host.saved()).toMatchObject({
    phase: "looking_up",
    cursor: 1,
    round: 3,
    output: null,
    data: { "lookup:one": true },
  });
  expect(host.saved()?.data).not.toHaveProperty("lookup:two");
  expect(
    host.checkpoints
      .filter((checkpoint) => checkpoint.phase === "searching")
      .map((checkpoint) => checkpoint.cursor),
  ).toEqual([0, 1, 2, 3, 4, 5, 6, 7]);
  expect(host.calls.filter((call) => call.startsWith("planDiagnostics:"))).toEqual([
    "planDiagnostics:1",
    "planDiagnostics:2",
    "planDiagnostics:3",
  ]);

  failLookup = false;
  const beforeSynthesis = host.calls.length;
  const second = await deliver();
  expect(Exit.isFailure(second)).toBe(true);
  if (Exit.isFailure(second)) {
    expect(second.cause.reasons).toHaveLength(3);
    const [failure, defect, interruption] = second.cause.reasons;
    expect(failure && Cause.isFailReason(failure) && failure.error).toBe(error);
    expect(defect && Cause.isDieReason(defect) && defect.defect).toBe(synthesisDefect);
    expect(interruption && Cause.isInterruptReason(interruption) && interruption.fiberId).toBe(73);
    expect(Context.get(Cause.annotations(second.cause), Provider)).toBe("synthesis");
  }
  expect(host.calls.slice(beforeSynthesis)).toEqual([
    "authorize",
    "lookup:two",
    "checkpoint:synthesizing:3:0",
    "synthesize",
  ]);
  expect(host.saved()).toMatchObject({ phase: "synthesizing", output: null, queries: [] });

  failSynthesis = false;
  const beforeResume = host.calls.length;
  const third = await deliver();
  expect(Exit.isSuccess(third)).toBe(true);
  expect(host.calls.slice(beforeResume)).toEqual([
    "authorize",
    "synthesize",
    "checkpoint:completed:3:0",
  ]);
  expect(host.saved()).toMatchObject({
    phase: "completed",
    output: { report: { "search:7": true, "diagnostic:3:b": true, "lookup:two": true } },
  });
  const beforeCompleted = host.calls.length;
  expect(await deliver()).toEqual(third);
  expect(host.calls.slice(beforeCompleted)).toEqual(["authorize"]);
  expect(observed.has("searching")).toBe(true);
  expect(observed.has("reading_diagnostics")).toBe(true);
  expect(observed.has("looking_up")).toBe(true);
  expect(observed.has("checkpointing")).toBe(true);
  expect(observed.has("completed")).toBe(true);
});

it("waits for checkpoint completion and resumes from the last successful save after a save failure", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const host = fixture();
      const saving = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void, InvestigationWorkflowError>();
      const error = InvestigationWorkflowError.make({
        code: "checkpoint_unavailable",
        cause: Cause.fail("storage"),
      });
      let failSave = true;
      const workflow = InvestigationWorkflow.of({
        ...host.workflow,
        planSearch: (data) =>
          host.record("planSearch", { data, queries: [{ id: "one" }, { id: "two" }] }),
        checkpoint: (checkpoint) =>
          failSave && checkpoint.phase === "searching" && checkpoint.cursor === 1
            ? Deferred.succeed(saving, undefined).pipe(Effect.andThen(Deferred.await(release)))
            : host.workflow.checkpoint(checkpoint),
      });
      const run = runInvestigation(null).pipe(
        Effect.provideService(InvestigationWorkflow, workflow),
      );
      const fiber = yield* Effect.forkChild(run);
      yield* Deferred.await(saving);
      expect(host.calls.at(-1)).toBe("search:one");
      expect(host.calls).not.toContain("search:two");
      expect(host.saved()).toMatchObject({
        phase: "searching",
        cursor: 0,
        data: { evidence: true },
      });
      yield* Deferred.fail(release, error);
      const failed = yield* Fiber.await(fiber);
      expect(Exit.isFailure(failed)).toBe(true);
      expect(host.calls).not.toContain("search:two");
      expect(host.saved()?.data).not.toHaveProperty("search:one");

      failSave = false;
      yield* runInvestigation(host.saved()).pipe(
        Effect.provideService(InvestigationWorkflow, workflow),
      );
      expect(host.calls.filter((call) => call.startsWith("search:"))).toEqual([
        "search:one",
        "search:one",
        "search:two",
      ]);
      expect(host.calls.filter((call) => call === "collect" || call === "planSearch")).toEqual([
        "collect",
        "planSearch",
      ]);
    }).pipe(Effect.timeout("3 seconds")),
  );
});

it("checkpoints retryable empty diagnostic plans and stops after three rounds", async () => {
  const host = fixture();
  await Effect.runPromise(
    runInvestigation(null).pipe(
      Effect.provideService(InvestigationWorkflow, {
        ...host.workflow,
        planDiagnostics: ({ data, round }) =>
          host.record(`planDiagnostics:${round}`, { data, queries: [], retry: true }),
      }),
    ),
  );
  expect(host.calls.filter((call) => call.startsWith("planDiagnostics:"))).toEqual([
    "planDiagnostics:1",
    "planDiagnostics:2",
    "planDiagnostics:3",
  ]);
  expect(
    host.checkpoints
      .filter((checkpoint) => checkpoint.phase === "planning_diagnostics")
      .map((checkpoint) => checkpoint.round),
  ).toEqual([1, 2, 3]);
  expect(host.calls.indexOf("checkpoint:planning_lookup:3:0")).toBeLessThan(
    host.calls.indexOf("planLookup"),
  );
  expect(host.saved()?.phase).toBe("completed");
});

it("awaits interrupted operation cleanup and retains its failure cause", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const host = fixture();
      const started = yield* Deferred.make<void>();
      const releasing = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const defect = { cleanup: "failed" };
      let active = false;
      const synthesis = Effect.acquireUseRelease(
        Effect.sync(() => {
          active = true;
        }),
        () => Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
        () =>
          Effect.gen(function* () {
            yield* Deferred.succeed(releasing, undefined);
            yield* Deferred.await(release);
            active = false;
            return yield* Effect.die(defect);
          }),
      );
      const checkpoint = decodeCheckpoint({
        version: "1",
        phase: "synthesizing",
        data: { evidence: true },
        queries: [],
        cursor: 0,
        round: 1,
        output: null,
      });
      const fiber = yield* runInvestigation(checkpoint).pipe(
        Effect.provideService(InvestigationWorkflow, {
          ...host.workflow,
          synthesize: () => synthesis,
        }),
        Effect.forkChild,
      );
      yield* Deferred.await(started);
      const stopping = yield* Effect.forkChild(Fiber.interrupt(fiber));
      yield* Deferred.await(releasing);
      expect(active).toBe(true);
      expect(stopping.pollUnsafe()).toBeUndefined();
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(stopping);
      expect(active).toBe(false);
      const outcome = yield* Fiber.await(fiber);
      expect(Exit.isFailure(outcome)).toBe(true);
      if (Exit.isFailure(outcome)) {
        expect(outcome.cause.reasons.some((reason) => Cause.isInterruptReason(reason))).toBe(true);
        expect(outcome.cause.reasons.some((reason) => Cause.isFailReason(reason))).toBe(false);
        expect(
          outcome.cause.reasons.some(
            (reason) => Cause.isDieReason(reason) && reason.defect === defect,
          ),
        ).toBe(true);
      }
      expect(host.calls).toEqual(["authorize"]);
    }).pipe(Effect.timeout("3 seconds")),
  );
});
