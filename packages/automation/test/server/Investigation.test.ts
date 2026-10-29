import * as SecureRandom from "effect/crypto/SecureRandom";
import { createEffectActor, setupEffect, waitFor } from "@xstate/effect";
import { Cause, Context, Deferred, Effect, Exit, Fiber, Schema, Scope, Stream } from "effect";
import { AiError, LanguageModel, type Response, Tool, Toolkit } from "effect/ai";
import { expect, it } from "vite-plus/test";

import {
  InvestigationCheckpoint,
  InvestigationError,
  InvestigationObservation,
} from "../../src/domain/Investigator";
import { InvestigationStore, makeInvestigator } from "../../src/server/Investigation";

const Read = Tool.make("read", {
  parameters: Schema.Struct({ query: Schema.String.check(Schema.isMaxLength(300)) }),
  success: InvestigationObservation,
  failure: AiError.AiError,
}).annotate(Tool.Readonly, true);
const tools = Toolkit.make(Read);
const definition = {
  tools,
  result: Schema.Struct({ summary: Schema.String }),
  version: "fixture-v1",
  instructions: "Investigate the failed request and retain unavailable evidence as a limitation.",
};
const input = {
  id: "investigation-1",
  prompt: "Investigate the failed request",
  evidence: { items: [], limitations: ["Worker logs unavailable"] },
};
const observation = (id: string): InvestigationObservation => ({
  items: [
    {
      id,
      kind: "trace",
      label: id,
      sourceId: "source-1",
      sourceName: "Request traces",
      occurredAt: null,
      status: "observed",
      text: `Observed ${id}`,
      details: {},
    },
  ],
  limitations: [],
});
const finish = (reason: Response.FinishReason): Response.FinishPartEncoded => ({
  type: "finish",
  reason,
  usage: { inputTokens: {}, outputTokens: {} },
});
const plan = (...queries: string[]): Response.PartEncoded[] => [
  ...queries.map((query) => ({
    type: "tool-call" as const,
    id: `read:${query}`,
    name: "read",
    params: { query },
  })),
  finish(queries.length ? "tool-calls" : "stop"),
];
const report = (output: unknown): Response.PartEncoded[] => [
  { type: "text", text: JSON.stringify(output) },
  finish("stop"),
];
const model = (generateText: Parameters<typeof LanguageModel.make>[0]["generateText"]) =>
  LanguageModel.make({ generateText, streamText: () => Stream.die("Unexpected streaming") });
const fixture = () => {
  let saved: InvestigationCheckpoint | null = null;
  const checkpoints: InvestigationCheckpoint[] = [];
  const store = InvestigationStore.of({
    load: Effect.sync(() => saved),
    save: (checkpoint) =>
      Effect.sync(() => {
        // Recreate durable data, not a live actor snapshot or shared object reference.
        saved = Schema.decodeUnknownSync(InvestigationCheckpoint)(
          JSON.parse(JSON.stringify(checkpoint)),
        );
        checkpoints.push(saved);
      }),
  });
  return { store, checkpoints, saved: () => saved };
};

// Regression: correcting an invalid read must never replay sibling writes.
it("rejects a writable toolkit before planning or executing any tool", async () => {
  const host = fixture();
  const writable = Toolkit.make(Read, Tool.make("write", { success: InvestigationObservation }));
  let calls = 0;
  let writes = 0;
  const result = await Effect.runPromiseExit(
    Effect.gen(function* () {
      const languageModel = yield* model(() =>
        Effect.sync(() => {
          calls++;
          return [
            { type: "tool-call" as const, id: "write", name: "write", params: {} },
            ...plan("x".repeat(304)),
          ];
        }),
      );
      return yield* makeInvestigator({ ...definition, tools: writable })
        .run(input)
        .pipe(Effect.provideService(LanguageModel.LanguageModel, languageModel));
    }).pipe(
      Effect.provide(
        writable.toLayer({
          read: () => Effect.die("Unexpected read"),
          write: () =>
            Effect.sync(() => {
              writes++;
              return observation("write");
            }),
        }),
      ),
      Effect.provideService(InvestigationStore, host.store),
      Effect.provide(SecureRandom.layerWebCrypto),
    ),
  );
  expect(result).toMatchObject({
    _tag: "Failure",
    cause: {
      reasons: [{ _tag: "Fail", error: { _tag: "InvestigationError", code: "write_tool" } }],
    },
  });
  expect(calls).toBe(0);
  expect(writes).toBe(0);
  expect(host.checkpoints).toEqual([]);
});

it.each([true, () => true] as const)(
  "rejects approval policies before planning or executing a read (%s)",
  async (needsApproval) => {
    const host = fixture();
    const approvalTools = Toolkit.make(Read.setNeedsApproval(needsApproval));
    let calls = 0;
    let reads = 0;
    const result = await Effect.runPromiseExit(
      Effect.gen(function* () {
        const languageModel = yield* model((options) =>
          Effect.sync(() => {
            calls++;
            return options.responseFormat.type === "json"
              ? report({ finding: { summary: "Unapproved read" }, citations: ["one"] })
              : plan("one");
          }),
        );
        return yield* makeInvestigator({ ...definition, tools: approvalTools })
          .run(input)
          .pipe(Effect.provideService(LanguageModel.LanguageModel, languageModel));
      }).pipe(
        Effect.provide(
          approvalTools.toLayer({
            read: () =>
              Effect.sync(() => {
                reads++;
                return observation("one");
              }),
          }),
        ),
        Effect.provideService(InvestigationStore, host.store),
        Effect.provide(SecureRandom.layerWebCrypto),
      ),
    );
    expect(reads).toBe(0);
    expect(calls).toBe(0);
    expect(result).toMatchObject({
      _tag: "Failure",
      cause: {
        reasons: [{ _tag: "Fail", error: { _tag: "InvestigationError", code: "approval_tool" } }],
      },
    });
    expect(host.checkpoints).toEqual([]);
  },
);

it.each(["defect", "interrupted", "handler", "returned handler"] as const)(
  "preserves %s validation causes without argument correction",
  async (scenario) => {
    const host = fixture();
    const inHandler = scenario === "handler" || scenario === "returned handler";
    const tools = Toolkit.make(
      Tool.make("read", {
        parameters: Read.parametersSchema,
        success: Read.successSchema,
        failure: Read.failureSchema,
        failureMode: scenario === "returned handler" ? "return" : "error",
      }).annotate(Tool.Readonly, true),
    );
    const error = AiError.make({
      module: "Toolkit",
      method: "read.handle",
      reason: AiError.ToolParameterValidationError.make({
        toolName: "read",
        description: "PRIVATE_DETAILS",
      }),
    });
    const origin = inHandler ? "handler" : "parameters";
    const validation = Cause.annotate(
      Cause.fail(error),
      Context.make(Toolkit.FailureOrigin, origin),
    );
    const original = inHandler
      ? validation
      : Cause.combine(
          validation,
          scenario === "defect" ? Cause.die({ detail: "PRIVATE_DEFECT" }) : Cause.interrupt(123),
        );
    let calls = 0;
    let reads = 0;
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const languageModel = yield* model(() =>
          Effect.suspend(() => {
            calls++;
            return inHandler ? Effect.succeed(plan("one")) : Effect.failCause(original);
          }),
        );
        const actor = yield* createEffectActor(makeInvestigator({ ...definition, tools }).actor, {
          input,
        }).pipe(Effect.provideService(LanguageModel.LanguageModel, languageModel));
        return (yield* waitFor(actor, (snapshot) => snapshot.status === "done")).output!;
      }).pipe(
        Effect.scoped,
        Effect.provide(
          tools.toLayer({
            read: () =>
              Effect.suspend(() => {
                reads++;
                return Effect.fail(error);
              }),
          }),
        ),
        Effect.provideService(InvestigationStore, host.store),
        Effect.provide(SecureRandom.layerWebCrypto),
      ),
    );
    expect(calls).toBe(1);
    expect(reads).toBe(inHandler ? 1 : 0);
    expect(Exit.isFailure(result)).toBe(true);
    if (Exit.isFailure(result)) {
      expect(result.cause.reasons).toHaveLength(original.reasons.length);
      for (const [index, reason] of original.reasons.entries()) {
        const actual = result.cause.reasons[index];
        // Effect 4.1's LanguageModel promotes mixed failure/defect causes to defects.
        if (scenario === "defect" && Cause.isFailReason(reason)) {
          expect(actual?._tag).toBe("Die");
          expect(actual && Cause.isDieReason(actual) && actual.defect).toBe(reason.error);
          continue;
        }
        expect(actual?._tag).toBe(reason._tag);
        if (Cause.isFailReason(reason)) {
          expect(actual && Cause.isFailReason(actual) && actual.error).toMatchObject({
            _tag: "InvestigationError",
            code: "investigation",
          });
          expect(actual && Cause.isFailReason(actual) && actual.error.cause).toBe(reason.error);
        } else if (Cause.isDieReason(reason)) {
          expect(actual && Cause.isDieReason(actual) && actual.defect).toBe(reason.defect);
        } else {
          expect(actual && Cause.isInterruptReason(actual) && actual.fiberId).toBe(reason.fiberId);
        }
      }
      expect(Context.get(Cause.annotations(result.cause), Toolkit.FailureOrigin)).toBe(origin);
    }
  },
);

it("stops after three research rounds and resumes a failed report without repeating saved tools", async () => {
  const host = fixture();
  const reads: string[] = [];
  let plans = 0;
  let reports = 0;
  let failReport = true;
  const output = { finding: { summary: "Request timed out" }, citations: ["trace-1", "trace-3"] };
  const error = AiError.make({
    module: "fixture",
    method: "report",
    reason: AiError.UnknownError.make({ description: "Provider unavailable" }),
  });
  const reportPrompts: LanguageModel.ProviderOptions[] = [];
  const deliver = () =>
    Effect.runPromiseExit(
      Effect.gen(function* () {
        const languageModel = yield* model((options) =>
          Effect.suspend(() => {
            if (options.responseFormat.type === "json") {
              reports++;
              reportPrompts.push(options);
              return failReport ? Effect.fail(error) : Effect.succeed(report(output));
            }
            plans++;
            return Effect.succeed(plan(`trace-${plans}`));
          }),
        );
        return yield* makeInvestigator(definition)
          .run(input)
          .pipe(Effect.provideService(LanguageModel.LanguageModel, languageModel));
      }).pipe(
        Effect.provide(
          tools.toLayer({
            read: ({ query }) =>
              Effect.sync(() => {
                reads.push(query);
                return query === "trace-2"
                  ? {
                      items: observation(query).items.map((item) => ({
                        ...item,
                        status: "unavailable",
                      })),
                      limitations: ["Trace details unavailable"],
                      status: "unavailable" as const,
                      resultCount: null,
                    }
                  : observation(query);
              }),
          }),
        ),
        Effect.provideService(InvestigationStore, host.store),
        Effect.provide(SecureRandom.layerWebCrypto),
      ),
    );
  const first = await deliver();
  expect(Exit.isFailure(first)).toBe(true);
  if (Exit.isFailure(first)) {
    const reason = first.cause.reasons[0];
    expect(reason && Cause.isFailReason(reason) && reason.error.cause).toBe(error);
  }
  expect(host.saved()).toMatchObject({
    version: "2",
    phase: "reporting",
    round: 3,
    calls: [],
    output: null,
  });
  expect(reads).toEqual(["trace-1", "trace-2", "trace-3"]);
  expect(plans).toBe(3);

  failReport = false;
  const resumed = await deliver();
  expect(resumed).toEqual(Exit.succeed(output));
  expect(host.saved()).toMatchObject({ phase: "completed", output });
  expect(host.saved()?.evidence.items.map((item) => item.id)).toEqual(reads);
  expect(host.saved()?.evidence.limitations).toEqual([
    ...input.evidence.limitations,
    "Trace details unavailable",
  ]);
  expect(host.saved()?.evidence.items[1]?.status).toBe("unavailable");
  expect(host.saved()?.activity.filter((activity) => activity.operation === "read")).toMatchObject([
    { status: "completed", resultCount: 1 },
    { status: "unavailable", resultCount: null },
    { status: "completed", resultCount: 1 },
  ]);
  expect(plans).toBe(3);
  expect(reports).toBe(2);
  expect(reads).toEqual(["trace-1", "trace-2", "trace-3"]);
  expect(reportPrompts[1]?.responseFormat).toMatchObject({
    type: "json",
    objectName: "investigation_finding",
  });
  expect(JSON.stringify(reportPrompts[1]?.prompt)).toContain("Worker logs unavailable");
  expect(
    reportPrompts[1]?.prompt.content.filter((message) => message.role === "tool"),
  ).toHaveLength(3);
  const saves = host.checkpoints.length;
  expect(await deliver()).toEqual(resumed);
  expect(plans).toBe(3);
  expect(reports).toBe(2);
  expect(reads).toHaveLength(3);
  expect(host.checkpoints).toHaveLength(saves);
});

it("waits for each checkpoint and stops before the next tool when saving fails", async () => {
  const host = fixture();
  const reads: string[] = [];
  let plans = 0;
  let reports = 0;
  let failSave = true;
  await Effect.runPromise(
    Effect.gen(function* () {
      const saving = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void, InvestigationError>();
      const error = InvestigationError.make({ code: "checkpoint_unavailable", cause: "storage" });
      const store = InvestigationStore.of({
        ...host.store,
        save: (checkpoint) =>
          failSave && checkpoint.phase === "reading" && checkpoint.cursor === 1
            ? Deferred.succeed(saving, undefined).pipe(Effect.andThen(Deferred.await(release)))
            : host.store.save(checkpoint),
      });
      const languageModel = yield* model((options) =>
        Effect.sync(() => {
          if (options.responseFormat.type === "json") {
            reports++;
            return report({ finding: { summary: "Read both traces" }, citations: ["one", "two"] });
          }
          plans++;
          return plans === 1 ? plan("one", "two") : plan();
        }),
      );
      const run = makeInvestigator(definition)
        .run(input)
        .pipe(
          Effect.provideService(LanguageModel.LanguageModel, languageModel),
          Effect.provideService(InvestigationStore, store),
        );
      const fiber = yield* Effect.forkChild(run);
      yield* Deferred.await(saving);
      const whileSaving = {
        reads: [...reads],
        plans,
        reports,
        saved: host.saved(),
        outcome: fiber.pollUnsafe(),
      };
      yield* Deferred.fail(release, error);
      const failed = yield* Fiber.await(fiber);
      expect(whileSaving).toMatchObject({
        reads: ["one"],
        plans: 1,
        reports: 0,
        outcome: undefined,
      });
      expect(whileSaving.saved).toMatchObject({
        phase: "reading",
        cursor: 0,
        evidence: { items: [] },
      });
      expect(Exit.isFailure(failed)).toBe(true);
      if (Exit.isFailure(failed)) {
        expect(failed.cause.reasons).toHaveLength(1);
        const reason = failed.cause.reasons[0];
        expect(reason && Cause.isFailReason(reason) && reason.error).toBe(error);
      }
      expect(reads).toEqual(["one"]);
      expect(reports).toBe(0);
      expect(host.saved()).toEqual(whileSaving.saved);

      failSave = false;
      yield* run;
      expect(reads).toEqual(["one", "one", "two"]);
      expect(plans).toBe(2);
      expect(reports).toBe(1);
      expect(host.saved()).toMatchObject({ phase: "completed" });
    }).pipe(
      Effect.provide(
        tools.toLayer({
          read: ({ query }) =>
            Effect.sync(() => {
              reads.push(query);
              return observation(query);
            }),
        }),
      ),
      Effect.provide(SecureRandom.layerWebCrypto),
      Effect.timeout("3 seconds"),
    ),
  );
});

it("waits for cleanup on scope cancellation and preserves interruption with its cleanup defect", async () => {
  const host = fixture();
  await Effect.runPromise(
    Effect.gen(function* () {
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
      const languageModel = yield* model((options) =>
        options.responseFormat.type === "json" ? synthesis : Effect.succeed(plan()),
      );
      const scope = yield* Scope.make();
      const fiber = yield* makeInvestigator(definition)
        .run(input)
        .pipe(
          Effect.provideService(LanguageModel.LanguageModel, languageModel),
          Effect.forkIn(scope),
        );
      yield* Deferred.await(started);
      const closing = yield* Effect.forkChild(Scope.close(scope, Exit.void));
      yield* Deferred.await(releasing);
      const whileClosing = { active, outcome: closing.pollUnsafe() };
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(closing);
      const outcome = yield* Fiber.await(fiber);
      expect(whileClosing).toEqual({ active: true, outcome: undefined });
      expect(active).toBe(false);
      expect(Exit.isFailure(outcome)).toBe(true);
      if (Exit.isFailure(outcome)) {
        expect(outcome.cause.reasons.some(Cause.isInterruptReason)).toBe(true);
        expect(outcome.cause.reasons.some(Cause.isFailReason)).toBe(false);
        expect(
          outcome.cause.reasons.some(
            (reason) => Cause.isDieReason(reason) && reason.defect === defect,
          ),
        ).toBe(true);
      }
      expect(host.saved()).toMatchObject({ phase: "reporting", output: null });
    }).pipe(
      Effect.provide(tools.toLayer({ read: () => Effect.die("Unexpected read") })),
      Effect.provideService(InvestigationStore, host.store),
      Effect.provide(SecureRandom.layerWebCrypto),
      Effect.timeout("3 seconds"),
    ),
  );
});

it("waits for cleanup when the public actor's consumer scope closes", async () => {
  const host = fixture();
  await Effect.runPromise(
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const releasing = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const cleaned = yield* Deferred.make<void>();
      const defect = { cleanup: "public actor" };
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
            yield* Deferred.succeed(cleaned, undefined);
            return yield* Effect.die(defect);
          }),
      );
      const languageModel = yield* model((options) =>
        options.responseFormat.type === "json" ? synthesis : Effect.succeed(plan()),
      );
      const scope = yield* Scope.make();
      yield* createEffectActor(makeInvestigator(definition).actor, { input }).pipe(
        Effect.provideService(Scope.Scope, scope),
        Effect.provideService(LanguageModel.LanguageModel, languageModel),
      );
      yield* Deferred.await(started);
      const closing = yield* Effect.forkChild(Scope.close(scope, Exit.void));
      yield* Deferred.await(releasing);
      yield* Effect.sleep("10 millis");
      const whileClosing = { active, outcome: closing.pollUnsafe() };
      yield* Deferred.succeed(release, undefined);
      yield* Deferred.await(cleaned);
      const outcome = yield* Fiber.await(closing);
      expect(whileClosing).toEqual({ active: true, outcome: undefined });
      expect(active).toBe(false);
      expect(Exit.isFailure(outcome)).toBe(true);
      if (Exit.isFailure(outcome))
        expect(
          outcome.cause.reasons.some(
            (reason) => Cause.isDieReason(reason) && reason.defect === defect,
          ),
        ).toBe(true);
      expect(host.saved()).toMatchObject({ phase: "reporting", output: null });
    }).pipe(
      Effect.provide(tools.toLayer({ read: () => Effect.die("Unexpected read") })),
      Effect.provideService(InvestigationStore, host.store),
      Effect.provide(SecureRandom.layerWebCrypto),
      Effect.timeout("3 seconds"),
    ),
  );
});

it.each(["error", "return"] as const)(
  "corrects deferred read arguments in %s mode inside a consumer's review graph",
  async (failureMode) => {
    const host = fixture();
    const tools = Toolkit.make(
      Tool.make("read", {
        parameters: Read.parametersSchema,
        success: Read.successSchema,
        failure: Read.failureSchema,
        failureMode,
      }).annotate(Tool.Readonly, true),
    );
    const reads: string[] = [];
    const requests: LanguageModel.ProviderOptions[] = [];
    const finding = { severity: "high" as const, service: "api" };
    const investigator = makeInvestigator({
      ...definition,
      tools,
      result: Schema.Struct({ severity: Schema.Literals(["low", "high"]), service: Schema.String }),
    });
    const consumer = setupEffect({
      schemas: {
        context: Schema.Struct({ summary: Schema.String, citations: Schema.Array(Schema.String) }),
        events: { APPROVE: Schema.Struct({}) },
        output: Schema.String,
      },
      actors: { investigate: investigator.actor },
    }).createMachine({
      context: { summary: "", citations: [] },
      initial: "investigating",
      states: {
        investigating: {
          invoke: {
            src: "investigate",
            input,
            onDone: ({ event }) =>
              Exit.isFailure(event.output)
                ? { target: "failed" }
                : {
                    target: "reviewing",
                    context: {
                      summary: `${event.output.value.finding.severity}: ${event.output.value.finding.service}`,
                      citations: event.output.value.citations,
                    },
                  },
          },
        },
        reviewing: { on: { APPROVE: { target: "approved" } } },
        approved: { type: "final", output: ({ context }) => `Approved ${context.summary}` },
        failed: { type: "final", output: () => "Investigation failed" },
      },
    });
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const languageModel = yield* model((options) =>
          Effect.sync(() => {
            requests.push(options);
            if (options.responseFormat.type === "json")
              return report({ finding, citations: ["request"] });
            return requests.length === 1
              ? plan("x".repeat(304))
              : requests.length === 2
                ? plan("request")
                : plan();
          }),
        );
        const actor = yield* createEffectActor(consumer).pipe(
          Effect.provideService(LanguageModel.LanguageModel, languageModel),
        );
        const reviewing = yield* waitFor(
          actor,
          (snapshot) => snapshot.matches("reviewing") || snapshot.status === "done",
        );
        expect(reviewing.context).toEqual({ summary: "high: api", citations: ["request"] });
        expect(host.saved()).toMatchObject({
          phase: "completed",
          output: { finding, citations: ["request"] },
        });
        actor.send({ type: "APPROVE" });
        return (yield* waitFor(actor, (snapshot) => snapshot.status === "done")).output;
      }).pipe(
        Effect.scoped,
        Effect.provide(
          tools.toLayer({
            read: ({ query }) =>
              Effect.sync(() => {
                reads.push(query);
                return observation(query);
              }),
          }),
        ),
        Effect.provideService(InvestigationStore, host.store),
        Effect.provide(SecureRandom.layerWebCrypto),
        Effect.timeout("3 seconds"),
      ),
    );
    expect(result).toBe("Approved high: api");
    expect(reads).toEqual(["request"]);
    expect(requests).toHaveLength(4);
    expect(requests.slice(0, 3).every((request) => request.toolCallValidation === "deferred")).toBe(
      true,
    );
    expect(
      requests[1]?.prompt.content.flatMap((message) =>
        message.role === "tool" ? message.content : [],
      ),
    ).toMatchObject([{ type: "tool-result", name: "read", isFailure: true }]);
    expect(requests[3]?.responseFormat).toMatchObject({
      type: "json",
      objectName: "investigation_finding",
    });
  },
);
