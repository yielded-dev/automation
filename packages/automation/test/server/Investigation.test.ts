import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { Cause, Context, Effect, Exit, Schema, Stream } from "effect";
import { AiError, LanguageModel, Tool, Toolkit } from "effect/ai";
import { expect, it } from "vite-plus/test";

import { InvestigationEvidence, makeInvestigator } from "../../src/server/Investigation";
import { TriageJournal } from "../../src/server/TriageJournal";

const Read = Tool.make("read", {
  parameters: Schema.Struct({ query: Schema.String.check(Schema.isMaxLength(300)) }),
  success: Schema.String,
}).annotate(Tool.Readonly, true);
const input = {
  requestKind: "bug" as const,
  model: "fixture",
  prompt: "Investigate",
  deadlineMs: 1000,
};

// Regression: https://reve-r6.sentry.io/issues/7791127826
// Correcting an invalid read must not replay sibling writes from a failed tool batch.
it("does not regenerate a failed batch when the toolkit can write", async () => {
  let calls = 0;
  let writes = 0;
  const tools = Toolkit.make(Read, Tool.make("write", { success: Schema.Void }));
  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const model = yield* LanguageModel.make({
        generateText: () =>
          Effect.sync(() => {
            calls++;
            return [
              { type: "tool-call" as const, id: "write", name: "write", params: {} },
              {
                type: "tool-call" as const,
                id: "read",
                name: "read",
                params: { query: "x".repeat(304) },
              },
            ];
          }),
        streamText: () => Stream.die("Unexpected streaming"),
      });
      return yield* Effect.exit(
        makeInvestigator(tools)(input).pipe(
          Effect.provideService(LanguageModel.LanguageModel, model),
        ),
      );
    }).pipe(
      Effect.provide(
        tools.toLayer({
          read: () => Effect.die("Invalid parameters reached the read handler"),
          write: () =>
            Effect.sync(() => {
              writes++;
            }),
        }),
      ),
      Effect.provideService(InvestigationEvidence, { sources: Effect.succeed([]) }),
      Effect.provideService(TriageJournal, { write: () => Effect.void }),
      Effect.provide(NodeCrypto.layer),
    ),
  );
  expect(Exit.isFailure(result)).toBe(true);
  expect(calls).toBe(1);
  expect(writes).toBeLessThanOrEqual(1);
  if (Exit.isFailure(result)) {
    const reason = result.cause.reasons[0];
    expect(
      reason &&
        Cause.isFailReason(reason) &&
        AiError.isAiError(reason.error) &&
        reason.error.reason._tag === "ToolParameterValidationError",
    ).toBe(true);
  }
});

// The new research recovery boundary must preserve mixed causes and handler failures.
it.each(["defect", "interrupted", "handler"] as const)(
  "preserves %s validation causes without correction",
  async (scenario) => {
    const error = AiError.make({
      module: "Toolkit",
      method: "read.handle",
      reason: new AiError.ToolParameterValidationError({
        toolName: "read",
        description: "PRIVATE_DETAILS",
      }),
    });
    const validation = Cause.annotate(
      Cause.fail(error),
      Context.make(Toolkit.FailureOrigin, scenario === "handler" ? "handler" : "parameters"),
    );
    const original =
      scenario === "handler"
        ? validation
        : Cause.combine(
            validation,
            scenario === "defect" ? Cause.die(new Error("PRIVATE_DEFECT")) : Cause.interrupt(123),
          );
    const tools = Toolkit.make(Read);
    let calls = 0;
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const model = yield* LanguageModel.make({
          generateText: () =>
            Effect.suspend(() => {
              calls++;
              return Effect.failCause(original);
            }),
          streamText: () => Stream.die("Unexpected streaming"),
        });
        return yield* Effect.exit(
          makeInvestigator(tools)(input).pipe(
            Effect.provideService(LanguageModel.LanguageModel, model),
          ),
        );
      }).pipe(
        Effect.provide(tools.toLayer({ read: () => Effect.die("Unexpected read") })),
        Effect.provideService(InvestigationEvidence, { sources: Effect.succeed([]) }),
        Effect.provideService(TriageJournal, { write: () => Effect.void }),
        Effect.provide(NodeCrypto.layer),
      ),
    );
    expect(calls).toBe(1);
    expect(Exit.isFailure(result)).toBe(true);
    if (Exit.isFailure(result)) {
      expect(result.cause.reasons).toHaveLength(original.reasons.length);
      for (const [index, reason] of original.reasons.entries()) {
        const actual = result.cause.reasons[index];
        expect(actual?._tag).toBe(reason._tag);
        if (Cause.isFailReason(reason))
          expect(actual && Cause.isFailReason(actual) && actual.error).toBe(reason.error);
        else if (Cause.isDieReason(reason))
          expect(actual && Cause.isDieReason(actual) && actual.defect).toBe(reason.defect);
        else
          expect(actual && Cause.isInterruptReason(actual) && actual.fiberId).toBe(reason.fiberId);
      }
      expect(Context.get(Cause.annotations(result.cause), Toolkit.FailureOrigin)).toBe(
        scenario === "handler" ? "handler" : "parameters",
      );
    }
  },
);
