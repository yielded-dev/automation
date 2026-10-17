import { createEffectActor, fromEffect, setupEffect, waitFor } from "@xstate/effect";
import { Cause, Context, Effect, Exit, Fiber, Schema } from "effect";
import type { InspectionEvent } from "xstate";

import { InvestigationCheckpoint, InvestigationError } from "../domain/Investigator";

/** Private execution seam. Consumers supply tools and storage, never these operations. */
export class InvestigatorOperations extends Context.Service<
  InvestigatorOperations,
  {
    readonly advance: (
      state: InvestigationCheckpoint,
    ) => Effect.Effect<InvestigationCheckpoint, InvestigationError>;
    readonly save: (state: InvestigationCheckpoint) => Effect.Effect<void, InvestigationError>;
  }
>()("@yielded/automation/internal/InvestigatorOperations") {}

const failure = Schema.Cause(InvestigationError, Schema.Defect());
const settled = ({
  event,
}: {
  readonly event: { readonly output: Exit.Exit<InvestigationCheckpoint, InvestigationError> };
}) =>
  Exit.isFailure(event.output)
    ? { target: "failed" as const, context: { failure: event.output.cause } }
    : { target: "saving" as const, context: { checkpoint: event.output.value } };

export const investigatorMachine = setupEffect({
  schemas: {
    input: InvestigationCheckpoint,
    context: Schema.Struct({ checkpoint: InvestigationCheckpoint, failure }),
    output: Schema.Exit(Schema.JsonObject, InvestigationError, Schema.Defect()),
  },
  actors: {
    advance: fromEffect({
      schemas: { input: InvestigationCheckpoint },
      effect: ({ input }) =>
        InvestigatorOperations.use((operations) => operations.advance(input)).pipe(Effect.exit),
    }),
    save: fromEffect({
      schemas: { input: InvestigationCheckpoint },
      effect: ({ input }) =>
        InvestigatorOperations.use((operations) => operations.save(input)).pipe(
          Effect.as(input),
          Effect.exit,
        ),
    }),
  },
}).createMachine({
  id: "investigator",
  context: ({ input }) => ({ checkpoint: input, failure: Cause.empty }),
  initial: "routing",
  states: {
    routing: {
      always: ({ context }) => ({ target: context.checkpoint.phase }),
    },
    researching: {
      invoke: { src: "advance", input: ({ context }) => context.checkpoint, onDone: settled },
    },
    reading: {
      invoke: { src: "advance", input: ({ context }) => context.checkpoint, onDone: settled },
    },
    reporting: {
      invoke: { src: "advance", input: ({ context }) => context.checkpoint, onDone: settled },
    },
    saving: {
      invoke: {
        src: "save",
        input: ({ context }) => context.checkpoint,
        onDone: ({ event }) =>
          Exit.isFailure(event.output)
            ? { target: "failed", context: { failure: event.output.cause } }
            : { target: "routing", context: { checkpoint: event.output.value } },
      },
    },
    completed: { type: "final", output: ({ context }) => Exit.succeed(context.checkpoint.output!) },
    failed: { type: "final", output: ({ context }) => Exit.failCause(context.failure) },
  },
});

export interface RunInvestigatorOptions {
  readonly inspect?: (event: InspectionEvent) => void;
}

/** The Effect alpha stops children without awaiting their finalizers; the owner joins them. */
export const runInvestigatorMachine = Effect.fn("Investigator.machine")(function* (
  input: InvestigationCheckpoint,
  options?: RunInvestigatorOptions,
) {
  const operations = yield* InvestigatorOperations;

  return yield* Effect.acquireUseRelease(
    Effect.sync(() => new Set<Fiber.Fiber<unknown, InvestigationError>>()),
    (pending) =>
      Effect.gen(function* () {
        const scope = yield* Effect.scope;
        const owned = <A>(effect: Effect.Effect<A, InvestigationError>) =>
          Effect.uninterruptibleMask((restore) =>
            Effect.gen(function* () {
              const fiber = yield* effect.pipe(Effect.forkIn(scope));
              pending.add(fiber);
              const value = yield* restore(Fiber.join(fiber));
              pending.delete(fiber);
              return value;
            }),
          );
        const actor = yield* createEffectActor(investigatorMachine, { input }).pipe(
          Effect.provideService(InvestigatorOperations, {
            advance: (state) => owned(operations.advance(state)),
            save: (state) => owned(operations.save(state)),
          }),
        );

        if (options?.inspect) {
          const subscription = actor.inspect(options.inspect);
          yield* Effect.addFinalizer(() => Effect.sync(() => subscription.unsubscribe()));
        }

        return yield* Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            const result = yield* restore(
              waitFor(actor, (snapshot) => snapshot.status === "done"),
            ).pipe(
              Effect.catchCause((cause) =>
                Effect.failCause(
                  Cause.map(cause, (error) =>
                    InvestigationError.make({ code: "actor_stopped", cause: error }),
                  ),
                ),
              ),
            );
            pending.clear();
            return yield* result.output;
          }),
        );
      }).pipe(Effect.scoped),
    (pending) =>
      Effect.gen(function* () {
        yield* Fiber.interruptAll(pending);
        const outcomes = yield* Fiber.awaitAll(pending);
        let unconsumed: Cause.Cause<InvestigationError> = Cause.empty;

        for (const outcome of outcomes) {
          if (Exit.isFailure(outcome) && !Cause.hasInterruptsOnly(outcome.cause))
            unconsumed = Cause.combine(unconsumed, outcome.cause);
        }
        if (unconsumed.reasons.length > 0) return yield* Effect.failCause(unconsumed);
      }),
  );
});
