import { createEffectActor, fromEffect, setupEffect, waitFor } from "@xstate/effect";
import { Cause, Context, Effect, Exit, Fiber, Schema } from "effect";
import type { InspectionEvent } from "xstate";

import {
  InvestigationCheckpoint,
  InvestigationPlan,
  InvestigationWorkflowError,
  maxDiagnosticRounds,
} from "../domain/InvestigationWorkflow";

/** Operations for one authorized delivery. Each result replaces the retained evidence data. */
export class InvestigationWorkflow extends Context.Service<
  InvestigationWorkflow,
  {
    readonly authorize: Effect.Effect<void, InvestigationWorkflowError>;
    readonly collect: Effect.Effect<Schema.JsonObject, InvestigationWorkflowError>;
    readonly planSearch: (
      data: Schema.JsonObject,
    ) => Effect.Effect<InvestigationPlan, InvestigationWorkflowError>;
    readonly search: (input: {
      readonly data: Schema.JsonObject;
      readonly query: Schema.JsonObject;
    }) => Effect.Effect<Schema.JsonObject, InvestigationWorkflowError>;
    readonly planDiagnostics: (input: {
      readonly data: Schema.JsonObject;
      readonly round: number;
    }) => Effect.Effect<InvestigationPlan, InvestigationWorkflowError>;
    readonly readDiagnostic: (input: {
      readonly data: Schema.JsonObject;
      readonly query: Schema.JsonObject;
    }) => Effect.Effect<Schema.JsonObject, InvestigationWorkflowError>;
    readonly planLookup: (
      data: Schema.JsonObject,
    ) => Effect.Effect<InvestigationPlan, InvestigationWorkflowError>;
    readonly lookup: (input: {
      readonly data: Schema.JsonObject;
      readonly query: Schema.JsonObject;
    }) => Effect.Effect<Schema.JsonObject, InvestigationWorkflowError>;
    readonly synthesize: (
      data: Schema.JsonObject,
    ) => Effect.Effect<Schema.JsonObject, InvestigationWorkflowError>;
    readonly checkpoint: (
      snapshot: InvestigationCheckpoint,
    ) => Effect.Effect<void, InvestigationWorkflowError>;
  }
>()("@yielded/automation/server/InvestigationWorkflow") {}

const initial: InvestigationCheckpoint = {
  version: "1",
  phase: "collecting",
  data: {},
  queries: [],
  cursor: 0,
  round: 0,
  output: null,
};
const failure = Schema.Cause(InvestigationWorkflowError, Schema.Defect());
const decodeCheckpoint = Schema.decodeUnknownEffect(InvestigationCheckpoint);
const decodePlan = Schema.decodeUnknownEffect(InvestigationPlan);

const validateCheckpoint = (input: unknown) =>
  decodeCheckpoint(input).pipe(
    Effect.catchCause((cause) =>
      Effect.failCause(
        Cause.map(cause, () =>
          InvestigationWorkflowError.make({ code: "invalid_checkpoint", cause }),
        ),
      ),
    ),
  );
const validatePlan = (input: unknown) =>
  decodePlan(input).pipe(
    Effect.catchCause((cause) =>
      Effect.failCause(
        Cause.map(cause, () => InvestigationWorkflowError.make({ code: "invalid_plan", cause })),
      ),
    ),
  );

const advance = (
  checkpoint: InvestigationCheckpoint,
  phase: Exclude<InvestigationCheckpoint["phase"], "completed">,
  data = checkpoint.data,
  round = checkpoint.round,
): InvestigationCheckpoint => ({
  ...checkpoint,
  phase,
  data,
  round,
  queries: [],
  cursor: 0,
  output: null,
});

type StepEvent = {
  readonly event: {
    readonly output: Exit.Exit<InvestigationCheckpoint, InvestigationWorkflowError>;
  };
};

const settled =
  (target: "checkpointing" | "routing") =>
  ({ event }: StepEvent) =>
    Exit.isFailure(event.output)
      ? { target: "failed" as const, context: { failure: event.output.cause } }
      : { target, context: { checkpoint: event.output.value } };

/**
 * Each host operation is an invoked Effect; only checkpointing can authorize the next operation.
 * Exit crosses the alpha's foreign boundary without squashing typed failures, defects or interrupts.
 * Execute with runInvestigation to await owned work and cleanup on cancellation.
 */
export const investigationMachine = setupEffect({
  schemas: {
    input: InvestigationCheckpoint,
    context: Schema.Struct({ checkpoint: InvestigationCheckpoint, failure }),
    output: Schema.Exit(Schema.JsonObject, InvestigationWorkflowError, Schema.Defect()),
  },
  states: {
    completed: { schemas: { context: Schema.Struct({ output: Schema.JsonObject }) } },
  },
  actors: {
    authorize: fromEffect({
      schemas: { input: InvestigationCheckpoint },
      effect: ({ input }) =>
        InvestigationWorkflow.use((workflow) => workflow.authorize).pipe(
          Effect.as(input),
          Effect.exit,
        ),
    }),
    collect: fromEffect({
      schemas: { input: InvestigationCheckpoint },
      effect: ({ input }) =>
        InvestigationWorkflow.use((workflow) => workflow.collect).pipe(
          Effect.map((data) => advance(input, "planning_search", data)),
          Effect.exit,
        ),
    }),
    planSearch: fromEffect({
      schemas: { input: InvestigationCheckpoint },
      effect: ({ input }) =>
        InvestigationWorkflow.use((workflow) => workflow.planSearch(input.data)).pipe(
          Effect.flatMap(validatePlan),
          Effect.map((plan): InvestigationCheckpoint =>
            plan.queries.length === 0
              ? advance(input, "planning_diagnostics", plan.data, 1)
              : { ...advance(input, "searching", plan.data), queries: plan.queries },
          ),
          Effect.exit,
        ),
    }),
    search: fromEffect({
      schemas: { input: InvestigationCheckpoint },
      effect: ({ input }) =>
        InvestigationWorkflow.use((workflow) =>
          workflow.search({ data: input.data, query: input.queries[input.cursor]! }),
        ).pipe(
          Effect.map((data): InvestigationCheckpoint =>
            input.cursor + 1 < input.queries.length
              ? { ...input, phase: "searching", output: null, data, cursor: input.cursor + 1 }
              : advance(input, "planning_diagnostics", data, 1),
          ),
          Effect.exit,
        ),
    }),
    planDiagnostics: fromEffect({
      schemas: { input: InvestigationCheckpoint },
      effect: ({ input }) =>
        InvestigationWorkflow.use((workflow) =>
          workflow.planDiagnostics({ data: input.data, round: input.round }),
        ).pipe(
          Effect.flatMap(validatePlan),
          Effect.map((plan): InvestigationCheckpoint => {
            if (plan.queries.length > 0)
              return {
                ...advance(input, "reading_diagnostics", plan.data),
                queries: plan.queries,
              };

            return plan.retry && input.round < maxDiagnosticRounds
              ? advance(input, "planning_diagnostics", plan.data, input.round + 1)
              : advance(input, "planning_lookup", plan.data);
          }),
          Effect.exit,
        ),
    }),
    readDiagnostic: fromEffect({
      schemas: { input: InvestigationCheckpoint },
      effect: ({ input }) =>
        InvestigationWorkflow.use((workflow) =>
          workflow.readDiagnostic({ data: input.data, query: input.queries[input.cursor]! }),
        ).pipe(
          Effect.map((data): InvestigationCheckpoint => {
            if (input.cursor + 1 < input.queries.length)
              return {
                ...input,
                phase: "reading_diagnostics",
                output: null,
                data,
                cursor: input.cursor + 1,
              };

            return input.round < maxDiagnosticRounds
              ? advance(input, "planning_diagnostics", data, input.round + 1)
              : advance(input, "planning_lookup", data);
          }),
          Effect.exit,
        ),
    }),
    planLookup: fromEffect({
      schemas: { input: InvestigationCheckpoint },
      effect: ({ input }) =>
        InvestigationWorkflow.use((workflow) => workflow.planLookup(input.data)).pipe(
          Effect.flatMap(validatePlan),
          Effect.map((plan): InvestigationCheckpoint =>
            plan.queries.length === 0
              ? advance(input, "synthesizing", plan.data)
              : { ...advance(input, "looking_up", plan.data), queries: plan.queries },
          ),
          Effect.exit,
        ),
    }),
    lookup: fromEffect({
      schemas: { input: InvestigationCheckpoint },
      effect: ({ input }) =>
        InvestigationWorkflow.use((workflow) =>
          workflow.lookup({ data: input.data, query: input.queries[input.cursor]! }),
        ).pipe(
          Effect.map((data): InvestigationCheckpoint =>
            input.cursor + 1 < input.queries.length
              ? { ...input, phase: "looking_up", output: null, data, cursor: input.cursor + 1 }
              : advance(input, "synthesizing", data),
          ),
          Effect.exit,
        ),
    }),
    synthesize: fromEffect({
      schemas: { input: InvestigationCheckpoint },
      effect: ({ input }) =>
        InvestigationWorkflow.use((workflow) => workflow.synthesize(input.data)).pipe(
          Effect.map((output): InvestigationCheckpoint => ({
            ...input,
            phase: "completed",
            queries: [],
            cursor: 0,
            output,
          })),
          Effect.exit,
        ),
    }),
    checkpoint: fromEffect({
      schemas: { input: InvestigationCheckpoint },
      effect: ({ input }) =>
        validateCheckpoint(input).pipe(
          Effect.tap((checkpoint) =>
            InvestigationWorkflow.use((workflow) => workflow.checkpoint(checkpoint)),
          ),
          Effect.exit,
        ),
    }),
  },
}).createMachine({
  id: "investigation",
  version: "1",
  context: ({ input }) => ({ checkpoint: input, failure: Cause.empty }),
  initial: "authorizing",
  states: {
    authorizing: {
      description:
        "Recheck current authority before resuming any delivery, including completed work.",
      invoke: {
        src: "authorize",
        input: ({ context }) => context.checkpoint,
        onDone: settled("routing"),
      },
    },
    routing: {
      description: "Resume the next operation recorded by the logical checkpoint.",
      always: ({ context }) =>
        context.checkpoint.phase === "completed"
          ? { target: "completed", context: { output: context.checkpoint.output } }
          : { target: context.checkpoint.phase },
    },
    collecting: {
      invoke: {
        src: "collect",
        input: ({ context }) => context.checkpoint,
        onDone: settled("checkpointing"),
      },
    },
    planning_search: {
      invoke: {
        src: "planSearch",
        input: ({ context }) => context.checkpoint,
        onDone: settled("checkpointing"),
      },
    },
    searching: {
      invoke: {
        src: "search",
        input: ({ context }) => context.checkpoint,
        onDone: settled("checkpointing"),
      },
    },
    planning_diagnostics: {
      invoke: {
        src: "planDiagnostics",
        input: ({ context }) => context.checkpoint,
        onDone: settled("checkpointing"),
      },
    },
    reading_diagnostics: {
      invoke: {
        src: "readDiagnostic",
        input: ({ context }) => context.checkpoint,
        onDone: settled("checkpointing"),
      },
    },
    planning_lookup: {
      invoke: {
        src: "planLookup",
        input: ({ context }) => context.checkpoint,
        onDone: settled("checkpointing"),
      },
    },
    looking_up: {
      invoke: {
        src: "lookup",
        input: ({ context }) => context.checkpoint,
        onDone: settled("checkpointing"),
      },
    },
    synthesizing: {
      invoke: {
        src: "synthesize",
        input: ({ context }) => context.checkpoint,
        onDone: settled("checkpointing"),
      },
    },
    checkpointing: {
      description: "Commit the next phase and evidence before starting any further host operation.",
      invoke: {
        src: "checkpoint",
        input: ({ context }) => context.checkpoint,
        onDone: settled("routing"),
      },
    },
    completed: { type: "final", output: ({ context }) => Exit.succeed(context.output) },
    failed: { type: "final", output: ({ context }) => Exit.failCause(context.failure) },
  },
});

export interface RunInvestigationOptions {
  readonly inspect?: (event: InspectionEvent) => void;
}

/** Resume only from the host's last committed checkpoint; failed saves never authorize more work. */
export const runInvestigation = Effect.fn("InvestigationWorkflow.run")(function* (
  checkpoint: InvestigationCheckpoint | null,
  options?: RunInvestigationOptions,
): Effect.fn.Return<Schema.JsonObject, InvestigationWorkflowError, InvestigationWorkflow> {
  const input = yield* validateCheckpoint(checkpoint ?? initial);
  const workflow = yield* InvestigationWorkflow;

  return yield* Effect.acquireUseRelease(
    Effect.sync(() => new Set<Fiber.Fiber<unknown, InvestigationWorkflowError>>()),
    (pending) =>
      Effect.gen(function* () {
        const scope = yield* Effect.scope;
        const owned = <A>(effect: Effect.Effect<A, InvestigationWorkflowError>) =>
          Effect.uninterruptibleMask((restore) =>
            Effect.gen(function* () {
              const fiber = yield* effect.pipe(Effect.forkIn(scope));
              pending.add(fiber);
              const value = yield* restore(Fiber.join(fiber));

              // Successful fibers have completed their cleanup. Retain failures until consumed.
              pending.delete(fiber);
              return value;
            }),
          );
        const actor = yield* createEffectActor(investigationMachine, { input }).pipe(
          Effect.provideService(InvestigationWorkflow, {
            authorize: owned(workflow.authorize),
            collect: owned(workflow.collect),
            planSearch: (data) => owned(workflow.planSearch(data)),
            search: (input) => owned(workflow.search(input)),
            planDiagnostics: (input) => owned(workflow.planDiagnostics(input)),
            readDiagnostic: (input) => owned(workflow.readDiagnostic(input)),
            planLookup: (data) => owned(workflow.planLookup(data)),
            lookup: (input) => owned(workflow.lookup(input)),
            synthesize: (data) => owned(workflow.synthesize(data)),
            checkpoint: (checkpoint) => owned(workflow.checkpoint(checkpoint)),
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
                  Cause.map(cause, () =>
                    InvestigationWorkflowError.make({ code: "actor_stopped", cause }),
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
        // The alpha interrupts hosted Effects without joining their cleanup or retaining its failures.
        yield* Fiber.interruptAll(pending);
        const outcomes = yield* Fiber.awaitAll(pending);
        let unconsumed: Cause.Cause<InvestigationWorkflowError> = Cause.empty;

        for (const outcome of outcomes) {
          if (Exit.isFailure(outcome) && !Cause.hasInterruptsOnly(outcome.cause))
            unconsumed = Cause.combine(unconsumed, outcome.cause);
        }

        if (unconsumed.reasons.length > 0) return yield* Effect.failCause(unconsumed);
      }),
  );
});
