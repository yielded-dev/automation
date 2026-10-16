import { Schema } from "effect";

export const maxDiagnosticRounds = 3;

/** Query kinds and retained evidence belong to the host; only progress belongs to the workflow. */
export const InvestigationPlan = Schema.Struct({
  data: Schema.JsonObject,
  queries: Schema.Array(Schema.JsonObject),
  retry: Schema.optionalKey(Schema.Boolean),
});
export type InvestigationPlan = typeof InvestigationPlan.Type;

const common = {
  version: Schema.Literal("1"),
  data: Schema.JsonObject,
};
const diagnosticRound = Schema.Int.check(
  Schema.isBetween({ minimum: 1, maximum: maxDiagnosticRounds }),
);
const noQueries = {
  queries: Schema.Array(Schema.JsonObject).check(Schema.isMaxLength(0)),
  cursor: Schema.Literal(0),
};

const pending = Schema.Struct({
  ...common,
  phase: Schema.Literals([
    "collecting",
    "planning_search",
    "searching",
    "planning_diagnostics",
    "reading_diagnostics",
    "planning_lookup",
    "looking_up",
    "synthesizing",
  ]),
  queries: Schema.Array(Schema.JsonObject),
  cursor: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  round: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: maxDiagnosticRounds })),
  output: Schema.Null,
}).check(
  Schema.makeFilter((checkpoint) => {
    const { phase, queries, cursor, round } = checkpoint;
    const beforeDiagnostics =
      phase === "collecting" || phase === "planning_search" || phase === "searching";

    if (beforeDiagnostics ? round !== 0 : round === 0)
      return { path: ["round"], issue: "Round does not match the investigation phase" };

    if (phase === "searching" || phase === "reading_diagnostics" || phase === "looking_up") {
      if (cursor >= queries.length)
        return { path: ["cursor"], issue: "A read requires an unconsumed query" };
    } else if (queries.length !== 0 || cursor !== 0) {
      return { path: ["queries"], issue: "Only read phases may retain pending queries" };
    }

    if (phase === "collecting" && Object.keys(checkpoint.data).length !== 0)
      return { path: ["data"], issue: "Collection starts with empty evidence" };
  }),
);

/** Persist this logical checkpoint, never an actor snapshot or host credentials. */
export const InvestigationCheckpoint = Schema.Union([
  pending,
  Schema.Struct({
    ...common,
    ...noQueries,
    phase: Schema.Literal("completed"),
    round: diagnosticRound,
    output: Schema.JsonObject,
  }),
]);
export type InvestigationCheckpoint = typeof InvestigationCheckpoint.Type;

/** Hosts translate their failures once, retaining the complete Effect Cause in cause. */
export class InvestigationWorkflowError extends Schema.TaggedError<InvestigationWorkflowError>()(
  "InvestigationWorkflowError",
  { code: Schema.String, cause: Schema.Defect() },
) {}
