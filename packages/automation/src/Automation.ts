import { Effect, Schema } from "effect";

import { DeduplicationError, DeduplicationInput } from "./domain/Deduplication";
import type { DeduplicationDecision } from "./domain/Deduplication";
import { DeduplicationRepository } from "./server/database/DeduplicationRepository";
import * as Source from "./Source";

/** Supply the invocation's scoped SqlClient; schema migration remains an explicit host operation. */
export const layerPostgres = DeduplicationRepository.layer;

export interface Input<Tag extends string> {
  /** Stable notification/work identity, retained across delivery retries. */
  readonly id: string;
  readonly source: Tag;
  readonly input: unknown;
}

export interface CanonicalIssue extends Source.Issue {
  readonly id: string;
  readonly source: string;
}

export const make = <
  const Sources extends ReadonlyArray<Source.Source<string, unknown, unknown, unknown>>,
  A,
  E,
  R,
>(options: {
  readonly name: string;
  readonly scope: string;
  readonly sources: Sources;
  readonly workflow: (issue: CanonicalIssue) => Effect.Effect<A, E, R>;
}) => {
  type Tag = Sources[number]["_tag"];
  type Policy = ReturnType<NonNullable<Sources[number]["dedupeIssue"]>>;
  type SelectedSource = Source.Source<
    Tag,
    Effect.Error<Policy>,
    Effect.Services<Policy>,
    Effect.Services<ReturnType<Sources[number]["issue"]>>
  >;
  const scope = JSON.stringify([options.name, options.scope]);

  const prepare = Effect.fnUntraced(function* (request: Input<Tag>) {
    yield* Schema.decodeUnknownEffect(Schema.Array(DeduplicationInput.fields.id))([
      options.name,
      options.scope,
      request.source,
    ]);
    const matching = options.sources.filter((candidate) => candidate._tag === request.source);
    if (matching.length !== 1)
      return yield* DeduplicationError.make({
        reason: "invalid_decision",
        cause: "Unknown or repeated source",
      });
    const source = matching[0]! as SelectedSource;
    const issue = yield* source.issue(request.input);
    const input = yield* Schema.decodeEffect(DeduplicationInput)({
      id: request.id,
      scope,
      sourceId: JSON.stringify([source._tag, issue.key]),
      context: { source: source._tag, ...issue },
    });
    return { source, issue, input };
  });

  const resolve = Effect.fn("Automation.resolve")(function* (request: Input<Tag>) {
    const { source, issue, input } = yield* prepare(request);
    const repository = yield* DeduplicationRepository;
    const commit = Effect.gen(function* () {
      const snapshot = yield* repository.load(input);
      if (snapshot._tag === "Retained") return snapshot.decision;
      let decision: DeduplicationDecision = { _tag: "Distinct", reason: null };
      if (snapshot.identityMatch !== null) {
        decision = {
          _tag: "Duplicate",
          canonicalId: snapshot.identityMatch,
          matchedBy: "identity",
          reason: null,
        };
      } else if (source.dedupeIssue && snapshot.candidates.length > 0) {
        const candidates = yield* Schema.decodeUnknownEffect(Schema.Array(Source.Candidate))(
          snapshot.candidates.map((candidate) => ({ ...candidate.context, id: candidate.id })),
        );
        const match = yield* source.dedupeIssue({
          incoming: { ...issue, source: source._tag },
          candidates,
        });
        if (match !== null) {
          if (match === input.id || !candidates.some((candidate) => candidate.id === match))
            return yield* DeduplicationError.make({ reason: "invalid_decision" });
          decision = { _tag: "Duplicate", canonicalId: match, matchedBy: "semantic", reason: null };
        }
      }
      return yield* repository.accept(input, snapshot.revision, decision);
    });
    const decision = yield* commit.pipe(
      Effect.retry({
        while: (error) => Schema.is(DeduplicationError)(error) && error.reason === "conflict",
        times: 2,
      }),
    );
    return { issue: { id: input.id, source: source._tag, ...issue }, decision };
  });

  /**
   * Distinct redeliveries invoke the same workflow identity so its checkpoints can
   * resume. The workflow owns durable execution and idempotent side effects;
   * this decision gate does not turn an arbitrary Effect into an exactly-once job.
   * Duplicate inputs return the canonical identity without invoking the workflow.
   */
  const run = Effect.fn("Automation.run")(function* (request: Input<Tag>) {
    const resolved = yield* resolve(request);
    if (resolved.decision._tag === "Duplicate")
      return { _tag: "Duplicate" as const, ...resolved, decision: resolved.decision };
    return {
      _tag: "Completed" as const,
      ...resolved,
      value: yield* options.workflow(resolved.issue),
    };
  });

  const lookup = Effect.fn("Automation.lookup")(function* (request: Input<Tag>) {
    const { input } = yield* prepare(request);
    return yield* (yield* DeduplicationRepository).lookup(input);
  });

  return {
    name: options.name,
    scope: options.scope,
    sources: options.sources,
    resolve,
    run,
    lookup,
  };
};
