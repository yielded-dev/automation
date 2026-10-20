import { Context, Effect, Option, Schema } from "effect";

import {
  DeduplicationError,
  SemanticDeduplicationResult,
  type DeduplicationCandidate,
  type DeduplicationDecision,
  type DeduplicationInput,
  type DeduplicationSnapshot,
} from "../domain/Deduplication";

/**
 * Bind to existing host records. load must look up source identity independently
 * of the bounded semantic shortlist, following previously retained aliases.
 * accept atomically rechecks the revision and identity, retains one decision for
 * the input id, and rejects stale comparisons with reason "conflict". Concurrent
 * delivery of an already accepted input returns its retained decision instead.
 * The host validates canonical ownership and preserves existing accepted work.
 */
export class DeduplicationStore extends Context.Service<
  DeduplicationStore,
  {
    readonly load: (
      input: DeduplicationInput,
    ) => Effect.Effect<DeduplicationSnapshot, DeduplicationError>;
    readonly accept: (
      input: DeduplicationInput,
      revision: string,
      decision: DeduplicationDecision,
    ) => Effect.Effect<DeduplicationDecision, DeduplicationError>;
  }
>()("@yielded/automation/DeduplicationStore") {}

/** Optional consumer policy. LLM credentials, model choice and evidence belong to the host. */
export class DeduplicationPolicy extends Context.Service<
  DeduplicationPolicy,
  {
    readonly compare: (input: {
      readonly incoming: DeduplicationInput;
      readonly candidates: ReadonlyArray<DeduplicationCandidate>;
    }) => Effect.Effect<SemanticDeduplicationResult, DeduplicationError>;
  }
>()("@yielded/automation/DeduplicationPolicy") {}

/** Resolve identity first, consult an optional policy, then retain the decision before work starts. */
export const deduplicate = Effect.fn("Deduplication.resolve")(function* (
  input: DeduplicationInput,
) {
  const store = yield* DeduplicationStore;
  const snapshot = yield* store.load(input);

  if (snapshot._tag === "Retained") return snapshot.decision;
  let decision: DeduplicationDecision = { _tag: "Distinct", reason: null };

  if (snapshot.identityMatch !== null) {
    decision = {
      _tag: "Duplicate",
      canonicalId: snapshot.identityMatch,
      matchedBy: "identity",
      reason: null,
    };
  } else if (snapshot.candidates.length > 0) {
    const policy = yield* Effect.serviceOption(DeduplicationPolicy);

    if (Option.isSome(policy)) {
      const proposed = yield* policy.value.compare({
        incoming: input,
        candidates: snapshot.candidates,
      });
      const result = yield* Schema.decodeUnknownEffect(SemanticDeduplicationResult)(proposed).pipe(
        Effect.mapError((cause) => DeduplicationError.make({ reason: "invalid_decision", cause })),
      );

      decision = { _tag: "Distinct", reason: result.reason };

      if (result.duplicateOf !== null) {
        if (
          result.duplicateOf === input.id ||
          !snapshot.candidates.some((candidate) => candidate.id === result.duplicateOf)
        )
          return yield* DeduplicationError.make({ reason: "invalid_decision" });
        decision = {
          _tag: "Duplicate",
          canonicalId: result.duplicateOf,
          matchedBy: "semantic",
          reason: result.reason,
        };
      }
    }
  }

  return yield* store.accept(input, snapshot.revision, decision);
});
