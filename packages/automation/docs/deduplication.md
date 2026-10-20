# Deduplication

`deduplicate` resolves exact source identity, optionally compares similar items,
and retains the decision before work starts. Bind `DeduplicationStore` to existing
host records. Bind `DeduplicationPolicy` only when semantic comparison is wanted;
the library requires no model or provider credentials.

## Consumer binding

The host implements `load` and `accept` under the storage contract below. These
bindings belong at the consumer's composition root; `hostStore` and `hostPolicy`
stand for those implementations, with their own dependencies already provided.

```ts
import { Effect, Layer } from "effect";
import {
  deduplicate,
  DeduplicationPolicy,
  DeduplicationStore,
} from "@yielded/automation/server/Deduplication";

declare const hostStore: DeduplicationStore["Service"];
declare const hostPolicy: DeduplicationPolicy["Service"];

const StoreLive = Layer.succeed(DeduplicationStore, hostStore);
const PolicyLive = Layer.succeed(DeduplicationPolicy, hostPolicy);
const input = {
  id: "work-42",
  scope: "project-7",
  sourceId: "event-18",
  context: { summary: "Request processing failed" },
};

// Exact identity and retained decisions work with only the store.
const resolveIdentity = deduplicate(input).pipe(Effect.provide(StoreLive));

// The optional policy is also supplied through Effect context.
const resolveWithComparison = deduplicate(input).pipe(
  Effect.provide(Layer.merge(StoreLive, PolicyLive)),
);
```

These are alternative effects for the same input. Once a decision is retained,
adding or changing a policy does not reclassify it on replay. Continue through the
host's existing accepted-work path: a retained `Distinct` is not permission to
start the same work twice.

## Identity and decisions

| Field      | Host responsibility                                                                                  |
| ---------- | ---------------------------------------------------------------------------------------------------- |
| `id`       | Stable identity for this decision and its accepted work; reuse it on retry.                          |
| `scope`    | Authorized deduplication boundary, such as a project; isolate identities and candidates accordingly. |
| `sourceId` | Stable exact source identity within that scope, independent of delivery or work IDs.                 |
| `context`  | Bounded JSON evidence suitable for the host's comparison policy.                                     |

`load(input)` returns either `{ _tag: "Retained", decision }` or a
`Pending` snapshot containing `revision`, `identityMatch` and `candidates`.
A retained decision returns immediately without calling policy or `accept`.

Look up the exact `sourceId` independently of the semantic candidate shortlist.
Age limits, ranking, or shortlist size must not hide an exact match. Follow
previously accepted source aliases to the canonical ID: a source previously
classified as a semantic duplicate must resolve to that same canonical work on a
later delivery. Scope selection, authorization, alias storage and canonical
ownership remain host responsibilities.

An `identityMatch` produces `Duplicate` with `matchedBy: "identity"` and
`reason: null`, bypassing policy. Otherwise, if candidates and policy are both
present, `compare({ incoming, candidates })` returns
`{ duplicateOf: string | null, reason: string }`. The ID must be one of the
supplied canonical candidates and cannot equal the incoming `id`. A match
produces `Duplicate` with `canonicalId`, `matchedBy: "semantic"` and the policy
reason. `duplicateOf: null` produces `Distinct` with that reason. Reasons must
be nonempty, trimmed strings of at most 2,000 characters.

With no policy or no candidates, the proposal is `Distinct` with `reason: null`.
Every new proposal goes through `accept`; its returned decision is authoritative.
Model choice, credentials, evidence selection and any model-specific validation
belong in the host policy.

## Atomic acceptance and retry

`accept(input, revision, decision)` must atomically:

1. Return the retained decision if this input was already accepted, including
   when a concurrent delivery committed it after `load`.
2. Recheck the revision, exact source identity and canonical ownership in the
   authorized scope. Reject a stale comparison with
   `DeduplicationError.make({ reason: "conflict" })`.
3. Retain the decision together with its source alias and any newly accepted
   canonical work in the host's existing records, preserving prior accepted
   identities. Make the new state visible to subsequent loads.

The revision must cover concurrent insertion of canonical work, including when
the original shortlist was empty. A revision on only the incoming row or the
listed candidates cannot protect against two different IDs both seeing no
candidates and becoming canonical. Use the database's atomic transaction or
conditional-write guarantees for the scoped invariant.

Read the snapshot, release database resources, then perform semantic comparison.
Open a short atomic commit only for acceptance; never hold a database transaction
across an LLM call. For example, if A and B both read an empty scope, A can commit
as canonical. B's old revision must then conflict. Retrying B reloads A as a
candidate and can retain B as its semantic duplicate.

Propagate the typed `conflict` into the caller's existing queue retry mechanism,
using the same `id`, `scope` and `sourceId`. Retry the whole `deduplicate` operation
so it obtains a fresh snapshot; do not retry only `accept` with a stale proposal.
The library does not create a queue or retry loop.

Store and policy failures remain typed `DeduplicationError` failures with their
original causes. Invalid policy output or an unlisted candidate produces
`reason: "invalid_decision"` without acceptance. Policy failure does not silently
admit distinct work. Apply the host's existing failure handling; only continue
work after a successful retained decision.
