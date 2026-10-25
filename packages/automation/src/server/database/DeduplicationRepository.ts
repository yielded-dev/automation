import { Cause, Context, Effect, Layer, Schema } from "effect";
import { SqlClient } from "effect/sql";

import {
  DeduplicationCandidate,
  DeduplicationDecision,
  DeduplicationError,
  type DeduplicationInput,
  type DeduplicationSnapshot,
} from "../../domain/Deduplication";

type Identity = Pick<DeduplicationInput, "scope" | "id" | "sourceId">;

const DecisionJson = Schema.fromJsonString(DeduplicationDecision);
const ContextJson = Schema.fromJsonString(Schema.JsonObject);
const StoredInput = Schema.Struct({
  id: Schema.String,
  sourceId: Schema.String,
  decision: DeduplicationDecision,
});
const StoredInputJson = Schema.fromJsonString(StoredInput);
const AcceptanceRow = Schema.Struct({
  retained: Schema.NullOr(Schema.String),
  identity: Schema.NullOr(Schema.String),
  root: Schema.NullOr(Schema.String),
});
const SnapshotRow = Schema.Struct({
  revision: Schema.String,
  retained: Schema.NullOr(StoredInputJson),
  identity: Schema.NullOr(StoredInputJson),
  candidates: Schema.fromJsonString(Schema.Array(DeduplicationCandidate)),
});
const canonicalId = (row: typeof StoredInput.Type) =>
  row.decision._tag === "Distinct" ? row.id : row.decision.canonicalId;

const storeErrors = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.catchCause((cause) =>
      Effect.failCause(
        Cause.map(cause, (error) =>
          Schema.is(DeduplicationError)(error)
            ? error
            : DeduplicationError.make({ reason: "store", cause: error }),
        ),
      ),
    ),
  );

const make = Effect.gen(function* () {
  const sql = (yield* SqlClient.SqlClient).withoutTransforms();

  const lookup = Effect.fn("DeduplicationRepository.lookup")(function* (input: Identity) {
    const row = (yield* sql<{ source_id: string; decision: string }>`
      SELECT source_id, decision FROM automation_deduplication_inputs
      WHERE scope = ${input.scope} AND id = ${input.id}`)[0];
    if (row === undefined) return undefined;
    if (row.source_id !== input.sourceId)
      return yield* DeduplicationError.make({ reason: "store", cause: "Input source changed" });
    return yield* Schema.decodeEffect(DecisionJson)(row.decision);
  }, storeErrors);

  const load = Effect.fn("DeduplicationRepository.load")(function* (input: DeduplicationInput) {
    // One statement binds the revision, exact identity and shortlist to one PG snapshot.
    const rows = yield* sql`
      WITH known AS MATERIALIZED (SELECT
        COALESCE((SELECT revision::text FROM automation_deduplication_scopes
          WHERE scope = ${input.scope}), '0') AS revision,
        (SELECT jsonb_build_object('id', id, 'sourceId', source_id,
          'decision', decision::jsonb)::text FROM automation_deduplication_inputs
          WHERE scope = ${input.scope} AND id = ${input.id}) AS retained,
        (SELECT jsonb_build_object('id', id, 'sourceId', source_id,
          'decision', decision::jsonb)::text FROM automation_deduplication_inputs
          WHERE scope = ${input.scope} AND source_id = ${input.sourceId}
          ORDER BY id LIMIT 1) AS identity)
      SELECT known.*, CASE WHEN retained IS NOT NULL OR identity IS NOT NULL THEN '[]'
        ELSE COALESCE((SELECT jsonb_agg(jsonb_build_object('id', candidate.id,
          'sourceId', candidate.source_id, 'context', candidate.context::jsonb)
          ORDER BY candidate.created_at DESC NULLS LAST, candidate.id)::text FROM (
          SELECT id, source_id, context, created_at
          FROM automation_deduplication_inputs WHERE scope = ${input.scope}
            AND decision::jsonb->>'_tag' = 'Distinct'
          ORDER BY created_at DESC NULLS LAST, id LIMIT 20
        ) candidate), '[]') END AS candidates FROM known`;
    const row = yield* Schema.decodeUnknownEffect(SnapshotRow)(rows[0]);
    if (row.retained !== null) {
      if (row.retained.sourceId !== input.sourceId)
        return yield* DeduplicationError.make({ reason: "store", cause: "Input source changed" });
      return { _tag: "Retained", decision: row.retained.decision } satisfies DeduplicationSnapshot;
    }
    return {
      _tag: "Pending",
      revision: row.revision,
      identityMatch: row.identity === null ? null : canonicalId(row.identity),
      candidates: row.candidates,
    } satisfies DeduplicationSnapshot;
  }, storeErrors);

  const persist = Effect.fnUntraced(function* (
    input: DeduplicationInput,
    revision: string,
    proposed: DeduplicationDecision,
  ) {
    yield* sql`INSERT INTO automation_deduplication_scopes (scope, revision)
      VALUES (${input.scope}, 0) ON CONFLICT DO NOTHING`;
    const [scope] = yield* sql<{ revision: string }>`
      SELECT revision::text AS revision FROM automation_deduplication_scopes
      WHERE scope = ${input.scope} FOR UPDATE`;
    if (scope === undefined)
      return yield* DeduplicationError.make({ reason: "store", cause: "Missing scope" });

    // The lock is acquired in the preceding statement, so this snapshot includes
    // any decision committed while we were waiting for it.
    // Decode the proposed target from JSON only if selected: an unused proposal
    // containing NUL must not fail PostgreSQL text binding before identity reuse.
    const rows = yield* sql`
      WITH known AS MATERIALIZED (SELECT
        (SELECT jsonb_build_object('id', id, 'sourceId', source_id,
          'decision', decision::jsonb)::text FROM automation_deduplication_inputs
          WHERE scope = ${input.scope} AND id = ${input.id}) AS retained,
        (SELECT jsonb_build_object('id', id, 'sourceId', source_id,
          'decision', decision::jsonb)::text FROM automation_deduplication_inputs
          WHERE scope = ${input.scope} AND source_id = ${input.sourceId}
          ORDER BY id LIMIT 1) AS identity,
        ${JSON.stringify(proposed._tag === "Duplicate" ? proposed.canonicalId : null)}::json AS proposed_root)
      SELECT retained, identity, CASE WHEN retained IS NULL THEN
        (SELECT decision FROM automation_deduplication_inputs WHERE scope = ${input.scope}
          AND id = COALESCE(CASE WHEN identity::jsonb->'decision'->>'_tag' = 'Distinct'
            THEN identity::jsonb->>'id' ELSE identity::jsonb->'decision'->>'canonicalId' END,
            known.proposed_root #>> '{}'))
        END AS root FROM known`;
    const row = yield* Schema.decodeUnknownEffect(AcceptanceRow)(rows[0]);
    if (row.retained !== null) {
      const retained = yield* Schema.decodeEffect(StoredInputJson)(row.retained);
      if (retained.sourceId !== input.sourceId)
        return yield* DeduplicationError.make({ reason: "store", cause: "Input source changed" });
      return retained.decision;
    }

    let decision = proposed;
    if (row.identity !== null) {
      const identity = yield* Schema.decodeEffect(StoredInputJson)(row.identity);
      decision = {
        _tag: "Duplicate",
        canonicalId: canonicalId(identity),
        matchedBy: "identity",
        reason: null,
      };
    } else if (scope.revision !== revision)
      return yield* DeduplicationError.make({ reason: "conflict" });

    if (decision._tag === "Duplicate") {
      if (
        decision.canonicalId === input.id ||
        row.root === null ||
        (yield* Schema.decodeEffect(DecisionJson)(row.root))._tag !== "Distinct"
      )
        return yield* DeduplicationError.make({ reason: "invalid_decision" });
    }

    // Candidate reads use jsonb; reject unsupported JSON before retaining a decision.
    yield* sql`INSERT INTO automation_deduplication_inputs (scope, id, source_id, context, decision)
      VALUES (${input.scope}, ${input.id}, ${input.sourceId},
        ${yield* Schema.encodeEffect(ContextJson)(input.context)}::jsonb::text,
        ${yield* Schema.encodeEffect(DecisionJson)(decision)}::jsonb::text)`;
    // Aliases affect exact identity lookup, which is always rechecked under this lock.
    // Only new canonical evidence invalidates an in-flight semantic comparison.
    if (decision._tag === "Distinct")
      yield* sql`UPDATE automation_deduplication_scopes SET revision = revision + 1
        WHERE scope = ${input.scope}`;
    return decision;
  });

  const accept = Effect.fn("DeduplicationRepository.accept")(
    (input: DeduplicationInput, revision: string, decision: DeduplicationDecision) =>
      sql.withTransaction(persist(input, revision, decision)),
    storeErrors,
  );
  return { load, accept, lookup };
});

/** Durable decision and alias ownership in the caller's PostgreSQL database. */
export class DeduplicationRepository extends Context.Service<
  DeduplicationRepository,
  Effect.Success<typeof make>
>()("@yielded/automation/DeduplicationRepository") {
  static readonly layer = Layer.effect(this, make);
}
