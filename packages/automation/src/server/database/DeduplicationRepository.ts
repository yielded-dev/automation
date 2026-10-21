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
const SnapshotRow = Schema.Struct({
  revision: Schema.String,
  retained: Schema.NullOr(Schema.fromJsonString(StoredInput)),
  identity: Schema.NullOr(Schema.fromJsonString(StoredInput)),
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
    return yield* Schema.decodeUnknownEffect(DecisionJson)(row.decision);
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

    const retained = yield* lookup(input);
    if (retained !== undefined) return retained;

    const [identity] = yield* sql<{ id: string; decision: string }>`
      SELECT id, decision FROM automation_deduplication_inputs
      WHERE scope = ${input.scope} AND source_id = ${input.sourceId}
      ORDER BY id LIMIT 1`;
    let decision = proposed;
    if (identity !== undefined) {
      const saved = yield* Schema.decodeUnknownEffect(DecisionJson)(identity.decision);
      const root = saved._tag === "Distinct" ? identity.id : saved.canonicalId;
      decision = { _tag: "Duplicate", canonicalId: root, matchedBy: "identity", reason: null };
    } else if (scope.revision !== revision)
      return yield* DeduplicationError.make({ reason: "conflict" });

    if (decision._tag === "Duplicate") {
      const [root] = yield* sql<{ decision: string }>`
        SELECT decision FROM automation_deduplication_inputs
        WHERE scope = ${input.scope} AND id = ${decision.canonicalId}`;
      if (
        decision.canonicalId === input.id ||
        root === undefined ||
        (yield* Schema.decodeUnknownEffect(DecisionJson)(root.decision))._tag !== "Distinct"
      )
        return yield* DeduplicationError.make({ reason: "invalid_decision" });
    }

    yield* sql`INSERT INTO automation_deduplication_inputs (scope, id, source_id, context, decision)
      VALUES (${input.scope}, ${input.id}, ${input.sourceId},
        ${yield* Schema.encodeEffect(ContextJson)(input.context)},
        ${yield* Schema.encodeEffect(DecisionJson)(decision)})`;
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
