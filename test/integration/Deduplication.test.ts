import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdtemp } from "node:fs/promises";
import { PgClient } from "@effect/sql-pg";
import {
  Context,
  Deferred,
  Effect,
  Layer,
  ManagedRuntime,
  Redacted,
  Schema,
  SchemaGetter,
} from "effect";
import { LanguageModel } from "effect/ai";
import { SqlClient, Statement } from "effect/sql";
import { afterAll, beforeAll, expect, expectTypeOf, it } from "vite-plus/test";

import * as Automation from "../../packages/automation/src/Automation";
import * as SemanticPolicy from "../../packages/automation/src/SemanticPolicy";
import * as Source from "../../packages/automation/src/Source";
import * as Sources from "../../packages/automation/src/Sources";
import {
  DeduplicationError,
  type DeduplicationDecision,
  type DeduplicationInput,
  type DeduplicationSnapshot,
} from "../../packages/automation/src/domain/Deduplication";
import { DeduplicationRepository } from "../../packages/automation/src/server/database/DeduplicationRepository";

const makeRuntime = (url: string) =>
  ManagedRuntime.make(
    DeduplicationRepository.layer.pipe(
      Layer.provideMerge(PgClient.layer({ url: Redacted.make(url), maxConnections: 4 })),
    ),
  );

let runtime: ReturnType<typeof makeRuntime>;
let pg: { url: string; migrate: () => void; close: () => Promise<void> };

beforeAll(async () => {
  const { localPostgres } = await import(new URL("../fixtures/postgres.mjs", import.meta.url).href);
  const directory = await mkdtemp("/tmp/automation-deduplication-");
  pg = await localPostgres(directory);
  pg.migrate();
  runtime = makeRuntime(pg.url);
}, 30_000);

afterAll(async () => {
  try {
    if (runtime) await runtime.dispose();
  } finally {
    if (pg) await pg.close();
  }
});

const distinct = { _tag: "Distinct", reason: null } as const;
const duplicate = (canonicalId: string) => ({
  _tag: "Duplicate" as const,
  canonicalId,
  matchedBy: "semantic" as const,
  reason: null,
});
const input = (scope: string, id: string, sourceId = id): DeduplicationInput => ({
  scope,
  id,
  sourceId,
  context: { summary: `Synthetic cause ${id}` },
});
const event = (id: string, key = id) => ({
  id,
  source: "synthetic" as const,
  input: { key, title: `Synthetic ${key}`, evidence: { cause: "synthetic failure" } },
});
const pipeline = (
  compare?: (comparison: Source.Comparison) => Effect.Effect<string | null, DeduplicationError>,
  executions: string[] = [],
) =>
  Automation.make({
    name: "regression",
    scope: randomUUID(),
    sources: [
      Source.make({
        _tag: "synthetic",
        schema: Source.Issue,
        issue: (issue) => issue,
        ...(compare ? { dedupeIssue: compare } : {}),
      }),
    ],
    workflow: (issue) =>
      Effect.sync(() => {
        executions.push(issue.id);
        return issue.id;
      }),
  });
const pending = Effect.fnUntraced(function* (incoming: DeduplicationInput) {
  const repository = yield* DeduplicationRepository;
  const snapshot = yield* repository.load(incoming);
  assert.ok(snapshot._tag === "Pending");
  return snapshot;
});
const retain = Effect.fnUntraced(function* (
  incoming: DeduplicationInput,
  decision: DeduplicationDecision,
) {
  const repository = yield* DeduplicationRepository;
  const snapshot = yield* pending(incoming);
  return yield* repository.accept(incoming, snapshot.revision, decision);
});

it("keeps decoding, policy and workflow requirements separate for mixed sources", async () => {
  class Decoder extends Context.Service<Decoder, string>()("test/SourceDecoder") {}
  class Workflow extends Context.Service<Workflow, string>()("test/SourceWorkflow") {}

  const plain = Source.make({ _tag: "plain", schema: Source.Issue, issue: (issue) => issue });
  const modeled = Source.make({
    _tag: "modeled",
    schema: Source.Issue,
    issue: (issue) => issue,
    dedupeIssue: SemanticPolicy.llm({ instructions: "Require the same cause." }),
  });
  const contextual = Source.make({
    _tag: "contextual",
    schema: Source.Issue.pipe(
      Schema.decodeTo(Source.Issue, {
        decode: SchemaGetter.transformEffect((issue) =>
          Effect.map(Decoder, (title) => ({ ...issue, title })),
        ),
        encode: SchemaGetter.passthrough(),
      }),
    ),
    issue: (issue) => issue,
    dedupeIssue: () => Effect.fail("policy unavailable" as const),
  });
  expectTypeOf<Effect.Services<ReturnType<typeof modeled.issue>>>().toEqualTypeOf<never>();
  expectTypeOf<
    Effect.Error<ReturnType<typeof modeled.issue>>
  >().toEqualTypeOf<Schema.SchemaError>();
  expectTypeOf<Effect.Services<ReturnType<typeof contextual.issue>>>().toEqualTypeOf<Decoder>();

  const mixed = Automation.make({
    name: "mixed",
    scope: randomUUID(),
    sources: [
      plain,
      modeled,
      contextual,
      Sources.cloudflareIssues(),
      Sources.sentry({
        providerInstance: "https://sentry.io",
        dedupeIssue: SemanticPolicy.llm({ instructions: "Require the same cause." }),
      }),
    ],
    workflow: () => Effect.as(Workflow, undefined),
  });
  const request = { id: "lookup", source: "contextual" as const, input: event("lookup").input };
  const lookup = mixed.lookup(request);
  const resolved = mixed.resolve(request);
  const run = mixed.run(request);
  expectTypeOf<Effect.Services<typeof lookup>>().toEqualTypeOf<DeduplicationRepository | Decoder>();
  expectTypeOf<Effect.Error<typeof lookup>>().toEqualTypeOf<
    DeduplicationError | Schema.SchemaError
  >();
  expectTypeOf<Effect.Services<typeof resolved>>().toEqualTypeOf<
    DeduplicationRepository | Decoder | LanguageModel.LanguageModel
  >();
  expectTypeOf<Effect.Error<typeof resolved>>().toEqualTypeOf<
    DeduplicationError | Schema.SchemaError | "policy unavailable"
  >();
  expectTypeOf<Effect.Services<typeof run>>().toEqualTypeOf<
    DeduplicationRepository | Decoder | LanguageModel.LanguageModel | Workflow
  >();
  expect(
    await runtime.runPromise(lookup.pipe(Effect.provideService(Decoder, "Decoded title"))),
  ).toBeUndefined();

  const withoutModel = Automation.make({
    name: "without-model",
    scope: randomUUID(),
    sources: [
      Source.make({ _tag: "inline", schema: Source.Issue, issue: (issue) => issue }),
      Sources.sentry({ providerInstance: "https://sentry.io" }),
      contextual,
    ],
    workflow: () => Effect.void,
  });
  expectTypeOf<Effect.Services<ReturnType<typeof withoutModel.run>>>().toEqualTypeOf<
    DeduplicationRepository | Decoder
  >();

  const noPolicy = Automation.make({
    name: "runtime-issues",
    scope: "owner/repository",
    sources: [Sources.cloudflareIssues()],
    workflow: () => Effect.void,
  });
  expectTypeOf<
    Effect.Services<ReturnType<typeof noPolicy.resolve>>
  >().toEqualTypeOf<DeduplicationRepository>();
  expectTypeOf<
    Effect.Services<ReturnType<typeof noPolicy.lookup>>
  >().toEqualTypeOf<DeduplicationRepository>();
  expectTypeOf<Effect.Error<ReturnType<typeof noPolicy.resolve>>>().toEqualTypeOf<
    DeduplicationError | Schema.SchemaError
  >();
  expectTypeOf<Effect.Error<ReturnType<typeof noPolicy.lookup>>>().toEqualTypeOf<
    DeduplicationError | Schema.SchemaError
  >();
});

it.each(["absent policy", "empty shortlist"] as const)(
  "accepts without a model: %s",
  async (mode) => {
    const automation = pipeline(
      mode === "empty shortlist"
        ? () => Effect.die("An empty shortlist must not invoke policy")
        : undefined,
    );
    if (mode === "absent policy") await runtime.runPromise(automation.run(event("first")));
    const result = await runtime.runPromise(automation.run(event("incoming")));
    expect(result.decision).toEqual(distinct);
    expect(result._tag).toBe("Completed");
  },
);

it.each(["canonical", null])(
  "retains semantic result %s and reuses it on replay",
  async (match) => {
    let comparisons = 0;
    const executions: string[] = [];
    const automation = pipeline(
      (comparison) =>
        Effect.sync(() => {
          comparisons++;
          expect(comparison.candidates.map((candidate) => candidate.id)).toEqual(["canonical"]);
          return match;
        }),
      executions,
    );
    await runtime.runPromise(automation.run(event("canonical")));
    const resolved = await runtime.runPromise(automation.run(event("incoming")));
    const expected = match === null ? distinct : duplicate(match);
    expect(resolved.decision).toEqual(expected);
    expect(resolved._tag).toBe(match === null ? "Completed" : "Duplicate");
    expect((await runtime.runPromise(automation.resolve(event("incoming")))).decision).toEqual(
      expected,
    );
    expect(await runtime.runPromise(automation.lookup(event("incoming")))).toEqual(expected);
    expect(comparisons).toBe(1);
    if (match !== null) {
      const recurrence = await runtime.runPromise(automation.run(event("recurrence", "incoming")));
      expect(recurrence.decision).toEqual({ ...duplicate(match), matchedBy: "identity" });
      expect(comparisons).toBe(1);
    }
    expect(executions).toEqual(match === null ? ["canonical", "incoming"] : ["canonical"]);
  },
);

it("rejects an unlisted semantic candidate without retaining it", async () => {
  const executions: string[] = [];
  const automation = pipeline(() => Effect.succeed("unlisted"), executions);
  await runtime.runPromise(automation.run(event("canonical")));
  const failure = await runtime.runPromise(automation.run(event("incoming")).pipe(Effect.flip));
  expect(failure).toMatchObject({ _tag: "DeduplicationError", reason: "invalid_decision" });
  expect(await runtime.runPromise(automation.lookup(event("incoming")))).toBeUndefined();
  expect(executions).toEqual(["canonical"]);
});

it("preserves a policy failure and its cause without admitting the incoming item", async () => {
  const failure = DeduplicationError.make({ reason: "policy", cause: { status: 503 } });
  const executions: string[] = [];
  const automation = pipeline(() => Effect.fail(failure), executions);
  await runtime.runPromise(automation.run(event("canonical")));
  expect(await runtime.runPromise(automation.run(event("incoming")).pipe(Effect.flip))).toBe(
    failure,
  );
  expect(await runtime.runPromise(automation.lookup(event("incoming")))).toBeUndefined();
  expect(executions).toEqual(["canonical"]);
});

it.each([
  ["NUL", "\u0000"],
  ["lone high surrogate", "\ud800"],
  ["lone low surrogate", "\udc00"],
])("rejects unreadable JSON containing %s without retaining a decision", async (_, character) => {
  await runtime.runPromise(
    Effect.gen(function* () {
      const repository = yield* DeduplicationRepository;
      const sql = (yield* SqlClient.SqlClient).withoutTransforms();
      const executions: string[] = [];
      const automation = pipeline(undefined, executions);
      const scope = JSON.stringify([automation.name, automation.scope]);
      const invalid = `failure${character}details`;
      for (const request of [
        { ...event("title"), input: { key: "title", title: invalid, evidence: {} } },
        {
          ...event("evidence"),
          input: { key: "evidence", title: "Evidence", evidence: { detail: invalid } },
        },
      ]) {
        expect(yield* automation.run(request).pipe(Effect.flip)).toMatchObject({
          _tag: "DeduplicationError",
          reason: "store",
          cause: { _tag: "SqlError" },
        });
        expect(yield* automation.lookup(request)).toBeUndefined();
      }
      const reasonInput = input(scope, "reason");
      expect(
        yield* repository
          .accept(reasonInput, "0", { ...distinct, reason: invalid })
          .pipe(Effect.flip),
      ).toMatchObject({ reason: "store", cause: { _tag: "SqlError" } });
      expect(yield* repository.lookup(reasonInput)).toBeUndefined();
      expect(
        yield* sql`SELECT id FROM automation_deduplication_inputs WHERE scope = ${scope}`,
      ).toEqual([]);
      expect(
        yield* sql`SELECT revision FROM automation_deduplication_scopes WHERE scope = ${scope}`,
      ).toEqual([]);
      expect(yield* pending(input(scope, "reader"))).toMatchObject({
        revision: "0",
        candidates: [],
      });
      expect(executions).toEqual([]);

      const valid = {
        ...event("valid"),
        input: {
          ...event("valid").input,
          title: "Valid pair \ud83d\ude80",
          evidence: { literalEscape: "\\u0000" },
        },
      };
      expect((yield* automation.run(valid))._tag).toBe("Completed");
      const snapshot = yield* pending(input(scope, "reader"));
      expect(snapshot.revision).toBe("1");
      expect(snapshot.candidates).toMatchObject([{ id: "valid", context: valid.input }]);
      expect((yield* automation.run(event("next")))._tag).toBe("Completed");
      expect(executions).toEqual(["valid", "next"]);
    }),
  );
});

it("retries a stale empty snapshot against the other committed canonical input", async () => {
  await runtime.runPromise(
    Effect.gen(function* () {
      const repository = yield* DeduplicationRepository;
      const bothLoaded = yield* Deferred.make<void>();
      const firstAccepted = yield* Deferred.make<void>();
      const snapshots: DeduplicationSnapshot[] = [];
      let conflicts = 0;
      let comparisons = 0;
      const executions: string[] = [];
      const automation = pipeline(
        (comparison) =>
          Effect.sync(() => {
            comparisons++;
            expect(comparison.candidates.map((candidate) => candidate.id)).toEqual(["canonical"]);
            return "canonical";
          }),
        executions,
      );
      // Gate the real repository calls; all snapshots, locks and writes remain PostgreSQL's.
      const gated = DeduplicationRepository.of({
        ...repository,
        load: Effect.fnUntraced(function* (incoming: DeduplicationInput) {
          const snapshot = yield* repository.load(incoming);
          snapshots.push(snapshot);
          if (snapshots.length === 2) yield* Deferred.succeed(bothLoaded, undefined);
          yield* Deferred.await(bothLoaded);
          return snapshot;
        }),
        accept: Effect.fnUntraced(function* (...args: Parameters<typeof repository.accept>) {
          if (args[0].id === "incoming") yield* Deferred.await(firstAccepted);
          const decision = yield* repository.accept(...args).pipe(
            Effect.tapError((error) =>
              Effect.sync(() => {
                if (error.reason === "conflict") conflicts++;
              }),
            ),
          );
          if (args[0].id === "canonical") yield* Deferred.succeed(firstAccepted, undefined);
          return decision;
        }),
      });
      const [first, second] = yield* Effect.all(
        [automation.run(event("canonical")), automation.run(event("incoming"))],
        { concurrency: "unbounded" },
      ).pipe(Effect.provideService(DeduplicationRepository, gated));
      expect(snapshots.slice(0, 2)).toEqual([
        { _tag: "Pending", revision: "0", identityMatch: null, candidates: [] },
        { _tag: "Pending", revision: "0", identityMatch: null, candidates: [] },
      ]);
      expect(first._tag).toBe("Completed");
      expect(second._tag).toBe("Duplicate");
      expect(second.decision).toEqual(duplicate("canonical"));
      expect(conflicts).toBe(1);
      expect(comparisons).toBe(1);
      expect(executions).toEqual(["canonical"]);
    }).pipe(Effect.timeout("5 seconds")),
  );
});

it("shortlists the newest 20 canonicals and resolves exact source aliases across all history", async () => {
  await runtime.runPromise(
    Effect.gen(function* () {
      const repository = yield* DeduplicationRepository;
      const root = input(randomUUID(), "00-root", "root-source");
      yield* retain(root, distinct);
      for (let index = 0; index < 20; index++)
        yield* retain(input(root.scope, `a-${String(index).padStart(2, "0")}`), distinct);
      const alias = input(root.scope, "alias", "alias-source");
      const before = yield* pending(alias);
      expect(before.candidates).toHaveLength(20);
      expect(before.candidates.map((candidate) => candidate.id)).toEqual(
        Array.from({ length: 20 }, (_, index) => `a-${String(19 - index).padStart(2, "0")}`),
      );
      expect(before.candidates.some((candidate) => candidate.id === root.id)).toBe(false);
      yield* retain(alias, duplicate(root.id));
      const recurrence = input(root.scope, "recurrence", alias.sourceId);
      const identity = yield* pending(recurrence);
      expect(identity.identityMatch).toBe(root.id);
      expect(identity.candidates).toEqual([]);
      expect(identity.revision).toBe(before.revision);
      expect((yield* pending(input(root.scope, "exact-root", root.sourceId))).identityMatch).toBe(
        root.id,
      );
      expect(yield* repository.accept(recurrence, "0", duplicate("unlisted"))).toEqual({
        ...duplicate(root.id),
        matchedBy: "identity",
      });
      expect((yield* pending(input(root.scope, "later"))).revision).toBe(before.revision);

      const sql = (yield* SqlClient.SqlClient).withoutTransforms();
      type Plan = { "Node Type": string; "Actual Loops": number; Plans?: Plan[] };
      for (const [incoming, loops] of [
        [input(root.scope, "new-source"), 1],
        [root, 0],
        [input(root.scope, "alias-recurrence", alias.sourceId), 0],
      ] as const) {
        let compiled: ReturnType<Statement.Statement<unknown>["compile"]> | undefined;
        yield* repository.load(incoming).pipe(
          Effect.provideService(Statement.CurrentTransformer, (statement) =>
            Effect.sync(() => {
              compiled = statement.compile();
              return statement;
            }),
          ),
        );
        assert.ok(compiled);
        const [query, parameters] = compiled;
        const [row] = yield* sql.unsafe<{ "QUERY PLAN": Array<{ Plan: Plan }> }>(
          `EXPLAIN (ANALYZE, FORMAT JSON) ${query}`,
          parameters,
        );
        const nodes = [row!["QUERY PLAN"][0]!.Plan];
        for (const node of nodes) nodes.push(...(node.Plans ?? []));
        expect(
          nodes
            .filter((node) => node["Node Type"] === "Aggregate")
            .map((node) => node["Actual Loops"]),
        ).toEqual([loops]);
      }
    }),
  );
});

it("serializes simultaneous exact identities and preserves immutable input/evidence on replay", async () => {
  await runtime.runPromise(
    Effect.gen(function* () {
      const repository = yield* DeduplicationRepository;
      const sql = (yield* SqlClient.SqlClient).withoutTransforms();
      const first = input(randomUUID(), "first", "same-source");
      const second = input(first.scope, "second", first.sourceId);
      const decisions = yield* Effect.all(
        [first, second].map((incoming) => repository.accept(incoming, "0", distinct)),
        { concurrency: "unbounded" },
      );
      expect(decisions.filter((decision) => decision._tag === "Distinct")).toHaveLength(1);
      expect(
        decisions.filter(
          (decision) => decision._tag === "Duplicate" && decision.matchedBy === "identity",
        ),
      ).toHaveLength(1);
      const changed = { ...first, sourceId: "changed" };
      for (const operation of [
        repository.lookup(changed).pipe(Effect.asVoid),
        repository.load(changed).pipe(Effect.asVoid),
        repository.accept(changed, "0", distinct).pipe(Effect.asVoid),
      ])
        expect(yield* operation.pipe(Effect.flip)).toMatchObject({ reason: "store" });
      expect(
        yield* repository.accept({ ...first, context: { replacement: true } }, "0", distinct),
      ).toEqual(decisions[0]);
      const [row] = yield* sql<{
        context: string;
      }>`SELECT context FROM automation_deduplication_inputs
      WHERE scope = ${first.scope} AND id = ${first.id}`;
      expect(JSON.parse(row!.context)).toEqual(first.context);
      expect((yield* pending(input(first.scope, "reader"))).revision).toBe("1");
    }),
  );
});

it.each(["\u0000", "\ud800", "\udfff"])(
  "retains identity precedence over unused non-Postgres proposal text %j",
  async (character) => {
    await runtime.runPromise(
      Effect.gen(function* () {
        const repository = yield* DeduplicationRepository;
        const root = input(randomUUID(), "root", "same-source");
        yield* retain(root, distinct);
        const proposed = duplicate(`bad${character}target`);
        expect(yield* repository.accept(root, "0", proposed)).toEqual(distinct);
        expect(
          yield* repository.accept(input(root.scope, "alias", root.sourceId), "0", proposed),
        ).toEqual({ ...duplicate(root.id), matchedBy: "identity" });
      }),
    );
  },
);

it("rejects invalid canonical targets, keeps scopes separate and joins caller transactions", async () => {
  await runtime.runPromise(
    Effect.gen(function* () {
      const repository = yield* DeduplicationRepository;
      const sql = (yield* SqlClient.SqlClient).withoutTransforms();
      const root = input(randomUUID(), "root");
      yield* retain(root, distinct);
      const alias = input(root.scope, "alias");
      yield* retain(alias, duplicate(root.id));
      const foreign = input(randomUUID(), "foreign");
      const self = input(root.scope, "self");
      for (const operation of [
        repository.accept(foreign, "0", duplicate(root.id)),
        retain(self, duplicate(self.id)),
        retain(input(root.scope, "chain"), duplicate(alias.id)),
      ])
        expect(yield* operation.pipe(Effect.flip)).toMatchObject({ reason: "invalid_decision" });
      expect(yield* repository.lookup(foreign)).toBeUndefined();
      expect(yield* repository.accept({ ...root, scope: foreign.scope }, "0", distinct)).toEqual(
        distinct,
      );
      const rolledBack = input(randomUUID(), "rollback");
      expect(
        yield* sql
          .withTransaction(
            retain(rolledBack, distinct).pipe(
              Effect.andThen(Effect.fail("abort outer transaction")),
            ),
          )
          .pipe(Effect.flip),
      ).toBe("abort outer transaction");
      expect(yield* repository.lookup(rolledBack)).toBeUndefined();
      expect((yield* pending(rolledBack)).revision).toBe("0");
    }),
  );
});

it("reads consistent snapshots during writes and preserves database error causes", async () => {
  await runtime.runPromise(
    Effect.gen(function* () {
      const repository = yield* DeduplicationRepository;
      const sql = (yield* SqlClient.SqlClient).withoutTransforms();
      const reader = input(randomUUID(), "reader");
      for (let index = 0; index < 6; index++) {
        const results: readonly [Extract<DeduplicationSnapshot, { _tag: "Pending" }>, unknown] =
          yield* Effect.all(
            [pending(reader), retain(input(reader.scope, `root-${index}`), distinct)],
            { concurrency: "unbounded" },
          );
        expect(Number(results[0].revision)).toBe(results[0].candidates.length);
      }
      yield* sql`ALTER TABLE automation_deduplication_inputs RENAME TO unavailable_inputs`;
      const failed = yield* repository
        .lookup(reader)
        .pipe(
          Effect.flip,
          Effect.ensuring(
            sql`ALTER TABLE unavailable_inputs RENAME TO automation_deduplication_inputs`.pipe(
              Effect.orDie,
            ),
          ),
        );
      expect(failed).toMatchObject({ reason: "store", cause: { _tag: "SqlError" } });
    }),
  );
});
