import { Deferred, Effect, Ref } from "effect";
import { expect, it } from "vite-plus/test";

import {
  DeduplicationError,
  type DeduplicationDecision,
  type DeduplicationInput,
  type DeduplicationSnapshot,
} from "../../src/domain/Deduplication";
import {
  deduplicate,
  DeduplicationPolicy,
  DeduplicationStore,
} from "../../src/server/Deduplication";

const canonical: DeduplicationInput = {
  id: "canonical",
  scope: "project-1",
  sourceId: "source-1",
  context: { summary: "Request processing failed" },
};
const incoming: DeduplicationInput = { ...canonical, id: "incoming", sourceId: "source-2" };
const distinct: DeduplicationDecision = { _tag: "Distinct", reason: null };
type Accepted = { input: DeduplicationInput; decision: DeduplicationDecision };
const canonicalId = ({ input, decision }: Accepted) =>
  decision._tag === "Distinct" ? input.id : decision.canonicalId;
const unusedPolicy = DeduplicationPolicy.of({
  compare: () => Effect.die("Unexpected semantic comparison"),
});

// A pure host: the scoped record count is its revision, including new canonical records.
const fixture = Effect.fnUntraced(function* (initial: ReadonlyArray<Accepted> = []) {
  const records = yield* Ref.make(initial);
  const store = DeduplicationStore.of({
    load: (input) =>
      Ref.get(records).pipe(
        Effect.map((saved): DeduplicationSnapshot => {
          const scoped = saved.filter((record) => record.input.scope === input.scope);
          const retained = scoped.find((record) => record.input.id === input.id);
          if (retained) return { _tag: "Retained", decision: retained.decision };
          const identity = scoped.find((record) => record.input.sourceId === input.sourceId);
          return {
            _tag: "Pending",
            revision: String(scoped.length),
            identityMatch: identity ? canonicalId(identity) : null,
            candidates: scoped
              .filter((record) => record.decision._tag === "Distinct")
              .map(({ input: { id, sourceId, context } }) => ({ id, sourceId, context })),
          };
        }),
      ),
    accept: (input, revision, decision) =>
      Ref.modify(
        records,
        (
          saved,
        ): [Effect.Effect<DeduplicationDecision, DeduplicationError>, ReadonlyArray<Accepted>] => {
          const scoped = saved.filter((record) => record.input.scope === input.scope);
          const retained = scoped.find((record) => record.input.id === input.id);
          if (retained) return [Effect.succeed(retained.decision), saved];
          const identity = scoped.find((record) => record.input.sourceId === input.sourceId);
          if (
            revision !== String(scoped.length) ||
            (identity &&
              (decision._tag !== "Duplicate" || decision.canonicalId !== canonicalId(identity))) ||
            (decision._tag === "Duplicate" &&
              !scoped.some(
                (record) =>
                  record.decision._tag === "Distinct" && record.input.id === decision.canonicalId,
              ))
          )
            return [Effect.fail(DeduplicationError.make({ reason: "conflict" })), saved];
          return [Effect.succeed(decision), [...saved, { input, decision }]];
        },
      ).pipe(Effect.flatten),
  });
  return { store, saved: Ref.get(records) };
});

it("resolves an exact source alias outside the semantic shortlist without consulting policy", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const host = yield* fixture([
        { input: canonical, decision: distinct },
        {
          input: incoming,
          decision: {
            _tag: "Duplicate",
            canonicalId: canonical.id,
            matchedBy: "semantic",
            reason: "Same failed request",
          },
        },
        {
          input: { ...canonical, id: "unrelated", sourceId: "source-3" },
          decision: distinct,
        },
      ]);
      const delivery = { ...incoming, id: "redelivery" };
      const decision = yield* deduplicate(delivery).pipe(
        Effect.provideService(DeduplicationStore, {
          ...host.store,
          load: (input) =>
            host.store.load(input).pipe(
              Effect.map((snapshot) =>
                snapshot._tag === "Pending"
                  ? {
                      ...snapshot,
                      candidates: snapshot.candidates.filter(
                        (candidate) => candidate.id !== canonical.id,
                      ),
                    }
                  : snapshot,
              ),
            ),
        }),
        Effect.provideService(DeduplicationPolicy, unusedPolicy),
      );
      expect(decision).toEqual({
        _tag: "Duplicate",
        canonicalId: canonical.id,
        matchedBy: "identity",
        reason: null,
      });
      expect(yield* host.saved).toHaveLength(4);
      expect(yield* host.store.load(delivery)).toEqual({ _tag: "Retained", decision });
    }),
  );
});

it.each(["absent policy", "empty shortlist"] as const)(
  "accepts without a model: %s",
  async (mode) => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const initial = mode === "absent policy" ? [{ input: canonical, decision: distinct }] : [];
        const host = yield* fixture(initial);
        const resolve = deduplicate(incoming).pipe(
          Effect.provideService(DeduplicationStore, host.store),
        );
        const decision = yield* mode === "absent policy"
          ? resolve
          : resolve.pipe(Effect.provideService(DeduplicationPolicy, unusedPolicy));
        expect(decision).toEqual(distinct);
        expect(yield* host.saved).toEqual([...initial, { input: incoming, decision: distinct }]);
      }),
    );
  },
);

it.each([canonical.id, null])(
  "retains semantic result %s and reuses it on replay",
  async (duplicateOf) => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const host = yield* fixture([{ input: canonical, decision: distinct }]);
        const reason = duplicateOf === null ? "Different failed request" : "Same failed request";
        const expected: DeduplicationDecision =
          duplicateOf === null
            ? { _tag: "Distinct", reason }
            : { _tag: "Duplicate", canonicalId: canonical.id, matchedBy: "semantic", reason };
        let comparisons = 0;
        const decision = yield* deduplicate(incoming).pipe(
          Effect.provideService(DeduplicationStore, host.store),
          Effect.provideService(DeduplicationPolicy, {
            compare: (request) =>
              Effect.sync(() => {
                comparisons++;
                expect(request).toEqual({
                  incoming,
                  candidates: [
                    { id: canonical.id, sourceId: canonical.sourceId, context: canonical.context },
                  ],
                });
                return { duplicateOf, reason };
              }),
          }),
        );
        expect(decision).toEqual(expected);
        const replay = yield* deduplicate(incoming).pipe(
          Effect.provideService(DeduplicationStore, {
            load: host.store.load,
            accept: () => Effect.die("A retained decision must not be accepted again"),
          }),
          Effect.provideService(DeduplicationPolicy, unusedPolicy),
        );
        expect(replay).toEqual(expected);
        expect(comparisons).toBe(1);
        expect(yield* host.saved).toEqual([
          { input: canonical, decision: distinct },
          { input: incoming, decision: expected },
        ]);
      }),
    );
  },
);

it("rejects a semantic selection outside the supplied candidates without retaining it", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const initial = [{ input: canonical, decision: distinct }];
      const host = yield* fixture(initial);
      const failure = yield* deduplicate(incoming).pipe(
        Effect.provideService(DeduplicationStore, host.store),
        Effect.provideService(DeduplicationPolicy, {
          compare: () => Effect.succeed({ duplicateOf: "unlisted", reason: "Unverified match" }),
        }),
        Effect.flip,
      );
      expect(failure).toMatchObject({ _tag: "DeduplicationError", reason: "invalid_decision" });
      expect(yield* host.saved).toEqual(initial);
    }),
  );
});

it("preserves a policy failure and its cause without admitting the incoming item", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const initial = [{ input: canonical, decision: distinct }];
      const host = yield* fixture(initial);
      const failure = DeduplicationError.make({ reason: "policy", cause: { status: 503 } });
      const observed = yield* deduplicate(incoming).pipe(
        Effect.provideService(DeduplicationStore, host.store),
        Effect.provideService(DeduplicationPolicy, { compare: () => Effect.fail(failure) }),
        Effect.flip,
      );
      expect(observed).toBe(failure);
      expect(yield* host.saved).toEqual(initial);
    }),
  );
});

it("conflicts when another ID becomes canonical after an empty read, then deduplicates on retry", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const host = yield* fixture();
      const bothLoaded = yield* Deferred.make<void>();
      const firstAccepted = yield* Deferred.make<void>();
      const snapshots: DeduplicationSnapshot[] = [];
      let comparisons = 0;
      const store = DeduplicationStore.of({
        load: Effect.fnUntraced(function* (input: DeduplicationInput) {
          const snapshot = yield* host.store.load(input);
          snapshots.push(snapshot);
          if (snapshots.length === 2) yield* Deferred.succeed(bothLoaded, undefined);
          yield* Deferred.await(bothLoaded);
          return snapshot;
        }),
        accept: Effect.fnUntraced(function* (
          input: DeduplicationInput,
          revision: string,
          decision: DeduplicationDecision,
        ) {
          if (input.id === incoming.id) yield* Deferred.await(firstAccepted);
          const retained = yield* host.store.accept(input, revision, decision);
          if (input.id === canonical.id) yield* Deferred.succeed(firstAccepted, undefined);
          return retained;
        }),
      });
      const policy = DeduplicationPolicy.of({
        compare: (request) =>
          Effect.sync(() => {
            comparisons++;
            expect(request.candidates.map((candidate) => candidate.id)).toEqual([canonical.id]);
            return { duplicateOf: canonical.id, reason: "Same failed request" };
          }),
      });
      const resolve = (input: DeduplicationInput) =>
        deduplicate(input).pipe(
          Effect.provideService(DeduplicationStore, store),
          Effect.provideService(DeduplicationPolicy, policy),
        );
      const [first, conflict] = yield* Effect.all(
        [resolve(canonical), resolve(incoming).pipe(Effect.flip)],
        { concurrency: "unbounded" },
      );
      expect(snapshots).toEqual([
        { _tag: "Pending", revision: "0", identityMatch: null, candidates: [] },
        { _tag: "Pending", revision: "0", identityMatch: null, candidates: [] },
      ]);
      expect(first).toEqual(distinct);
      expect(conflict).toMatchObject({ _tag: "DeduplicationError", reason: "conflict" });
      expect(comparisons).toBe(0);
      expect(yield* host.saved).toEqual([{ input: canonical, decision: distinct }]);

      const retried = yield* resolve(incoming);
      expect(retried).toEqual({
        _tag: "Duplicate",
        canonicalId: canonical.id,
        matchedBy: "semantic",
        reason: "Same failed request",
      });
      expect(comparisons).toBe(1);
      expect(yield* host.saved).toEqual([
        { input: canonical, decision: distinct },
        { input: incoming, decision: retried },
      ]);
    }).pipe(Effect.timeout("3 seconds")),
  );
});
