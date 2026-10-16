import { Schema } from "effect";

/** Lowercase full Git object id (SHA-1 or SHA-256). */
export const GitObjectId = Schema.Trim.pipe(
  Schema.check(
    Schema.makeFilter((value) => /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/.test(value), {
      expected: "a lowercase Git object id",
    }),
  ),
);
