import { Schema } from "effect";
//#region src/server/sandbox-contracts/git.ts
/** Lowercase full Git object id (SHA-1 or SHA-256). */
const GitObjectId = Schema.Trim.pipe(Schema.check(Schema.makeFilter((value) => /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/.test(value), { expected: "a lowercase Git object id" })));
//#endregion
export { GitObjectId };
