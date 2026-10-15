import { Effect } from "effect";
import { Hex } from "effect/encoding";
//#region src/server/hash.ts
const contentHash = (value) => Effect.promise(async () => Hex.encode(new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value)))));
//#endregion
export { contentHash };
