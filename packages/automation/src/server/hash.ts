import { Effect } from "effect";
import { Hex } from "effect/encoding";

export const contentHash = (value: string) =>
  Effect.promise(async () =>
    Hex.encode(
      new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value))),
    ),
  );
