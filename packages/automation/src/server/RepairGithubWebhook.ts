import { Effect, Redacted, Result, Schema } from "effect";
import { Hex } from "effect/encoding";

export class GithubWebhookCryptoError extends Schema.TaggedError<GithubWebhookCryptoError>()(
  "GithubWebhookCryptoError",
  {
    operation: Schema.Literals(["importKey", "verify"]),
    cause: Schema.Defect(),
  },
) {}

/** Verify the raw webhook body before decoding or accepting its event facts. */
export const verifyGithubSignature = Effect.fn("RepairGithub.verifySignature")(function* (
  body: string,
  signature: string | null,
  secret: Redacted.Redacted<string>,
) {
  if (signature === null || !/^sha256=[a-f0-9]{64}$/.test(signature)) return false;
  if (Redacted.value(secret).length === 0) return false;
  const bytes = Result.getOrUndefined(Hex.decode(signature.slice(7)));

  if (bytes === undefined) return false;
  const encoder = new TextEncoder();
  const key = yield* Effect.tryPromise({
    try: () =>
      crypto.subtle.importKey(
        "raw",
        encoder.encode(Redacted.value(secret)),
        { name: "HMAC", hash: "SHA-256" },
        false,
        ["verify"],
      ),
    catch: (cause) => GithubWebhookCryptoError.make({ operation: "importKey", cause }),
  });

  return yield* Effect.tryPromise({
    try: () => crypto.subtle.verify("HMAC", key, new Uint8Array(bytes), encoder.encode(body)),
    catch: (cause) => GithubWebhookCryptoError.make({ operation: "verify", cause }),
  });
});
