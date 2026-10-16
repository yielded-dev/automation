import { Effect, Schema } from "effect";
import { Hex } from "effect/encoding";

/**
 * Deterministic per-session resource naming. The URL-safe session id is
 * hashed before becoming resource names, so arbitrary ids map onto bounded,
 * charset-safe sandbox and fork identifiers.
 */

export class ResourceHashError extends Schema.TaggedError<ResourceHashError>()(
  "ResourceHashError",
  {
    cause: Schema.Defect(),
  },
) {}

export const resourceHash = Effect.fn("resource-names.resourceHash")(function* (sessionId: string) {
  const digest = yield* Effect.tryPromise({
    try: () => crypto.subtle.digest("SHA-256", new TextEncoder().encode(sessionId)),
    catch: (cause) => ResourceHashError.make({ cause }),
  });

  return Hex.encode(new Uint8Array(digest)).slice(0, 16);
});

export const sandboxNameFor = (hash: string): string => `agent-${hash}`;
export const scopedSandboxName = (name: string, namespace?: string): string =>
  namespace === undefined ? name : `${namespace}-${name}`;
export const forkNameFor = (hash: string): string => `agent-${hash}`;

/**
 * The session's deterministic Artifacts fork and Sandbox names in one step.
 * A host holding the same bindings the kernel does reaches a session's fork
 * and container by these names — the read-only side-band the adoption plan
 * relies on — without composing the hash and the per-resource namer itself.
 */
export const sessionResourceNames = Effect.fn("resource-names.sessionResourceNames")(function* (
  sessionId: string,
  sandboxNamespace?: string,
) {
  const hash = yield* resourceHash(sessionId);

  return {
    forkName: forkNameFor(hash),
    sandboxName: scopedSandboxName(sandboxNameFor(hash), sandboxNamespace),
  } as const;
});
