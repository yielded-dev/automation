import { Effect, Schema } from "effect";
import { Hex } from "effect/encoding";
//#region src/cloudflare/resource-names.ts
/**
* Deterministic per-session resource naming. The URL-safe session id is
* hashed before becoming resource names, so arbitrary ids map onto bounded,
* charset-safe sandbox and fork identifiers.
*/
var ResourceHashError = class extends Schema.TaggedError()("ResourceHashError", { cause: Schema.Defect() }) {};
const resourceHash = Effect.fn("resource-names.resourceHash")(function* (sessionId) {
	const digest = yield* Effect.tryPromise({
		try: () => crypto.subtle.digest("SHA-256", new TextEncoder().encode(sessionId)),
		catch: (cause) => ResourceHashError.make({ cause })
	});
	return Hex.encode(new Uint8Array(digest)).slice(0, 16);
});
const sandboxNameFor = (hash) => `agent-${hash}`;
const scopedSandboxName = (name, namespace) => namespace === void 0 ? name : `${namespace}-${name}`;
const forkNameFor = (hash) => `agent-${hash}`;
/**
* The session's deterministic Artifacts fork and Sandbox names in one step.
* A host holding the same bindings the kernel does reaches a session's fork
* and container by these names — the read-only side-band the adoption plan
* relies on — without composing the hash and the per-resource namer itself.
*/
const sessionResourceNames = Effect.fn("resource-names.sessionResourceNames")(function* (sessionId, sandboxNamespace) {
	const hash = yield* resourceHash(sessionId);
	return {
		forkName: forkNameFor(hash),
		sandboxName: scopedSandboxName(sandboxNameFor(hash), sandboxNamespace)
	};
});
//#endregion
export { ResourceHashError, forkNameFor, resourceHash, sandboxNameFor, scopedSandboxName, sessionResourceNames };
