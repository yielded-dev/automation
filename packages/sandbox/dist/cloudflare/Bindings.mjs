import { SandboxAgentConfig } from "./Config.mjs";
import { CAPACITY_NAME, NativeSandboxNamespace, SandboxOperationError, decodeNativeReply, makeNativeSandbox, previewTokenHash } from "./NativeSandbox.mjs";
import { scopedSandboxName } from "./resource-names.mjs";
import { Cause, Context, Effect, Layer, Option } from "effect";
import { Artifacts } from "effect-cf";
//#region src/cloudflare/Bindings.ts
var SandboxAgentArtifacts = class extends Artifacts.make("sandbox-agent/Artifacts") {};
/** One namespace; all instance sizes and images share its admission authority. */
var Sandboxes = class Sandboxes extends Context.Service()("sandbox-agent/Sandboxes") {
	static layer = (options) => Layer.effect(Sandboxes, Effect.gen(function* () {
		const namespace = yield* NativeSandboxNamespace;
		const preservePreviewFailure = (operation) => (cause) => Effect.failCause(Cause.map(cause, (cause) => new SandboxOperationError({
			operation,
			cause
		})));
		return Sandboxes.of({
			resolvePreview: (token) => Effect.gen(function* () {
				if (!/^[a-f0-9]{32}$/.test(token)) return Option.none();
				const coordinator = yield* namespace.getByName(CAPACITY_NAME);
				const hash = yield* previewTokenHash(token);
				const reply = yield* namespace.call(coordinator, "resolvePreview", hash).pipe(Effect.catchCause(preservePreviewFailure("resolvePreview")));
				return Option.fromNullishOr(yield* decodeNativeReply("resolvePreview", reply));
			}),
			forwardPreview: (sandboxId, request) => Effect.gen(function* () {
				const id = yield* namespace.idFromString(sandboxId);
				const stub = yield* namespace.get(id);
				return yield* namespace.fetch(stub, request).pipe(Effect.catchCause(preservePreviewFailure("forwardPreview")));
			}),
			get: (name, start = {}) => makeNativeSandbox(name, start).pipe(Effect.provideService(NativeSandboxNamespace, namespace))
		});
	})).pipe(Layer.provide(NativeSandboxNamespace.layer(options)));
	static resolvePreview = (token) => Effect.flatMap(Sandboxes, (sandboxes) => sandboxes.resolvePreview(token));
	static forwardPreview = (sandboxId, request) => Effect.flatMap(Sandboxes, (sandboxes) => sandboxes.forwardPreview(sandboxId, request));
	static get = Effect.fnUntraced(function* (name, options) {
		return yield* (yield* Sandboxes).get(name, options);
	});
};
const sandboxFor = Effect.fn("sandboxFor")(function* (size, name, options = {}) {
	const config = yield* SandboxAgentConfig;
	return yield* Sandboxes.get(scopedSandboxName(name, config.sandboxNamespace), {
		...options,
		size
	});
});
//#endregion
export { SandboxAgentArtifacts, Sandboxes, sandboxFor };
