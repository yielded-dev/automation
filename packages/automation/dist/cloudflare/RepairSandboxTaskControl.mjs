import { Context, Effect, Layer, Ref, Semaphore } from "effect";
//#region src/cloudflare/RepairSandboxTaskControl.ts
var RepairSandboxTaskControl = class extends Context.Service()("@reve/repairs/RepairSandboxTaskControl") {
	static layer = Layer.effect(this, Effect.gen(function* () {
		return {
			lifecycle: yield* Semaphore.make(1),
			active: yield* Ref.make(void 0)
		};
	}));
};
//#endregion
export { RepairSandboxTaskControl };
