import { Context, Effect, Layer, Schema } from "effect";
//#region src/cloudflare/RepairSandboxPreparation.ts
var RepairSandboxPreparationError = class extends Schema.TaggedError()("RepairSandboxPreparationError", { cause: Schema.Defect() }) {};
/** Join host-owned preparation before the first workspace initialization, never during receipt replay. */
var RepairSandboxPreparation = class extends Context.Service()("@reve/repairs/RepairSandboxPreparation") {
	static noopLayer = Layer.succeed(this, { prepare: () => Effect.void });
};
//#endregion
export { RepairSandboxPreparation, RepairSandboxPreparationError };
