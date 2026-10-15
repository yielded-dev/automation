import { Context, Schema } from "effect";
//#region src/server/RepairSandboxTaskClient.ts
var RepairSandboxTaskTransportError = class extends Schema.TaggedError()("RepairSandboxTaskTransportError", {
	operation: Schema.String,
	status: Schema.optionalKey(Schema.Int),
	cause: Schema.optionalKey(Schema.Defect())
}) {};
/** The host connects its existing sandbox runtime, locally or over authenticated transport. */
var RepairSandboxTaskClient = class extends Context.Service()("kommunikasie/RepairSandboxTaskClient") {};
//#endregion
export { RepairSandboxTaskClient, RepairSandboxTaskTransportError };
