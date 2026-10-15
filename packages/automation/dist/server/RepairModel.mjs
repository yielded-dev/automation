import { Context } from "effect";
//#region src/server/RepairModel.ts
/** The host owns supported model capabilities and provider accounting. */
var RepairModel = class extends Context.Service()("@reve/repairs/RepairModel") {};
//#endregion
export { RepairModel };
