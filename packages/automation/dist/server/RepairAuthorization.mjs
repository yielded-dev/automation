import { Context } from "effect";
//#region src/server/RepairAuthorization.ts
/** Product consent and repository policy remain authoritative in the host. */
var RepairAuthorization = class extends Context.Service()("@reve/repairs/RepairAuthorization") {};
//#endregion
export { RepairAuthorization };
