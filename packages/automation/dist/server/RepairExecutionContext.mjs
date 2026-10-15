import { Context } from "effect";
//#region src/server/RepairExecutionContext.ts
/** The authenticated run scope supplied once at an application operation boundary. */
var RepairExecutionContext = class extends Context.Service()("kommunikasie/RepairExecutionContext") {};
//#endregion
export { RepairExecutionContext };
