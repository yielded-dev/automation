import { Context } from "effect";
//#region src/server/RunConfig.ts
/** The host selects its resource namespace and GitHub installation. */
var RunConfig = class extends Context.Service()("@reve/repairs/RunConfig") {};
//#endregion
export { RunConfig };
