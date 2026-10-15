import { Context } from "effect";
//#region src/server/RepairGithubConfig.ts
/** The host selects the installation; repository authority is supplied per operation. */
var RepairGithubConfig = class extends Context.Service()("@reve/repairs/RepairGithubConfig") {};
//#endregion
export { RepairGithubConfig };
