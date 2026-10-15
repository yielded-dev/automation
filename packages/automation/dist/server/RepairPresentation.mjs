import { Context } from "effect";
//#region src/server/RepairPresentation.ts
/** The host retains reviewed media, sanitizes content, and renders its PR presentation. */
var RepairPresentation = class extends Context.Service()("@reve/repairs/RepairPresentation") {};
//#endregion
export { RepairPresentation };
