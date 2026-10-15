import { Context } from "effect";
//#region src/SessionSandbox.ts
/** One resolved native container capability for the current workspace operation. */
var SessionSandbox = class extends Context.Service()("sandbox-agent/SessionSandbox") {};
//#endregion
export { SessionSandbox };
