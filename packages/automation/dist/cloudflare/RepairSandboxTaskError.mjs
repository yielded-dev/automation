import { ConflictError } from "../domain/errors.mjs";
import { Cause, Effect, Schema } from "effect";
//#region src/cloudflare/RepairSandboxTaskError.ts
var RepairSandboxTaskError = class extends Schema.TaggedError()("RepairSandboxTaskError", { cause: Schema.Defect() }) {};
const preserveRepairSandboxFailure = (effect) => effect.pipe(Effect.catchCause((cause) => Effect.failCause(Cause.map(cause, (error) => Schema.is(ConflictError)(error) || Schema.is(RepairSandboxTaskError)(error) ? error : RepairSandboxTaskError.make({ cause: error })))));
//#endregion
export { RepairSandboxTaskError, preserveRepairSandboxFailure };
