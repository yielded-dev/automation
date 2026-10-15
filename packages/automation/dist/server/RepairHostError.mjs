import { Cause, Effect, Schema } from "effect";
//#region src/server/RepairHostError.ts
/** Original host failures stay attached until the public transport boundary reports them. */
var RepairHostError = class extends Schema.TaggedError()("RepairHostError", {
	operation: Schema.String,
	cause: Schema.Defect()
}) {};
const mapRepairHostError = (operation) => (effect) => effect.pipe(Effect.catchCause((cause) => Effect.failCause(Cause.map(cause, (error) => RepairHostError.make({
	operation,
	cause: error
})))));
//#endregion
export { RepairHostError, mapRepairHostError };
