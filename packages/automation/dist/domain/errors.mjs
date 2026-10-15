import { Schema } from "effect";
//#region src/domain/errors.ts
var ConflictError = class extends Schema.TaggedError()("RepairConflict", { message: Schema.String }) {};
var ServiceUnavailableError = class extends Schema.TaggedError()("RepairUnavailable", { message: Schema.String }) {};
//#endregion
export { ConflictError, ServiceUnavailableError };
