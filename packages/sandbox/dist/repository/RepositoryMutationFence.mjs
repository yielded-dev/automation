import { Context, Schema } from "effect";
//#region src/repository/RepositoryMutationFence.ts
var RepositoryMutationFenceFailure = class extends Schema.TaggedError()("RepositoryMutationFenceFailure", {
	reason: Schema.Literals(["stopped", "unavailable"]),
	message: Schema.String,
	cause: Schema.optionalKey(Schema.Defect())
}) {};
/** The operation owner rechecks its durable authority immediately before a remote mutation. */
var RepositoryMutationFence = class extends Context.Service()("kommunikasie/RepositoryMutationFence") {};
//#endregion
export { RepositoryMutationFence, RepositoryMutationFenceFailure };
