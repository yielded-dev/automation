import { Context, Schema } from "effect";
//#region src/cloudflare/NativeWorkspaceIdentity.ts
/** Host-selected marker that grants execution ownership of a disposable workspace. */
const NativeWorkspaceIdentityData = Schema.Struct({
	path: Schema.NonEmptyString,
	id: Schema.NonEmptyString
});
const NativeWorkspaceIdentity = Context.Reference("sandbox-agent/NativeWorkspaceIdentity", { defaultValue: () => null });
//#endregion
export { NativeWorkspaceIdentity, NativeWorkspaceIdentityData };
