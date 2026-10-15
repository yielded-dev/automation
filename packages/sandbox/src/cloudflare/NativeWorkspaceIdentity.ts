import { Context, Schema } from "effect";

/** Host-selected marker that grants execution ownership of a disposable workspace. */
export const NativeWorkspaceIdentityData = Schema.Struct({
  path: Schema.NonEmptyString,
  id: Schema.NonEmptyString,
});
export type NativeWorkspaceIdentityData = typeof NativeWorkspaceIdentityData.Type;

export const NativeWorkspaceIdentity = Context.Reference<NativeWorkspaceIdentityData | null>(
  "sandbox-agent/NativeWorkspaceIdentity",
  { defaultValue: () => null },
);
