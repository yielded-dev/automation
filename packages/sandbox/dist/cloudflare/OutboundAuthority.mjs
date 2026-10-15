import { Context, Data, Effect, Layer, Option } from "effect";
//#region src/cloudflare/OutboundAuthority.ts
var OutboundAuthorityError = class extends Data.TaggedError("OutboundAuthorityError") {};
/** The owning host supplies outbound authority for this exact native turn. */
var OutboundAuthority = class extends Context.Service()("kommunikasie/sandbox-agent/OutboundAuthority") {
	/** Standalone sessions have no application host capability. */
	static noneLayer = Layer.succeed(this, { mint: () => Effect.succeed(Option.none()) });
};
//#endregion
export { OutboundAuthority, OutboundAuthorityError };
