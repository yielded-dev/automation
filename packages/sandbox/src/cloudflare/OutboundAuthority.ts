import { Context, Data, Effect, Layer, Option } from "effect";

export class OutboundAuthorityError extends Data.TaggedError("OutboundAuthorityError")<{
  readonly cause: unknown;
}> {}

/** The owning host supplies outbound authority for this exact native turn. */
export class OutboundAuthority extends Context.Service<
  OutboundAuthority,
  {
    readonly mint: (turn: {
      readonly sessionId: string;
      readonly turnId: string;
      readonly instanceId: string;
    }) => Effect.Effect<Option.Option<unknown>, OutboundAuthorityError>;
  }
>()("kommunikasie/sandbox-agent/OutboundAuthority") {
  /** Standalone sessions have no application host capability. */
  static readonly noneLayer = Layer.succeed(this, { mint: () => Effect.succeedNone });
}
