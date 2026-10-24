import { Cause, Context, Crypto, Effect, Layer, Option, PlatformError } from "effect";
import { Artifacts } from "effect-cf";

import type { ContainerSize } from "../Contract";
import { SandboxAgentConfig } from "./Config";
import {
  CAPACITY_NAME,
  decodeNativeReply,
  makeNativeSandbox,
  NativeSandboxNamespace,
  previewTokenHash,
  type PreviewRoute,
  SandboxOperationError,
  type EnsureOptions,
  type NativeSandbox,
} from "./NativeSandbox";
import { scopedSandboxName } from "./resource-names";

/** Web Crypto implementation for Cloudflare composition roots. */
export const webCryptoLayer = Layer.succeed(
  Crypto.Crypto,
  Crypto.make({
    randomBytes: (size) => crypto.getRandomValues(new Uint8Array(size)),
    digest: (algorithm, data) =>
      Effect.tryPromise({
        try: async () =>
          new Uint8Array(await crypto.subtle.digest(algorithm, Uint8Array.from(data).buffer)),
        catch: (cause) =>
          PlatformError.systemError({ _tag: "Unknown", module: "Crypto", method: "digest", cause }),
      }),
  }),
);

export class SandboxAgentArtifacts extends Artifacts.make("sandbox-agent/Artifacts") {}

/** One namespace; all instance sizes and images share its admission authority. */
export class Sandboxes extends Context.Service<
  Sandboxes,
  {
    readonly resolvePreview: (
      token: string,
    ) => Effect.Effect<Option.Option<PreviewRoute>, SandboxOperationError>;
    readonly forwardPreview: (
      sandboxId: string,
      request: Request,
    ) => Effect.Effect<Response, SandboxOperationError>;
    readonly get: (name: string, options?: EnsureOptions) => Effect.Effect<NativeSandbox>;
  }
>()("sandbox-agent/Sandboxes") {
  static readonly layer = (options: { readonly binding: string }) =>
    Layer.effect(
      Sandboxes,
      Effect.gen(function* () {
        const namespace = yield* NativeSandboxNamespace;
        const preservePreviewFailure =
          (operation: string) =>
          <E>(cause: Cause.Cause<E>) =>
            Effect.failCause(
              Cause.map(cause, (cause) => new SandboxOperationError({ operation, cause })),
            );

        return Sandboxes.of({
          resolvePreview: (token) =>
            Effect.gen(function* () {
              if (!/^[a-f0-9]{32}$/.test(token)) return Option.none();
              const coordinator = yield* namespace.getByName(CAPACITY_NAME);
              const hash = yield* previewTokenHash(token);
              const reply = yield* namespace
                .call(coordinator, "resolvePreview", hash)
                .pipe(Effect.catchCause(preservePreviewFailure("resolvePreview")));

              return Option.fromNullishOr(yield* decodeNativeReply("resolvePreview", reply));
            }),
          forwardPreview: (sandboxId, request) =>
            Effect.gen(function* () {
              const id = yield* namespace.idFromString(sandboxId);
              const stub = yield* namespace.get(id);

              return yield* namespace
                .fetch(stub, request)
                .pipe(Effect.catchCause(preservePreviewFailure("forwardPreview")));
            }),
          get: (name, start = {}) =>
            makeNativeSandbox(name, start).pipe(
              Effect.provideService(NativeSandboxNamespace, namespace),
            ),
        });
      }),
    ).pipe(Layer.provide(NativeSandboxNamespace.layer(options)));
  static readonly resolvePreview = (token: string) =>
    Effect.flatMap(Sandboxes, (sandboxes) => sandboxes.resolvePreview(token));
  static readonly forwardPreview = (sandboxId: string, request: Request) =>
    Effect.flatMap(Sandboxes, (sandboxes) => sandboxes.forwardPreview(sandboxId, request));
  static readonly get = Effect.fnUntraced(function* (name: string, options?: EnsureOptions) {
    return yield* (yield* Sandboxes).get(name, options);
  });
}

export const sandboxFor = Effect.fn("sandboxFor")(function* (
  size: ContainerSize,
  name: string,
  options: EnsureOptions = {},
) {
  const config = yield* SandboxAgentConfig;

  return yield* Sandboxes.get(scopedSandboxName(name, config.sandboxNamespace), {
    ...options,
    size,
  });
});
