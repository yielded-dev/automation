import { Clock, Context, Effect, Layer, Redacted, Schema, Semaphore } from "effect";
import { DurableObjectContainer, DurableObjectState, Worker, WorkerEnvironment } from "effect-cf";
import { HttpServerError } from "effect/http";

import { reportSanitizedCause } from "../observability/RuntimeDiagnostics";

import { ContainerState, RepositoryAuthorization, OutboundProps, error } from "./ContainerState";

export interface SandboxAgentContainerOptions<
  Environment extends Cloudflare.Env,
  EventServices = never,
  EventError = never,
> {
  readonly eventLayer?: Layer.Layer<EventServices, EventError, WorkerEnvironment>;
  readonly namespaceBinding: string;
  readonly outboundEntrypoint?: string;
  readonly capacityBinding?: string;
  readonly imageKey?: string;
  readonly idleMillis?: number;
  /** Allow direct public internet access at container start. Defaults to false.
   * Explicit outbound interceptors still apply to their configured hosts.
   */
  readonly enableInternet?: boolean;
  readonly backupBucketBinding?: string;
  readonly openAiApiKey?: (environment: Environment) => Redacted.Redacted<string>;
  readonly openAiFetch?: (
    request: Request,
    environment: Environment,
    context: { readonly containerId: string; readonly params?: unknown },
  ) => Response | Promise<Response> | undefined;
  readonly hostOperationFetch?: (
    request: Request,
    environment: Environment,
    context: { readonly containerId: string; readonly params?: unknown },
  ) => Response | Promise<Response>;
  readonly sentryMcpHostname?: string;
  readonly sentryMcpFetch?: (
    request: Request,
    environment: Environment,
    context: { readonly containerId: string; readonly params?: unknown },
  ) => Response | Promise<Response>;
  readonly allowedHosts: (environment: Environment) => ReadonlyArray<string>;
  readonly environmentVariables?: Readonly<Record<string, string>>;
}
/** Export as SandboxOutbound. Only host-created props can grant outbound authority. */
export const makeSandboxAgentOutbound = <
  Environment extends Cloudflare.Env,
  EventServices = never,
  EventError = never,
>(
  options: Omit<
    SandboxAgentContainerOptions<Environment, EventServices, EventError>,
    "eventLayer"
  > & {
    readonly eventLayer?: Layer.Layer<
      EventServices,
      EventError,
      WorkerEnvironment | Worker.ExecutionContext
    >;
  },
) =>
  Worker.make(Layer.empty, {
    eventLayer: options.eventLayer ?? Layer.empty,
    fetch: Effect.gen(function* () {
      const request = yield* Worker.NativeRequest;
      const context = yield* Worker.ExecutionContext;
      const environment = (yield* WorkerEnvironment) as Environment;
      // oxlint-disable-next-line no-restricted-properties -- native entrypoint props are untyped.
      const props = yield* Schema.decodeUnknownEffect(OutboundProps)(context.props);
      const url = new URL(request.url);
      const denied = () => new Response(null, { status: 403 });

      if (url.protocol !== "https:" || url.username || url.password) return denied();
      if (url.hostname === HOST_OPERATION_HOST) {
        const forward = options.hostOperationFetch;

        if (!forward) return denied();

        return yield* Effect.tryPromise(() =>
          Promise.resolve(
            forward(request, environment, {
              containerId: props.containerId,
              params: props.authority,
            }),
          ),
        );
      }
      if (url.hostname === OPENAI_HOST) {
        const overridden = options.openAiFetch?.(request, environment, {
          containerId: props.containerId,
          params: props.authority,
        });

        if (overridden !== undefined)
          return yield* Effect.tryPromise(() => Promise.resolve(overridden));
        if (!options.openAiApiKey) return denied();
        const headers = new Headers(request.headers);

        headers.delete("cookie");
        headers.set("authorization", `Bearer ${Redacted.value(options.openAiApiKey(environment))}`);

        return yield* Effect.tryPromise(() =>
          fetch(new Request(request, { headers, redirect: "manual" })),
        );
      }
      if (url.hostname === options.sentryMcpHostname) {
        const forward = options.sentryMcpFetch;

        if (!forward) return denied();

        return yield* Effect.tryPromise(() =>
          Promise.resolve(
            forward(request, environment, {
              containerId: props.containerId,
              params: props.authority,
            }),
          ),
        );
      }
      if (!options.allowedHosts(environment).includes(url.hostname)) return denied();
      const now = yield* Clock.currentTimeMillis;
      const authorization = props.repositoryAuthorizations.find((entry) => {
        const remote = new URL(entry.remote);

        return (
          entry.expiresAt > now &&
          remote.origin === url.origin &&
          ["/info/refs", "/git-upload-pack", "/git-receive-pack"].some(
            (suffix) => url.pathname === `${remote.pathname}${suffix}`,
          )
        );
      });
      const headers = new Headers(request.headers);

      headers.delete("authorization");
      headers.delete("cookie");
      if (authorization) headers.set("authorization", authorization.credential);

      return yield* Effect.tryPromise(() =>
        fetch(new Request(request, { headers, redirect: "manual" })),
      );
    }).pipe(
      Effect.catchCause((cause) =>
        reportSanitizedCause(cause, { operation: "SandboxOutbound.fetch" }).pipe(
          // Own failure translation here: HttpEffect's reporting cause also contains
          // its synthesized response, which must not become a second captured defect.
          Effect.andThen(HttpServerError.causeResponse(cause)),
          Effect.map(([response]) => response),
        ),
      ),
    ),
  });

export const HOST_OPERATION_HOST = "sandbox-task.kommunikasie.invalid";
export const OPENAI_HOST = "api.openai.com";

const make = Effect.gen(function* () {
  const container = yield* DurableObjectContainer.DurableObjectContainer;
  const state = yield* DurableObjectState.DurableObjectState;

  const { read, identity, boot, settings: options } = yield* ContainerState;
  const repositoryAuthorizations = new Map<
    string,
    typeof RepositoryAuthorization.Type & {
      readonly instanceId: string;
      readonly grantId: string;
    }
  >();
  const outbound = yield* Semaphore.make(1);
  const configureOutbound = Effect.gen(function* () {
    const authority = (yield* state.storage.kv.get("outbound-authority")) ?? null;
    const current = yield* identity;
    const now = yield* Clock.currentTimeMillis;

    for (const [remote, authorization] of repositoryAuthorizations)
      if (authorization.instanceId !== current?.instanceId || authorization.expiresAt <= now)
        repositoryAuthorizations.delete(remote);
    // Each operation family has its own host-created outbound capability.
    const exports = state.raw.exports as unknown as Record<
      string,
      (input: { props: OutboundProps }) => Fetcher
    >;
    const entrypoint = exports[options.outboundEntrypoint ?? "SandboxOutbound"];

    if (!entrypoint) return yield* error("outbound", "Outbound entrypoint is not configured");
    const fetcher = entrypoint({
      props: {
        containerId: state.id.toString(),
        authority,
        repositoryAuthorizations: [...repositoryAuthorizations.values()],
      },
    });

    for (const host of new Set([
      OPENAI_HOST,
      ...(options.hostOperationEnabled ? [HOST_OPERATION_HOST] : []),
      ...(options.sentryMcpHostname === undefined ? [] : [options.sentryMcpHostname]),
      ...options.allowedHosts,
    ]))
      yield* container.interceptOutboundHttps(host, fetcher);
  }).pipe(outbound.withPermits(1), Effect.withSpan("SandboxContainer.configureOutbound"));

  return {
    configureOutboundAuthority: (authority: unknown) =>
      state.storage.kv.put("outbound-authority", authority).pipe(
        Effect.andThen(
          Effect.gen(function* () {
            if (yield* container.running) yield* configureOutbound;
          }),
        ),
      ),
    setRepositoryAuthorization: (
      remote: string,
      credential: string | null,
      expiresAt: number | null,
      instanceId: string,
      grantId: string,
    ) =>
      boot.withPermits(1)(
        Effect.gen(function* () {
          const url = new URL(remote);

          if (
            url.protocol !== "https:" ||
            url.username ||
            url.password ||
            url.search ||
            url.hash ||
            !options.allowedHosts.includes(url.hostname) ||
            !url.pathname.endsWith(".git") ||
            !instanceId ||
            !grantId
          )
            return yield* error("repositoryAuthorization", "Repository authority is not allowed");
          const running = yield* container.running;
          const now = yield* Clock.currentTimeMillis;
          const changed = yield* state.storage.transactionSync(() =>
            Effect.gen(function* () {
              const current = yield* identity;
              const stopping = yield* read("stopping-instance", Schema.String);

              if (current?.instanceId !== instanceId || stopping === instanceId) {
                if (credential === null) return false;

                return yield* error(
                  "repositoryAuthorization",
                  "Repository authority belongs to a retired container",
                );
              }
              const previous = repositoryAuthorizations.get(remote);

              if (credential === null) {
                // Reapply an empty/current map after a lost native installation
                // acknowledgment, while a newer grant remains untouched.
                if (previous === undefined) return true;
                if (previous?.instanceId !== instanceId || previous.grantId !== grantId)
                  return false;
                repositoryAuthorizations.delete(remote);

                return true;
              }
              if (!running || expiresAt === null || !Number.isFinite(expiresAt) || expiresAt <= now)
                return yield* error(
                  "repositoryAuthorization",
                  "Repository authority has no live operation deadline",
                );
              if (
                previous?.instanceId === instanceId &&
                previous.expiresAt > now &&
                (previous.grantId !== grantId ||
                  previous.credential !== credential ||
                  previous.expiresAt !== expiresAt)
              )
                return yield* error(
                  "repositoryAuthorization",
                  "Repository authority is owned by another operation",
                );
              repositoryAuthorizations.set(remote, {
                remote,
                credential,
                expiresAt,
                instanceId,
                grantId,
              });

              return true;
            }),
          );

          if (changed && running) yield* configureOutbound;
        }),
      ),
    configureOutbound,
    clear: Effect.sync(() => repositoryAuthorizations.clear()),
  };
});

export class ContainerOutbound extends Context.Service<
  ContainerOutbound,
  Effect.Success<typeof make>
>()("kommunikasie/ContainerOutbound") {
  static readonly layer = Layer.effect(ContainerOutbound, make);
}
