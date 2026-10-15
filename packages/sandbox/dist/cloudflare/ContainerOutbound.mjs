import { reportSanitizedCause } from "../observability/RuntimeDiagnostics.mjs";
import { ContainerState, OutboundProps, error } from "./ContainerState.mjs";
import { Clock, Context, Effect, Layer, Redacted, Schema, Semaphore } from "effect";
import { HttpServerError } from "effect/http";
import { DurableObjectContainer, DurableObjectState, Worker, WorkerEnvironment } from "effect-cf";
//#region src/cloudflare/ContainerOutbound.ts
/** Export as SandboxOutbound. Only host-created props can grant outbound authority. */
const makeSandboxAgentOutbound = (options) => Worker.make(Layer.empty, {
	eventLayer: options.eventLayer ?? Layer.empty,
	fetch: Effect.gen(function* () {
		const request = yield* Worker.NativeRequest;
		const context = yield* Worker.ExecutionContext;
		const environment = yield* WorkerEnvironment;
		const props = yield* Schema.decodeUnknownEffect(OutboundProps)(context.props);
		const url = new URL(request.url);
		const denied = () => new Response(null, { status: 403 });
		if (url.protocol !== "https:" || url.username || url.password) return denied();
		if (url.hostname === "sandbox-task.kommunikasie.invalid") {
			const forward = options.hostOperationFetch;
			if (!forward) return denied();
			return yield* Effect.tryPromise(() => Promise.resolve(forward(request, environment, {
				containerId: props.containerId,
				params: props.authority
			})));
		}
		if (url.hostname === "api.openai.com") {
			const overridden = options.openAiFetch?.(request, environment, {
				containerId: props.containerId,
				params: props.authority
			});
			if (overridden !== void 0) return yield* Effect.tryPromise(() => Promise.resolve(overridden));
			if (!options.openAiApiKey) return denied();
			const headers = new Headers(request.headers);
			headers.delete("cookie");
			headers.set("authorization", `Bearer ${Redacted.value(options.openAiApiKey(environment))}`);
			return yield* Effect.tryPromise(() => fetch(new Request(request, {
				headers,
				redirect: "manual"
			})));
		}
		if (url.hostname === options.sentryMcpHostname) {
			const forward = options.sentryMcpFetch;
			if (!forward) return denied();
			return yield* Effect.tryPromise(() => Promise.resolve(forward(request, environment, {
				containerId: props.containerId,
				params: props.authority
			})));
		}
		if (!options.allowedHosts(environment).includes(url.hostname)) return denied();
		const now = yield* Clock.currentTimeMillis;
		const authorization = props.repositoryAuthorizations.find((entry) => {
			const remote = new URL(entry.remote);
			return entry.expiresAt > now && remote.origin === url.origin && [
				"/info/refs",
				"/git-upload-pack",
				"/git-receive-pack"
			].some((suffix) => url.pathname === `${remote.pathname}${suffix}`);
		});
		const headers = new Headers(request.headers);
		headers.delete("authorization");
		headers.delete("cookie");
		if (authorization) headers.set("authorization", authorization.credential);
		return yield* Effect.tryPromise(() => fetch(new Request(request, {
			headers,
			redirect: "manual"
		})));
	}).pipe(Effect.catchCause((cause) => reportSanitizedCause(cause, { operation: "SandboxOutbound.fetch" }).pipe(Effect.andThen(HttpServerError.causeResponse(cause)), Effect.map(([response]) => response))))
});
const HOST_OPERATION_HOST = "sandbox-task.kommunikasie.invalid";
const OPENAI_HOST = "api.openai.com";
const make = Effect.gen(function* () {
	const container = yield* DurableObjectContainer.DurableObjectContainer;
	const state = yield* DurableObjectState.DurableObjectState;
	const { read, identity, boot, settings: options } = yield* ContainerState;
	const repositoryAuthorizations = /* @__PURE__ */ new Map();
	const outbound = yield* Semaphore.make(1);
	const configureOutbound = Effect.gen(function* () {
		const authority = (yield* state.storage.kv.get("outbound-authority")) ?? null;
		const current = yield* identity();
		const now = yield* Clock.currentTimeMillis;
		for (const [remote, authorization] of repositoryAuthorizations) if (authorization.instanceId !== current?.instanceId || authorization.expiresAt <= now) repositoryAuthorizations.delete(remote);
		const entrypoint = state.raw.exports[options.outboundEntrypoint ?? "SandboxOutbound"];
		if (!entrypoint) return yield* error("outbound", "Outbound entrypoint is not configured");
		const fetcher = entrypoint({ props: {
			containerId: state.id.toString(),
			authority,
			repositoryAuthorizations: [...repositoryAuthorizations.values()]
		} });
		for (const host of /* @__PURE__ */ new Set([
			OPENAI_HOST,
			...options.hostOperationEnabled ? [HOST_OPERATION_HOST] : [],
			...options.sentryMcpHostname === void 0 ? [] : [options.sentryMcpHostname],
			...options.allowedHosts
		])) yield* container.interceptOutboundHttps(host, fetcher);
	}).pipe(outbound.withPermits(1), Effect.withSpan("SandboxContainer.configureOutbound"));
	return {
		configureOutboundAuthority: (authority) => state.storage.kv.put("outbound-authority", authority).pipe(Effect.andThen(Effect.gen(function* () {
			if (yield* container.running) yield* configureOutbound;
		}))),
		setRepositoryAuthorization: (remote, credential, expiresAt, instanceId, grantId) => boot.withPermits(1)(Effect.gen(function* () {
			const url = new URL(remote);
			if (url.protocol !== "https:" || url.username || url.password || url.search || url.hash || !options.allowedHosts.includes(url.hostname) || !url.pathname.endsWith(".git") || !instanceId || !grantId) return yield* error("repositoryAuthorization", "Repository authority is not allowed");
			const running = yield* container.running;
			const now = yield* Clock.currentTimeMillis;
			if ((yield* state.storage.transactionSync(() => Effect.gen(function* () {
				const current = yield* identity();
				const stopping = yield* read("stopping-instance", Schema.String);
				if (current?.instanceId !== instanceId || stopping === instanceId) {
					if (credential === null) return false;
					return yield* error("repositoryAuthorization", "Repository authority belongs to a retired container");
				}
				const previous = repositoryAuthorizations.get(remote);
				if (credential === null) {
					if (previous === void 0) return true;
					if (previous?.instanceId !== instanceId || previous.grantId !== grantId) return false;
					repositoryAuthorizations.delete(remote);
					return true;
				}
				if (!running || expiresAt === null || !Number.isFinite(expiresAt) || expiresAt <= now) return yield* error("repositoryAuthorization", "Repository authority has no live operation deadline");
				if (previous?.instanceId === instanceId && previous.expiresAt > now && (previous.grantId !== grantId || previous.credential !== credential || previous.expiresAt !== expiresAt)) return yield* error("repositoryAuthorization", "Repository authority is owned by another operation");
				repositoryAuthorizations.set(remote, {
					remote,
					credential,
					expiresAt,
					instanceId,
					grantId
				});
				return true;
			}))) && running) yield* configureOutbound;
		})),
		configureOutbound,
		clear: Effect.sync(() => repositoryAuthorizations.clear())
	};
});
var ContainerOutbound = class ContainerOutbound extends Context.Service()("kommunikasie/ContainerOutbound") {
	static layer = Layer.effect(ContainerOutbound, make);
};
//#endregion
export { ContainerOutbound, HOST_OPERATION_HOST, OPENAI_HOST, makeSandboxAgentOutbound };
