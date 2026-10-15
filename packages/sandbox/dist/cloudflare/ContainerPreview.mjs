import { NATIVE_LISTENER_PORTS } from "../NativeExecutionLimits.mjs";
import { withErrorReportingScope } from "../observability/RuntimeDiagnostics.mjs";
import "../workspace-paths.mjs";
import { CAPACITY_NAME, NATIVE_INSTANCE_HEADER, NativeSandboxNamespace, PREVIEW_TOKEN_HEADER, decodeNativeReply, previewTokenHash } from "./NativeSandbox.mjs";
import { ContainerState, PreviewIndexRecord, PreviewRecord, error, samePreviewRecord, samePreviewRoute } from "./ContainerState.mjs";
import { ContainerLifecycle } from "./ContainerLifecycle.mjs";
import { ContainerProcesses } from "./ContainerProcesses.mjs";
import { Cause, Clock, Context, DateTime, Effect, Layer, Schema } from "effect";
import { DurableObjectAlarm, DurableObjectContainer, DurableObjectState, Worker } from "effect-cf";
//#region src/cloudflare/ContainerPreview.ts
const previewExpiryAlarm = {
	tag: "sandbox-preview",
	id: "routes"
};
const previewProgressKey = "sandbox-preview:progress";
const make = Effect.gen(function* () {
	const container = yield* DurableObjectContainer.DurableObjectContainer;
	const state = yield* DurableObjectState.DurableObjectState;
	const namespace = yield* NativeSandboxNamespace;
	const { read, identity, readFileString, admitted, previewValidation, cache } = yield* ContainerState;
	const alarms = yield* DurableObjectAlarm.DurableObjectAlarm;
	const { revokePreview, touch } = yield* ContainerLifecycle;
	const { processStatus, processes } = yield* ContainerProcesses;
	const registerPreview = Effect.fnUntraced(function* (hash, route, expiresAt) {
		const coordinator = yield* namespace.getByName(CAPACITY_NAME);
		const reply = yield* namespace.call(coordinator, "registerPreview", hash, route, expiresAt).pipe(Effect.catchCause((cause) => Effect.failCause(Cause.map(cause, (cause) => new DurableObjectContainer.ContainerError({
			operation: "preview.grant",
			cause
		})))));
		return yield* decodeNativeReply("preview.grant", reply);
	});
	const progress = (key) => read(key, Schema.Natural).pipe(Effect.map((value) => value ?? 0));
	const revokeObservedPreview = (record) => admitted(Effect.gen(function* () {
		const latest = yield* read("preview", PreviewRecord);
		if (latest !== null && samePreviewRecord(latest, record)) yield* revokePreview;
	}));
	const updatePreviewExpiry = Effect.fnUntraced(function* (tx, changed = false) {
		const now = yield* Clock.currentTimeMillis;
		let next = null;
		for (const [key] of yield* state.storage.kv.list({ prefix: "preview:" })) {
			const record = yield* read(key, PreviewIndexRecord);
			if (record && record.expiresAt <= now) {
				yield* state.storage.kv.delete(key);
				changed = true;
			} else if (record) next = Math.min(next ?? record.expiresAt, record.expiresAt);
		}
		const cursor = (yield* progress(previewProgressKey)) + (changed ? 1 : 0);
		if (changed) yield* state.storage.kv.put(previewProgressKey, cursor);
		if (next === null) yield* tx.cancelAlarm(previewExpiryAlarm);
		else yield* tx.scheduleAlarm({
			...previewExpiryAlarm,
			payload: null,
			runAt: DateTime.makeUnsafe(next),
			progress: cursor
		});
	});
	return {
		expirePreviews: alarms.transaction((tx) => updatePreviewExpiry(tx)),
		registerPreview: (hash, route, expiresAt) => Effect.gen(function* () {
			if (!/^[a-f0-9]{64}$/.test(hash)) return yield* error("preview.register", "Invalid preview identity");
			yield* alarms.transaction((tx) => Effect.gen(function* () {
				const key = `preview:${hash}`;
				const previous = yield* read(key, PreviewIndexRecord);
				if (route === null) {
					if (!previous) return;
					yield* state.storage.kv.delete(key);
				} else {
					if (previous?.expiresAt === expiresAt && samePreviewRoute(previous.route, route)) return;
					yield* state.storage.kv.put(key, {
						route,
						expiresAt
					});
				}
				yield* updatePreviewExpiry(tx, true);
			}));
		}),
		resolvePreview: (hash) => Effect.gen(function* () {
			if (!/^[a-f0-9]{64}$/.test(hash)) return null;
			const record = yield* read(`preview:${hash}`, PreviewIndexRecord);
			if (!record) return null;
			if (record.expiresAt <= (yield* Clock.currentTimeMillis)) return null;
			return record.route;
		}),
		grantPreview: (input) => admitted(Effect.gen(function* () {
			if (input.port !== 5173 || !/^[a-f0-9]{32}$/.test(input.token) || !Number.isSafeInteger(input.generation)) return yield* error("preview.grant", "Invalid preview capability");
			const current = yield* identity();
			if (!current || current.instanceId !== input.instanceId || !(yield* container.running) || (yield* readFileString("/workspace/.sandbox-agent-materialization")).trim() !== input.materializationId) return yield* error("preview.grant", "Preview workspace is unavailable");
			const processIds = (yield* processes).filter((job) => job.id === input.processId && job.instanceId === input.instanceId && job.state === "running").map((job) => job.id);
			if (!processIds.length) return yield* error("preview.grant", "Preview process is unavailable");
			const now = yield* Clock.currentTimeMillis;
			const hash = yield* previewTokenHash(input.token);
			const route = {
				sandboxId: state.id.toString(),
				port: 5173,
				materializationId: input.materializationId,
				generation: input.generation
			};
			const previous = yield* read("preview", PreviewRecord);
			const sameGrant = previous?.hash === hash && previous.instanceId === current.instanceId && samePreviewRoute(previous.route, route) && previous.processIds.length === processIds.length && previous.processIds.every((id) => processIds.includes(id));
			const expiresAt = Math.min(input.expiresAt ?? (sameGrant ? previous.expiresAt : now + 864e5), now + 864e5);
			if (expiresAt <= now) return yield* error("preview.grant", "Preview capability has expired");
			if (sameGrant && previous.expiresAt === expiresAt) {
				yield* registerPreview(hash, route, expiresAt);
				return;
			}
			yield* revokePreview;
			const granted = {
				hash,
				route,
				expiresAt,
				instanceId: current.instanceId,
				processIds
			};
			yield* state.storage.kv.put("preview", granted);
			yield* registerPreview(hash, route, expiresAt);
		})),
		fetchPreview: Effect.gen(function* () {
			const request = yield* Worker.NativeRequest;
			const token = request.headers.get(PREVIEW_TOKEN_HEADER);
			if (!token || !/^[a-f0-9]{32}$/.test(token)) return new Response(null, { status: 404 });
			const saved = yield* read("preview", PreviewRecord);
			const current = yield* identity();
			if (!saved || !current || saved.hash !== (yield* previewTokenHash(token)) || saved.instanceId !== current.instanceId || saved.expiresAt <= (yield* Clock.currentTimeMillis) || (yield* read("stopping-instance", Schema.String)) === current.instanceId || !(yield* container.running)) return new Response(null, { status: 404 });
			if (!(yield* previewValidation.withPermits(1)(Effect.gen(function* () {
				if (cache.mutations > 0) return false;
				const revision = cache.revision;
				const marker = (yield* readFileString("/workspace/.sandbox-agent-materialization")).trim();
				const observations = yield* Effect.forEach(saved.processIds, (id) => processStatus(id, 0));
				if (marker !== saved.route.materializationId || !observations.some((job) => job?.state === "running" && job.instanceId === current.instanceId)) {
					yield* state.waitUntil(revokeObservedPreview(saved).pipe(withErrorReportingScope));
					return false;
				}
				const latest = yield* read("preview", PreviewRecord);
				return cache.mutations === 0 && cache.revision === revision && latest !== null && samePreviewRecord(latest, saved);
			})))) return new Response(null, { status: cache.mutations > 0 ? 503 : 404 });
			const headers = new Headers(request.headers);
			for (const name of [
				PREVIEW_TOKEN_HEADER,
				"host",
				"x-forwarded-host",
				"forwarded"
			]) headers.delete(name);
			const target = new URL(request.url);
			target.hostname = "localhost";
			target.port = String(saved.route.port);
			target.protocol = "http:";
			if (headers.has("origin")) headers.set("origin", target.origin);
			headers.delete("referer");
			yield* touch;
			return yield* (yield* container.getTcpPort(saved.route.port)).proxy(new Request(new Request(target, request), { headers }));
		}),
		fetchNative: admitted(Effect.gen(function* () {
			const request = yield* Worker.NativeRequest;
			const url = new URL(request.url);
			const port = Number(url.port);
			const expected = request.headers.get(NATIVE_INSTANCE_HEADER);
			const current = yield* identity();
			if (url.hostname !== "sandbox-native.internal" || ![...NATIVE_LISTENER_PORTS, 5173].includes(port) || !expected || current?.instanceId !== expected || (yield* read("stopping-instance", Schema.String)) === expected || !(yield* container.running)) return new Response(null, { status: 404 });
			const headers = new Headers(request.headers);
			headers.delete(NATIVE_INSTANCE_HEADER);
			headers.delete("host");
			url.hostname = "localhost";
			yield* touch;
			return yield* (yield* container.getTcpPort(port)).proxy(new Request(new Request(url, request), { headers }));
		}))
	};
});
var ContainerPreview = class ContainerPreview extends Context.Service()("kommunikasie/ContainerPreview") {
	static layer = Layer.effect(ContainerPreview, make);
};
//#endregion
export { ContainerPreview };
