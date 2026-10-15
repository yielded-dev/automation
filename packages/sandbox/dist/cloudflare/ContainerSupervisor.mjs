import { ContainerState, NativeProcessError, SupervisorReply, error } from "./ContainerState.mjs";
import { PROCESS_SUPERVISOR_PORT } from "./ProcessSupervisorSource.mjs";
import { Clock, Context, Effect, Layer, Result, Schema } from "effect";
import { DurableObjectContainer, DurableObjectState } from "effect-cf";
//#region src/cloudflare/ContainerSupervisor.ts
/** Supervisors before protocol 2 answer `/health` with plain text. */
const HealthReply = Schema.Struct({ protocol: Schema.Int });
const make = Effect.gen(function* () {
	const container = yield* DurableObjectContainer.DurableObjectContainer;
	const state = yield* DurableObjectState.DurableObjectState;
	const { read } = yield* ContainerState;
	const supervisorCall = Effect.fnUntraced(function* (method, args, token) {
		const response = yield* (yield* container.getTcpPort(PROCESS_SUPERVISOR_PORT)).fetch(`http://localhost:${PROCESS_SUPERVISOR_PORT}/rpc`, {
			method: "POST",
			headers: {
				authorization: `Bearer ${token}`,
				"content-type": "application/json"
			},
			body: JSON.stringify({
				method,
				args
			})
		});
		const body = yield* Effect.tryPromise({
			try: () => response.text(),
			catch: (cause) => new DurableObjectContainer.ContainerError({
				operation: method,
				cause
			})
		});
		const result = yield* Schema.decodeEffect(Schema.fromJsonString(SupervisorReply))(body);
		if (!result.ok) return yield* new NativeProcessError({
			operation: method,
			code: result.error.code,
			cause: result.error
		});
		return result.value;
	});
	const readProtocol = Effect.gen(function* () {
		const response = yield* (yield* container.getTcpPort(PROCESS_SUPERVISOR_PORT)).fetch(`http://localhost:${PROCESS_SUPERVISOR_PORT}/health`);
		const body = yield* Effect.tryPromise({
			try: () => response.text(),
			catch: (cause) => new DurableObjectContainer.ContainerError({
				operation: "health",
				cause
			})
		});
		const decoded = yield* Schema.decodeEffect(Schema.fromJsonString(HealthReply))(body).pipe(Effect.result);
		const protocol = Result.isSuccess(decoded) ? decoded.success.protocol : 1;
		yield* state.storage.kv.put("supervisor-protocol", protocol);
		return protocol;
	});
	const ensureSupervisor = Effect.gen(function* () {
		yield* container.waitForHttp(PROCESS_SUPERVISOR_PORT, {
			path: "/health",
			timeout: 3e4,
			status: {
				min: 200,
				max: 299
			}
		}).pipe(Effect.withSpan("SandboxContainer.supervisorHttpReady"));
		yield* readProtocol.pipe(Effect.withSpan("SandboxContainer.supervisorProtocol"));
		yield* inspectProcess("listJobs", []).pipe(Effect.withSpan("SandboxContainer.supervisorAuthenticated"));
	});
	const inspectProcess = Effect.fnUntraced(function* (method, args) {
		if (!(yield* container.running)) {
			if (method === "listJobs") return [];
			if ([
				"observeJob",
				"waitJob",
				"signalJob",
				"acknowledgeJob"
			].includes(method)) return null;
			return yield* new NativeProcessError({
				operation: method,
				code: method === "lease" ? "LEASE_LOST" : "PROCESS_LOST",
				cause: /* @__PURE__ */ new Error("Container boot stopped")
			});
		}
		const token = yield* read("process-supervisor-token", Schema.String);
		if (!token) return yield* error(method, "Process supervisor boot capability is unavailable");
		return yield* supervisorCall(method, args, token);
	});
	return {
		inspectProcess,
		ensureSupervisor,
		protect: Effect.fnUntraced(function* (id, millis) {
			if (millis === null) return yield* inspectProcess("protect", [id, null]);
			const protocol = (yield* read("supervisor-protocol", Schema.Int)) ?? (yield* readProtocol);
			return yield* inspectProcess("protect", [id, protocol >= 2 ? millis : (yield* Clock.currentTimeMillis) + millis]);
		})
	};
});
var ContainerSupervisor = class ContainerSupervisor extends Context.Service()("kommunikasie/ContainerSupervisor") {
	static layer = Layer.effect(ContainerSupervisor, make);
};
//#endregion
export { ContainerSupervisor };
