import { toJson } from "../Http.mjs";
import { SessionSandbox } from "../SessionSandbox.mjs";
import { NATIVE_RECEIPTS_DIRECTORY, WORKSPACE_DIR } from "../workspace-paths.mjs";
import { sanitizeCause } from "../observability/FailureDiagnostic.mjs";
import { HarnessTurnError } from "../harness/Harness.mjs";
import { SandboxOperationError } from "./NativeSandbox.mjs";
import { NATIVE_GATEWAY_SOURCE } from "../harness/NativeGatewaySource.mjs";
import { Cause, Effect, Option, Predicate, Schema } from "effect";
//#region src/cloudflare/NativeOperation.ts
const NativeState = Schema.Literals([
	"running",
	"completed",
	"cancelled",
	"quarantined"
]);
const NativeCommandStatus = Schema.Struct({
	state: NativeState,
	stdout: Schema.String,
	stderr: Schema.String,
	exitCode: Schema.NullOr(Schema.Int),
	timedOut: Schema.Boolean,
	truncated: Schema.Boolean
});
const Reply = Schema.Struct({
	id: Schema.String,
	result: Schema.optionalKey(Schema.Unknown),
	error: Schema.optionalKey(Schema.Struct({ message: Schema.String }))
});
const nativeOperationRequest = (operation) => new Request(`http://localhost:${operation.port}/${encodeURIComponent(operation.id)}`, { headers: {
	Upgrade: "websocket",
	Authorization: `Bearer ${operation.token}`
} });
const expectedTransportFailure = (cause) => cause.reasons.length > 0 && cause.reasons.every((reason) => Cause.isFailReason(reason) && Predicate.isTagged(reason.error, "SandboxOperationError"));
const connect = Effect.fn("NativeOperation.connect")(function* (operation, bootstrap = true) {
	const sandbox = yield* SessionSandbox;
	const directory = NATIVE_RECEIPTS_DIRECTORY;
	while (true) {
		const listeningProcess = (yield* sandbox.listProcesses).find(({ state, command }) => state === "running" && command[0] === "bun" && command[1] === "-e" && command.at(-2) === String(operation.port) && command.at(-1) === directory);
		let listener = listeningProcess ? yield* sandbox.getProcess(listeningProcess.id) : Option.none();
		if (Option.isNone(listener)) {
			if (!bootstrap) return yield* HarnessTurnError.make({
				reason: "connectionLost",
				message: "Native listener has stopped"
			});
			listener = yield* sandbox.exec([
				"bun",
				"-e",
				NATIVE_GATEWAY_SOURCE,
				String(operation.port),
				directory
			], {
				cwd: WORKSPACE_DIR,
				nativeListener: true
			}).pipe(Effect.map(Option.some), Effect.catchCause((cause) => expectedTransportFailure(cause) ? Effect.logWarning("Native gateway bootstrap acknowledgement was lost", sanitizeCause(cause, { operation: "NativeOperation.bootstrap" })).pipe(Effect.as(Option.none())) : Effect.failCause(cause)));
		}
		if (Option.isNone(listener)) {
			yield* Effect.sleep("100 millis");
			continue;
		}
		const response = yield* listener.value.waitForPort(operation.port, { timeout: 1e4 }).pipe(Effect.andThen(sandbox.forwardPort(operation.port, nativeOperationRequest(operation))), Effect.catchCause((cause) => expectedTransportFailure(cause) ? Effect.logWarning("Native gateway connection failed", sanitizeCause(cause, { operation: "NativeOperation.forwardPort" })).pipe(Effect.as(null)) : Effect.failCause(cause)));
		if (response !== null && response.webSocket !== null) return response.webSocket;
		yield* Effect.sleep("100 millis");
	}
}, Effect.timeoutOrElse({
	duration: "10 seconds",
	orElse: () => HarnessTurnError.make({
		reason: "connectionLost",
		message: "Native gateway could not be reached"
	})
}));
const open = Effect.fn("NativeOperation.open")(function* (operation, bootstrap = true) {
	const socket = yield* Effect.acquireRelease(connect(operation, bootstrap), (socket) => Effect.sync(() => socket.close(1e3, "Native operation receipt read")), { interruptible: true });
	yield* Effect.try({
		try: () => socket.accept(),
		catch: () => HarnessTurnError.make({
			reason: "connectionLost",
			message: "Native gateway disconnected"
		})
	});
	return Effect.fn("NativeOperation.rpc")(function* (method, params) {
		const text = yield* Effect.callback((resume) => {
			const cleanup = () => {
				socket.removeEventListener("message", received);
				socket.removeEventListener("error", failed);
				socket.removeEventListener("close", failed);
			};
			const failed = () => {
				cleanup();
				resume(HarnessTurnError.make({
					reason: "connectionLost",
					message: "Native gateway disconnected"
				}));
			};
			const received = (event) => {
				if (typeof event.data !== "string" || event.data.length > 8388608) failed();
				else {
					cleanup();
					resume(Effect.succeed(event.data));
				}
			};
			socket.addEventListener("message", received);
			socket.addEventListener("error", failed);
			socket.addEventListener("close", failed);
			try {
				socket.send(toJson({
					id: method,
					method,
					params
				}));
			} catch {
				failed();
			}
			return Effect.sync(cleanup);
		}).pipe(Effect.timeoutOrElse({
			duration: "10 seconds",
			orElse: () => HarnessTurnError.make({
				reason: "connectionLost",
				message: "Native gateway did not reply"
			})
		}));
		const reply = yield* Schema.decodeEffect(Schema.fromJsonString(Reply))(text).pipe(Effect.mapError(() => HarnessTurnError.make({
			reason: "malformedEvent",
			message: "Invalid native gateway receipt"
		})));
		if (reply.id !== method) return yield* HarnessTurnError.make({
			reason: "malformedEvent",
			message: "Unexpected native gateway receipt"
		});
		if (reply.error !== void 0) return yield* HarnessTurnError.make({
			reason: "turnFailed",
			message: reply.error.message
		});
		return reply.result;
	});
});
const ensureConnection = Effect.fn("NativeOperation.ensure")(function* (operation) {
	const rpc = yield* open(operation);
	const { token: _token, port: _port, ...configuration } = operation;
	const result = yield* rpc("gateway/ensure", configuration);
	const receipt = yield* Schema.decodeUnknownEffect(Schema.Struct({
		state: NativeState,
		timedOut: Schema.Boolean
	}))(result).pipe(Effect.mapError(() => HarnessTurnError.make({
		reason: "malformedEvent",
		message: "Invalid native operation state"
	})));
	if (receipt.state === "quarantined" || receipt.state === "cancelled") return yield* HarnessTurnError.make({
		reason: receipt.timedOut ? "deadlineExceeded" : "turnFailed",
		message: `Native operation is ${receipt.state}; its input was not replayed`
	});
	return rpc;
});
const ensureNativeOperation = (operation) => ensureConnection(operation).pipe(Effect.asVoid, Effect.scoped);
/** Status is authoritative at the native owner, including completed output after reconnect. */
const readNativeCommand = Effect.fn("NativeOperation.readCommand")(function* (operation) {
	const result = yield* (yield* ensureConnection(operation))("gateway/status", {});
	return yield* Schema.decodeUnknownEffect(NativeCommandStatus)(result).pipe(Effect.mapError(() => HarnessTurnError.make({
		reason: "malformedEvent",
		message: "Invalid native command receipt"
	})));
}, Effect.scoped);
const listeningProcesses = Effect.fnUntraced(function* (port) {
	return yield* (yield* SessionSandbox).listProcesses.pipe(Effect.map((jobs) => jobs.filter((job) => job.state === "running" && job.command[0] === "bun" && job.command[1] === "-e" && job.command.at(-2) === String(port) && job.command.at(-1) === "/workspace/.sandbox-agent/native")));
});
const retiredProcess = (error) => error.code === "PROCESS_RETIRED";
const existingListenerCall = Effect.fnUntraced(function* (operation, method) {
	if ((yield* listeningProcesses(operation.port)).length === 0) return false;
	const observation = yield* Effect.gen(function* () {
		yield* (yield* open(operation, false))(method, {});
	}).pipe(Effect.scoped, Effect.exit);
	if (observation._tag === "Success") return true;
	if (!observation.cause.reasons.every((reason) => Cause.isFailReason(reason) && Predicate.isTagged(reason.error, "HarnessTurnError") && reason.error.reason === "connectionLost")) return yield* Effect.failCause(observation.cause);
	const listeners = yield* listeningProcesses(operation.port).pipe(Effect.exit);
	if (listeners._tag === "Failure") return yield* Effect.failCause(Cause.combine(observation.cause, listeners.cause));
	if (listeners.value.length !== 0) return yield* Effect.failCause(observation.cause);
	return false;
});
/** Fences an operation even when its ensure request has not arrived yet. */
const cancelNativeOperation = Effect.fn("NativeOperation.cancel")(function* (operation) {
	yield* (yield* SessionSandbox).signalProcess(operation.id).pipe(Effect.catchIf(retiredProcess, () => Effect.void));
	yield* existingListenerCall(operation, "gateway/cancel");
}, Effect.scoped);
/** The caller must retain the terminal result before releasing native output. */
const acknowledgeNativeOperation = Effect.fn("NativeOperation.acknowledge")(function* (operation) {
	const sandbox = yield* SessionSandbox;
	const process = yield* sandbox.getProcess(operation.id).pipe(Effect.catchCause((cause) => cause.reasons.length > 0 && cause.reasons.every((reason) => Cause.isFailReason(reason) && retiredProcess(reason.error)) ? Effect.succeed(Option.none()) : Effect.failCause(cause)));
	if (Option.isSome(process)) yield* process.value.waitForExit({ timeout: 1e4 });
	if (yield* existingListenerCall(operation, "gateway/acknowledge")) return;
	if (Option.isNone(process)) yield* sandbox.signalProcess(operation.id).pipe(Effect.catchIf(retiredProcess, () => Effect.void));
	yield* sandbox.acknowledgeProcess(operation.id);
}, Effect.scoped);
/** Close only an idle listener; completed operation receipts remain replayable. */
const quiesceNativeOperation = Effect.fn("NativeOperation.quiesce")(function* (operation) {
	const sandbox = yield* SessionSandbox;
	const listeners = yield* listeningProcesses(operation.port);
	if (listeners.length === 0) return;
	const observed = yield* Effect.gen(function* () {
		const result = yield* (yield* open(operation, false))("gateway/quiesce", {});
		return {
			...yield* Schema.decodeUnknownEffect(Schema.Struct({ quiesced: Schema.Boolean }))(result).pipe(Effect.catchCause((cause) => Effect.failCause(Cause.map(cause, (cause) => new SandboxOperationError({
				operation: "NativeOperation.quiesce",
				cause
			}))))),
			listeners
		};
	}).pipe(Effect.scoped);
	if (!observed.quiesced) return;
	for (const listener of observed.listeners) {
		const process = yield* sandbox.getProcess(listener.id);
		if (Option.isSome(process)) yield* process.value.waitForExit({ timeout: 1e4 });
	}
});
//#endregion
export { acknowledgeNativeOperation, cancelNativeOperation, ensureNativeOperation, nativeOperationRequest, quiesceNativeOperation, readNativeCommand };
