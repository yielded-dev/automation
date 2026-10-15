import { NATIVE_LISTENER_PORTS } from "../NativeExecutionLimits.mjs";
import "../workspace-paths.mjs";
import { InspectionOptions, ProcessResource, ProcessStatus } from "./NativeSandbox.mjs";
import { NATIVE_GATEWAY_SOURCE } from "../harness/NativeGatewaySource.mjs";
import { ContainerState, NativeProcessError, ReadinessProcessError, error, failureCode } from "./ContainerState.mjs";
import { ContainerSupervisor } from "./ContainerSupervisor.mjs";
import { ContainerLifecycle } from "./ContainerLifecycle.mjs";
import { Cause, Clock, Context, Effect, Layer, Predicate, Schema } from "effect";
import { DurableObjectContainer } from "effect-cf";
//#region src/cloudflare/ContainerProcesses.ts
const make = Effect.gen(function* () {
	const container = yield* DurableObjectContainer.DurableObjectContainer;
	const { identity, admitted } = yield* ContainerState;
	const { inspectProcess } = yield* ContainerSupervisor;
	const { ensure } = yield* ContainerLifecycle;
	const allocateProcess = Effect.fn("SandboxContainer.allocateProcess")(function* (start, resource, listenerPort) {
		if (start.expectedInstanceId === void 0) return yield* error("allocateProcess", "Process allocation requires an exact container instance");
		if (resource !== null && !Schema.is(ProcessResource)(resource)) return yield* error("allocateProcess", "Invalid process resource");
		if (listenerPort !== null && (resource !== null || !NATIVE_LISTENER_PORTS.some((port) => port === listenerPort))) return yield* error("allocateProcess", "Invalid native listener reservation");
		yield* ensure({
			...start,
			recovery: "exactWorkspaceRequired"
		});
		return yield* inspectProcess("allocateTicket", [resource, listenerPort]).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.String)));
	});
	const processBelongsToBoot = Effect.fnUntraced(function* (id) {
		const current = yield* identity();
		if (current === null && (yield* container.running)) return yield* error("processStatus", "Running container has no ownership receipt");
		return current !== null && id.startsWith(`process:${current.instanceId}:`);
	});
	const processStatus = (id, maxBytes) => processBelongsToBoot(id).pipe(Effect.flatMap((owned) => owned ? inspectProcess("observeJob", [id, maxBytes]) : Effect.succeed(null)), Effect.catchIf((cause) => cause instanceof NativeProcessError && cause.code === "PROCESS_RETIRED", () => Effect.succeed(null)), Effect.flatMap(Schema.decodeUnknownEffect(Schema.NullOr(ProcessStatus))));
	const waitProcess = (id, maxBytes, waitMillis) => processBelongsToBoot(id).pipe(Effect.flatMap((owned) => owned ? inspectProcess("waitJob", [
		id,
		maxBytes,
		Math.min(2e4, Math.max(0, waitMillis))
	]) : Effect.succeed(null)), Effect.catchIf((cause) => cause instanceof NativeProcessError && cause.code === "PROCESS_RETIRED", () => Effect.succeed(null)), Effect.flatMap(Schema.decodeUnknownEffect(Schema.NullOr(ProcessStatus))));
	const processes = inspectProcess("listJobs", []).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(ProcessStatus))));
	const exec = Effect.fn("SandboxContainer.exec")(function* (argv, input, start) {
		if (start.expectedInstanceId === void 0) return yield* error("exec", "Process admission requires an exact container instance");
		const listenerPort = input.nativeListener === true ? Number(argv[3]) : void 0;
		if (input.nativeListener !== void 0 && (input.nativeListener !== true || argv.length !== 5 || argv[0] !== "bun" || argv[1] !== "-e" || argv[2] !== NATIVE_GATEWAY_SOURCE || argv[4] !== "/workspace/.sandbox-agent/native" || !NATIVE_LISTENER_PORTS.some((port) => port === listenerPort) || input.cwd !== "/workspace" || input.env !== void 0 || input.timeout !== void 0 || input.resource !== void 0 || input.workspaceIdentity !== void 0)) return yield* error("exec", "Invalid native listener bootstrap");
		const current = yield* ensure({
			...start,
			recovery: "exactWorkspaceRequired"
		});
		if (input.processId === void 0) return yield* error("exec", "Process dispatch requires an issued ticket for this boot");
		const config = {
			id: input.processId,
			resource: input.resource,
			argv,
			cwd: input.cwd,
			env: input.env,
			instanceId: current.instanceId,
			deadlineAt: input.deadlineAt ?? null,
			workspaceIdentity: input.workspaceIdentity,
			nativeListenerPort: listenerPort
		};
		return yield* inspectProcess("launchJob", [config]).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ProcessStatus)));
	});
	return {
		allocateProcess: (start, resource, listenerPort) => admitted(allocateProcess(start, resource, listenerPort)),
		exec: (argv, input, start) => admitted(exec(argv, input, start)),
		inspect: Effect.fn("SandboxContainer.inspect")(function* (argv, options, start) {
			const input = yield* Schema.decodeUnknownEffect(InspectionOptions)(options);
			const deadlineAt = Math.min(input.deadlineAt ?? Infinity, (yield* Clock.currentTimeMillis) + input.timeout);
			let completed = false;
			return yield* Effect.acquireUseRelease(admitted(allocateProcess(start, null, null)), (id) => Effect.gen(function* () {
				yield* admitted(exec(argv, {
					cwd: input.cwd,
					processId: id,
					deadlineAt
				}, start));
				while (true) {
					const remaining = deadlineAt - (yield* Clock.currentTimeMillis);
					const result = yield* waitProcess(id, input.maxBytes, Math.max(0, remaining));
					if (result === null || result.state === "lost") return yield* error("inspect", "Inspection process ownership was lost");
					if (result.state === "exited") {
						completed = true;
						return {
							stdout: result.stdout,
							stderr: result.stderr,
							exitCode: result.exitCode ?? -1,
							timedOut: result.timedOut,
							truncated: result.truncated
						};
					}
					if (remaining <= 0) return yield* new NativeProcessError({
						operation: "inspect",
						code: "ETIMEDOUT",
						cause: /* @__PURE__ */ new Error("Inspection observation timed out")
					});
				}
			}), (id) => Effect.gen(function* () {
				if (!(yield* processBelongsToBoot(id))) return;
				if (!completed) {
					yield* inspectProcess("signalJob", [id, 9]);
					const result = yield* waitProcess(id, 0, 2e4);
					if (result !== null && result.state !== "exited") return yield* error("inspect", "Inspection process retirement is unconfirmed");
				}
				yield* inspectProcess("acknowledgeJob", [id]);
			}));
		}),
		processes,
		processStatus,
		waitProcess,
		acknowledgeProcess: (id) => processBelongsToBoot(id).pipe(Effect.flatMap((owned) => owned ? inspectProcess("acknowledgeJob", [id]).pipe(Effect.asVoid) : Effect.void)),
		signalProcess: (id, instanceId, signal) => Effect.gen(function* () {
			const current = yield* identity();
			if (current?.instanceId !== instanceId) {
				if (current === null && (yield* container.running)) return yield* error("signalProcess", "Running container has no ownership receipt");
				return;
			}
			yield* inspectProcess("signalJob", [id, signal]);
		}),
		waitForHttp: (id, port, input) => Effect.gen(function* () {
			const before = yield* processStatus(id, 0);
			if (before?.state !== "running") return yield* new ReadinessProcessError({
				processId: id,
				code: before?.state === "exited" ? "PROCESS_EXITED" : "PROCESS_LOST"
			});
			yield* container.waitForHttp(port, input).pipe(Effect.catchCause((cause) => Effect.gen(function* () {
				if (!cause.reasons.every((reason) => Cause.isFailReason(reason) && (Predicate.isTagged(reason.error, "ContainerReadinessTimeoutError") || Predicate.isTagged(reason.error, "ContainerError")))) return yield* Effect.failCause(cause);
				const observation = yield* processStatus(id, 0).pipe(Effect.exit);
				if (observation._tag === "Failure") return yield* Effect.failCause(Cause.combine(cause, observation.cause));
				const state = observation.value?.state;
				if (state === "running") return yield* Effect.failCause(cause);
				return yield* Effect.failCause(Cause.map(cause, (original) => new ReadinessProcessError({
					processId: id,
					code: state === "exited" ? "PROCESS_EXITED" : "PROCESS_LOST",
					cause: original
				})));
			})));
			const after = yield* processStatus(id, 0);
			if (after?.state !== "running") return yield* new ReadinessProcessError({
				processId: id,
				code: after?.state === "exited" ? "PROCESS_EXITED" : "PROCESS_LOST"
			});
		}),
		waitForPort: Effect.fn("SandboxContainer.waitForPort")(function* (id, port, timeout) {
			yield* Effect.annotateCurrentSpan({
				"sandbox.process.id": id,
				"sandbox.port": port
			});
			const deadline = (yield* Clock.currentTimeMillis) + timeout;
			while (true) {
				const remaining = deadline - (yield* Clock.currentTimeMillis);
				const result = yield* inspectProcess("waitForPort", [
					id,
					port,
					Math.max(1, Math.min(2e4, remaining))
				]).pipe(Effect.exit);
				if (result._tag === "Success") return;
				if (failureCode(result.cause) !== "ETIMEDOUT" || (yield* Clock.currentTimeMillis) >= deadline) return yield* Effect.failCause(result.cause);
			}
		})
	};
});
var ContainerProcesses = class ContainerProcesses extends Context.Service()("kommunikasie/ContainerProcesses") {
	static layer = Layer.effect(ContainerProcesses, make);
};
//#endregion
export { ContainerProcesses };
