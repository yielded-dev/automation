import { SANDBOX_PACKAGE_VERSION } from "../RuntimeVersions.mjs";
import { reportSanitizedCause, withErrorReportingScope } from "../observability/RuntimeDiagnostics.mjs";
import { NativeSandboxNamespace, decodeNativeReply } from "./NativeSandbox.mjs";
import { ContainerState, error, reply } from "./ContainerState.mjs";
import { ContainerOutbound, HOST_OPERATION_HOST, OPENAI_HOST, makeSandboxAgentOutbound } from "./ContainerOutbound.mjs";
import { ContainerSupervisor } from "./ContainerSupervisor.mjs";
import { ContainerLifecycle } from "./ContainerLifecycle.mjs";
import { ContainerProcesses } from "./ContainerProcesses.mjs";
import { ContainerPreview } from "./ContainerPreview.mjs";
import { ContainerWorkspace } from "./ContainerWorkspace.mjs";
import { Context, Effect, Layer, Schema } from "effect";
import { DurableObject, DurableObjectAlarm, DurableObjectContainer, DurableObjectState, Worker } from "effect-cf";
import { DirectoryBackupGateway } from "@cloudflare/sandbox";
//#region src/cloudflare/Container.ts
const AlarmHandlers = DurableObjectAlarm.define({ "sandbox-preview": Schema.Null });
const makeSandboxAgentContainer = (options) => {
	const make = Effect.gen(function* () {
		const lifecycle = yield* ContainerLifecycle;
		const processes = yield* ContainerProcesses;
		const workspace = yield* ContainerWorkspace;
		const preview = yield* ContainerPreview;
		const outbound = yield* ContainerOutbound;
		const { admitted } = yield* ContainerState;
		return {
			...lifecycle,
			...processes,
			...workspace,
			...preview,
			...outbound,
			ensure: (input = {}) => admitted(lifecycle.ensure(input)),
			revokePreview: admitted(lifecycle.revokePreview)
		};
	});
	class Runtime extends Context.Service()("sandbox-agent/ContainerRuntime") {}
	const stateLayer = ContainerState.layer(options);
	const supervisorLayer = ContainerSupervisor.layer.pipe(Layer.provideMerge(stateLayer));
	const outboundLayer = ContainerOutbound.layer.pipe(Layer.provideMerge(stateLayer));
	const lifecycleLayer = ContainerLifecycle.layer.pipe(Layer.provideMerge(Layer.merge(supervisorLayer, outboundLayer)));
	const processesLayer = ContainerProcesses.layer.pipe(Layer.provideMerge(lifecycleLayer));
	const operationsLayer = Layer.merge(ContainerWorkspace.layer, ContainerPreview.layer).pipe(Layer.provideMerge(processesLayer));
	const layer = Layer.effect(Runtime, make).pipe(Layer.provideMerge(operationsLayer), Layer.provideMerge(Layer.merge(DurableObjectContainer.layer, NativeSandboxNamespace.layer({ binding: options.namespaceBinding }))));
	return DurableObject.make(layer, {
		eventLayer: options.eventLayer ?? Layer.empty,
		initialize: Effect.flatMap(Runtime, (runtime) => runtime.initialize).pipe(Effect.tapCause((cause) => reportSanitizedCause(cause, { operation: "SandboxContainer.initialize" })), withErrorReportingScope),
		alarms: AlarmHandlers.handlers({ "sandbox-preview": () => Effect.flatMap(Runtime, (runtime) => runtime.expirePreviews) }).pipe(Effect.tapCause((cause) => reportSanitizedCause(cause, { operation: "SandboxContainer.alarm" })), withErrorReportingScope),
		fetch: Effect.gen(function* () {
			const runtime = yield* Runtime;
			const request = yield* Worker.NativeRequest;
			return yield* new URL(request.url).hostname === "sandbox-native.internal" ? runtime.fetchNative : runtime.fetchPreview;
		}),
		rpc: {
			locate: (name) => reply("SandboxContainer.locate", Effect.gen(function* () {
				const namespace = yield* NativeSandboxNamespace;
				const state = yield* DurableObjectState.DurableObjectState;
				if ((yield* namespace.idFromName(name)).toString() === state.id.toString()) return;
				if ((yield* namespace.idFromName("sandbox-capacity")).toString() !== state.id.toString()) return yield* error("locate", "Only the capacity owner can locate another sandbox");
				const child = yield* namespace.getByName(name);
				const result = yield* namespace.call(child, "locate", name);
				yield* decodeNativeReply("locate", result);
			})),
			describe: (input) => reply("SandboxContainer.describe", Effect.flatMap(Runtime, (r) => r.describe(input))),
			grantPreview: (input) => reply("SandboxContainer.grantPreview", Effect.flatMap(Runtime, (r) => r.grantPreview(input))),
			revokePreview: () => reply("SandboxContainer.revokePreview", Effect.flatMap(Runtime, (r) => r.revokePreview)),
			registerPreview: (hash, route, expiresAt) => reply("SandboxContainer.registerPreview", Effect.flatMap(Runtime, (r) => r.registerPreview(hash, route, expiresAt))),
			resolvePreview: (hash) => reply("SandboxContainer.resolvePreview", Effect.flatMap(Runtime, (r) => r.resolvePreview(hash))),
			exists: (path, start) => reply("SandboxContainer.exists", Effect.flatMap(Runtime, (r) => r.exists(path, start))),
			waitForPort: (id, port, timeout) => reply("SandboxContainer.waitForPort", Effect.flatMap(Runtime, (r) => r.waitForPort(id, port, timeout))),
			ensure: (input) => reply("SandboxContainer.ensure", Effect.flatMap(Runtime, (r) => r.ensure(input))),
			exec: (argv, input, start) => reply("SandboxContainer.exec", Effect.flatMap(Runtime, (r) => r.exec(argv, input, start))),
			inspect: (argv, input, start) => reply("SandboxContainer.inspect", Effect.flatMap(Runtime, (r) => r.inspect(argv, input, start))),
			allocateProcess: (start, resource, listenerPort) => reply("SandboxContainer.allocateProcess", Effect.flatMap(Runtime, (r) => r.allocateProcess(start, resource, listenerPort))),
			process: (id, maxBytes) => reply("SandboxContainer.process", Effect.flatMap(Runtime, (r) => r.processStatus(id, maxBytes))),
			waitProcess: (id, maxBytes, waitMillis) => reply("SandboxContainer.waitProcess", Effect.flatMap(Runtime, (r) => r.waitProcess(id, maxBytes, waitMillis))),
			acknowledgeProcess: (id) => reply("SandboxContainer.acknowledgeProcess", Effect.flatMap(Runtime, (r) => r.acknowledgeProcess(id))),
			processes: () => reply("SandboxContainer.processes", Effect.flatMap(Runtime, (r) => r.processes)),
			signalProcess: (id, instanceId, signal) => reply("SandboxContainer.signalProcess", Effect.flatMap(Runtime, (r) => r.signalProcess(id, instanceId, signal))),
			waitForHttp: (id, port, input) => reply("SandboxContainer.waitForHttp", Effect.flatMap(Runtime, (r) => r.waitForHttp(id, port, input))),
			readFileString: (path, start) => reply("SandboxContainer.readFileString", Effect.flatMap(Runtime, (r) => r.readFileString(path, start))),
			readFile: (path, start) => reply("SandboxContainer.readFile", Effect.flatMap(Runtime, (r) => r.readFile(path, start))),
			lstat: (path, start) => reply("SandboxContainer.lstat", Effect.flatMap(Runtime, (r) => r.lstat(path, start))),
			writeFile: (path, content, start) => reply("SandboxContainer.writeFile", Effect.flatMap(Runtime, (r) => r.writeFile(path, content, start))),
			readDirectory: (path, start) => reply("SandboxContainer.readDirectory", Effect.flatMap(Runtime, (r) => r.readDirectory(path, start))),
			mkdir: (path, recursive, start) => reply("SandboxContainer.mkdir", Effect.flatMap(Runtime, (r) => r.mkdir(path, recursive, start))),
			remove: (path, recursive, force, start) => reply("SandboxContainer.remove", Effect.flatMap(Runtime, (r) => r.remove(path, recursive, force, start))),
			checkpoint: (input) => reply("SandboxContainer.checkpoint", Effect.flatMap(Runtime, (r) => r.checkpoint(input))),
			configureOutboundAuthority: (authority) => reply("SandboxContainer.configureOutboundAuthority", Effect.flatMap(Runtime, (r) => r.configureOutboundAuthority(authority))),
			setRepositoryAuthorization: (remote, credential, expiresAt, instanceId, grantId) => reply("SandboxContainer.setRepositoryAuthorization", Effect.flatMap(Runtime, (r) => r.setRepositoryAuthorization(remote, credential, expiresAt, instanceId, grantId))),
			backupDirectory: (input, start) => reply("SandboxContainer.backupDirectory", Effect.flatMap(Runtime, (r) => r.backupDirectory(input, start))),
			restoreDirectory: (receipt, start) => reply("SandboxContainer.restoreDirectory", Effect.flatMap(Runtime, (r) => r.restoreDirectory(receipt, start))),
			deleteDirectoryBackup: (receipt) => reply("SandboxContainer.deleteDirectoryBackup", Effect.flatMap(Runtime, (r) => r.deleteDirectoryBackup(receipt))),
			destroy: () => reply("SandboxContainer.destroy", Effect.flatMap(Runtime, (r) => r.destroy)),
			park: () => reply("SandboxContainer.park", Effect.flatMap(Runtime, (r) => r.park)),
			pinImage: (imageDigest) => reply("SandboxContainer.pinImage", Effect.flatMap(Runtime, (r) => r.pinImage(imageDigest))),
			lease: (id, active, instanceId, renewal) => reply("SandboxContainer.lease", Effect.flatMap(Runtime, (r) => r.lease(id, active, instanceId, renewal))),
			capacityInUse: (instanceId) => reply("SandboxContainer.capacityInUse", Effect.flatMap(Runtime, (r) => r.capacityInUse(instanceId))),
			reserveCapacity: (id, instanceId) => reply("SandboxContainer.reserveCapacity", Effect.flatMap(Runtime, (r) => r.reserveCapacity(id, instanceId))),
			releaseCapacity: (id, instanceId) => reply("SandboxContainer.releaseCapacity", Effect.flatMap(Runtime, (r) => r.releaseCapacity(id, instanceId)))
		}
	});
};
//#endregion
export { DirectoryBackupGateway, HOST_OPERATION_HOST, OPENAI_HOST, SANDBOX_PACKAGE_VERSION, makeSandboxAgentContainer, makeSandboxAgentOutbound };
