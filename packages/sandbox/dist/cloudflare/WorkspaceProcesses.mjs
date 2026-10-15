import { SessionSandbox } from "../SessionSandbox.mjs";
import { REPO_DIR } from "../workspace-paths.mjs";
import { Harness } from "../harness/Harness.mjs";
import { inMeasure } from "../tracing/RunSpanCapture.mjs";
import { SessionStateStore } from "./SessionStateStore.mjs";
import { Context, Effect, Layer, Option, Redacted, Schema, Semaphore } from "effect";
//#region src/cloudflare/WorkspaceProcesses.ts
/**
* Process hygiene and long-lived workspace services inside the session's
* sandbox. Turns are serialized per session, so any harness process found
* running at turn start is a zombie from a dead isolate; it is killed before
* the workspace is touched so it cannot dirty files a later persist would
* silently commit. Services (dev server, database) are caller-declared per
* message, keep running between turns, and never block or fail a turn.
*/
/** The message's declared workspace services; defaults to none, so hosts
* that never declare services provide nothing. */
const TurnWorkspaceServices = Context.Reference("sandbox-agent/WorkspaceProcesses/TurnWorkspaceServices", { defaultValue: () => [] });
const TurnWorkspaceServiceEnvironment = Context.Reference("sandbox-agent/WorkspaceProcesses/TurnWorkspaceServiceEnvironment", { defaultValue: () => Redacted.make({}) });
const TurnWorkspaceServicePorts = Context.Reference("sandbox-agent/WorkspaceProcesses/TurnWorkspaceServicePorts", { defaultValue: () => ({}) });
const RequiredWorkspaceServices = Context.Reference("sandbox-agent/WorkspaceProcesses/RequiredWorkspaceServices", { defaultValue: () => [] });
var RequiredWorkspaceServiceExited = class extends Schema.TaggedError()("RequiredWorkspaceServiceExited", { name: Schema.String }) {};
/**
* Serializes workspace-service reconciliation within one session Durable
* Object. A turn and an on-demand preview activation can both observe the
* same process snapshot; one coordinator prevents them from double-launching
* a declared service.
*/
var WorkspaceServiceCoordinator = class WorkspaceServiceCoordinator extends Context.Service()("sandbox-agent/WorkspaceProcesses/WorkspaceServiceCoordinator") {
	static layer = Layer.sync(WorkspaceServiceCoordinator, () => {
		const semaphore = Semaphore.makeUnsafe(1);
		return WorkspaceServiceCoordinator.of({ run: (effect) => semaphore.withPermit(effect) });
	});
};
const SIGKILL = 9;
const sameArgv = (command, argv) => command.length === argv.length && command.every((token, index) => token === argv[index]);
const sameManagedServices = (left, right) => left.length === right.length && left.every((service, index) => {
	const candidate = right[index];
	return candidate !== void 0 && service.processId === candidate.processId && service.service.name === candidate.service.name && sameArgv(service.service.argv, candidate.service.argv);
});
const killProcess = Effect.fn("WorkspaceProcesses.killProcess")(function* (processId) {
	const handle = yield* (yield* SessionSandbox).getProcess(processId);
	if (Option.isSome(handle)) yield* handle.value.kill(SIGKILL);
});
const runningProcesses = Effect.fn("WorkspaceProcesses.runningProcesses")(function* (candidates) {
	const sandbox = yield* SessionSandbox;
	const running = [];
	for (const candidate of candidates) {
		const handle = yield* sandbox.getProcess(candidate.id);
		if (Option.isNone(handle)) continue;
		const current = yield* handle.value.status;
		if (current.state === "running" && sameArgv(current.command, candidate.command)) running.push(current);
	}
	return running;
});
/** Kills direct harness processes and their native bootstrap wrappers. */
const killStrayHarnessProcesses = Effect.fn("WorkspaceProcesses.killStrayHarnessProcesses")(function* () {
	const sandbox = yield* SessionSandbox;
	const harness = yield* Harness;
	const strays = (yield* sandbox.listProcesses).filter((process) => process.state === "running" && (process.command[0] === harness.binary || process.command[0] === "sh" && process.command[1] === "-c" && process.command[3] === "codex-app-server" && process.command[4] === harness.binary && process.command[5] === "app-server"));
	yield* Effect.forEach(strays, (stray) => killProcess(stray.id), { concurrency: "unbounded" });
}, inMeasure("stray-harness-kill"));
/**
* Ensures every service declared on the turn has a running process. When the
* workspace was re-materialized this turn, matching processes are killed
* first — their working tree is gone — and started fresh against the new
* checkout. Started processes are deliberately not awaited; the sandbox
* supervises them.
*/
const reconcileWorkspaceServices = Effect.fn("WorkspaceProcesses.reconcileWorkspaceServices")(function* (workspaceReplaced) {
	const services = yield* TurnWorkspaceServices;
	const environment = yield* TurnWorkspaceServiceEnvironment;
	const ports = yield* TurnWorkspaceServicePorts;
	const required = yield* RequiredWorkspaceServices;
	const store = yield* SessionStateStore;
	const previous = (yield* store.managedWorkspaceServices.get) ?? [];
	if (services.length === 0 && previous.length === 0) return;
	const sandbox = yield* SessionSandbox;
	const processes = yield* sandbox.listProcesses;
	const seen = /* @__PURE__ */ new Set();
	const unique = services.filter((service) => {
		const key = service.argv.join("\0");
		if (seen.has(key)) return false;
		seen.add(key);
		return true;
	});
	const desiredByName = new Map(unique.map((service) => [service.name, service]));
	const killedIds = /* @__PURE__ */ new Set();
	const stale = previous.filter((registered) => {
		const desired = desiredByName.get(registered.service.name);
		return workspaceReplaced || desired === void 0 || !sameArgv(desired.argv, registered.service.argv);
	});
	yield* Effect.forEach(stale, (registered) => killProcess(registered.processId).pipe(Effect.tap(() => Effect.sync(() => killedIds.add(registered.processId)))), { concurrency: "unbounded" });
	if (workspaceReplaced) {
		const invalidated = processes.filter((process) => process.state === "running" && !killedIds.has(process.id) && unique.some((service) => sameArgv(process.command, service.argv)));
		yield* Effect.forEach(invalidated, (process) => killProcess(process.id).pipe(Effect.tap(() => Effect.sync(() => killedIds.add(process.id)))), { concurrency: "unbounded" });
	}
	const claimed = /* @__PURE__ */ new Set();
	const reconciled = [];
	for (const service of unique) {
		const registered = previous.find((candidate) => candidate.service.name === service.name && sameArgv(candidate.service.argv, service.argv) && !killedIds.has(candidate.processId));
		const candidates = processes.filter((process) => process.state === "running" && !killedIds.has(process.id) && !claimed.has(process.id) && sameArgv(process.command, service.argv));
		candidates.sort((left, right) => left.id === right.id ? 0 : left.id < right.id ? -1 : 1);
		const registeredIndex = registered === void 0 ? -1 : candidates.findIndex(({ id }) => id === registered.processId);
		if (registeredIndex > 0) {
			const registeredCandidate = candidates[registeredIndex];
			if (registeredCandidate !== void 0) {
				candidates.splice(registeredIndex, 1);
				candidates.unshift(registeredCandidate);
			}
		}
		const running = yield* runningProcesses(candidates);
		const adopted = running[0];
		if (adopted !== void 0) {
			yield* Effect.forEach(running.slice(1), (surplus) => killProcess(surplus.id).pipe(Effect.tap(() => Effect.sync(() => killedIds.add(surplus.id))), Effect.catchCause((cause) => Effect.logError("sandbox-agent duplicate workspace service cleanup failed", cause).pipe(Effect.annotateLogs({ "workspace.service": service.name })))), { concurrency: "unbounded" });
			claimed.add(adopted.id);
			reconciled.push({
				service,
				processId: adopted.id
			});
			const port = ports[service.name];
			if (port !== void 0) {
				const handle = yield* sandbox.getProcess(adopted.id);
				if (Option.isSome(handle)) yield* handle.value.waitForPort(port, { timeout: 12e4 });
			}
			if (required.includes(service.name)) {
				const handle = yield* sandbox.getProcess(adopted.id);
				if (Option.isNone(handle) || (yield* handle.value.status).state !== "running") return yield* RequiredWorkspaceServiceExited.make({ name: service.name });
			}
			continue;
		}
		const command = [service.argv[0], ...service.argv.slice(1)];
		const handle = yield* sandbox.exec(command, {
			cwd: REPO_DIR,
			env: Redacted.value(environment)
		}).pipe(inMeasure(`service:${service.name}`));
		claimed.add(handle.id);
		reconciled.push({
			service,
			processId: handle.id
		});
		yield* store.managedWorkspaceServices.put([...reconciled, ...previous.filter((registered) => !killedIds.has(registered.processId) && !reconciled.some((current) => current.service.name === registered.service.name))]);
		const port = ports[service.name];
		if (port !== void 0) yield* handle.waitForPort(port, { timeout: 12e4 });
		if (required.includes(service.name)) {
			if (port === void 0) yield* Effect.sleep("100 millis");
			if ((yield* handle.status).state !== "running") return yield* RequiredWorkspaceServiceExited.make({ name: service.name });
		}
	}
	if (!sameManagedServices(previous, reconciled)) yield* store.managedWorkspaceServices.put(reconciled);
});
const ensureWorkspaceServices = Effect.fn("WorkspaceProcesses.ensureWorkspaceServices")(function* (workspaceReplaced) {
	yield* (yield* WorkspaceServiceCoordinator).run(reconcileWorkspaceServices(workspaceReplaced));
});
//#endregion
export { RequiredWorkspaceServiceExited, RequiredWorkspaceServices, TurnWorkspaceServiceEnvironment, TurnWorkspaceServicePorts, TurnWorkspaceServices, WorkspaceServiceCoordinator, ensureWorkspaceServices, killStrayHarnessProcesses };
