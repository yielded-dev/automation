import { NATIVE_OUTPUT_LIMIT_BYTES } from "../NativeExecutionLimits.mjs";
import { causeFromFailureDiagnostic } from "../observability/FailureDiagnostic.mjs";
import { Cause, Clock, Context, Data, Effect, Option, Schema } from "effect";
import { DurableObjectNamespace } from "effect-cf";
//#region src/cloudflare/NativeSandbox.ts
const ProcessResource = Schema.Struct({
	key: Schema.String.check(Schema.isPattern(/^[a-z][a-z0-9-]{0,63}$/u)),
	generation: Schema.Natural.check(Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)),
	materializationId: Schema.NonEmptyString.check(Schema.isMaxLength(256))
});
const InspectionOptions = Schema.Struct({
	cwd: Schema.optionalKey(Schema.String),
	timeout: Schema.Finite.check(Schema.isGreaterThan(0)),
	maxBytes: Schema.Natural.check(Schema.isLessThanOrEqualTo(NATIVE_OUTPUT_LIMIT_BYTES)),
	deadlineAt: Schema.optionalKey(Schema.Finite)
});
const WorkspaceCheckpoint = Schema.Struct({
	kind: Schema.Literal("container"),
	snapshotId: Schema.String,
	imageDigest: Schema.String,
	setupFingerprint: Schema.String,
	head: Schema.String,
	materializationId: Schema.String,
	classIdentity: Schema.String,
	instanceId: Schema.String,
	capturedAt: Schema.Finite,
	expiresAt: Schema.Finite
});
const RepositoryAuthorizationGrant = Schema.Struct({
	remote: Schema.String,
	instanceId: Schema.String,
	grantId: Schema.String
});
const ProcessStatus = Schema.Struct({
	id: Schema.String,
	command: Schema.Array(Schema.String),
	instanceId: Schema.String,
	state: Schema.Literals([
		"running",
		"exited",
		"lost"
	]),
	exit: Schema.optionalKey(Schema.Struct({
		code: Schema.Int,
		signal: Schema.optionalKey(Schema.String)
	})),
	stdout: Schema.String,
	stderr: Schema.String,
	exitCode: Schema.NullOr(Schema.Int),
	timedOut: Schema.Boolean,
	truncated: Schema.Boolean,
	deadlineAt: Schema.NullOr(Schema.Finite),
	completionRevision: Schema.NullOr(Schema.Natural)
});
var SandboxOperationError = class extends Data.TaggedError("SandboxOperationError") {
	get message() {
		return `Sandbox operation ${this.operation} failed`;
	}
};
const isSandboxFileMissing = (error) => error.code === "ENOENT";
const PreviewRoute = Schema.Struct({
	sandboxId: Schema.String,
	port: Schema.Literal(5173),
	materializationId: Schema.String,
	generation: Schema.Int
});
const PREVIEW_TOKEN_HEADER = "x-sandbox-preview-token";
const NATIVE_PORT_HOST = "sandbox-native.internal";
const NATIVE_INSTANCE_HEADER = "x-sandbox-native-instance";
const CAPACITY_NAME = "sandbox-capacity";
var NativeSandboxNamespace = class NativeSandboxNamespace extends Context.Service()("sandbox-agent/NativeSandboxNamespace") {
	static layer = (options) => DurableObjectNamespace.layer(NativeSandboxNamespace, options);
};
const decodeNativeReply = (operation, reply) => reply._tag === "Success" ? Effect.succeed(reply.value) : Effect.failCause(Cause.map(causeFromFailureDiagnostic(reply.diagnostic), (cause) => new SandboxOperationError({
	operation,
	cause,
	...reply.code === void 0 ? {} : { code: reply.code }
})));
/** Resolve native channels within each invocation; only the address survives durable retries. */
const makeNativeSandbox = Effect.fnUntraced(function* (name, defaults = {}) {
	const namespace = yield* NativeSandboxNamespace;
	let located = defaults.recovery === "exactWorkspaceRequired" && defaults.expectedInstanceId !== void 0;
	const resolve = Effect.gen(function* () {
		if (!located) {
			const coordinator = yield* namespace.getByName(CAPACITY_NAME);
			const reply = yield* namespace.call(coordinator, "locate", name).pipe(Effect.mapError((cause) => new SandboxOperationError({
				operation: "locate",
				cause,
				code: "RPC_TRANSPORT"
			})));
			yield* decodeNativeReply("locate", reply);
			located = true;
		}
		return yield* namespace.getByName(name);
	});
	const call = Effect.fnUntraced(function* (method, ...args) {
		const stub = yield* resolve;
		const reply = yield* namespace.call(stub, method, ...args).pipe(Effect.catchCause((cause) => Effect.failCause(Cause.map(cause, (error) => new SandboxOperationError({
			operation: method,
			cause: error,
			code: "RPC_TRANSPORT"
		})))));
		return yield* decodeNativeReply(method, reply);
	});
	const observe = (id, maxBytes = 0) => call("process", id, maxBytes);
	const handle = (initial) => {
		let terminal;
		const status = observe(initial.id).pipe(Effect.flatMap((value) => value === null ? Effect.fail(new SandboxOperationError({
			operation: "process",
			cause: /* @__PURE__ */ new Error("Process receipt is missing")
		})) : Effect.succeed(value)));
		const wait = Effect.fnUntraced(function* (timeout, maxBytes) {
			const deadline = (yield* Clock.currentTimeMillis) + timeout;
			while (true) {
				const remaining = deadline - (yield* Clock.currentTimeMillis);
				const value = terminal ?? (yield* call("waitProcess", initial.id, maxBytes, Math.max(0, Math.min(2e4, remaining))));
				if (value === null || value.state === "lost") return yield* new SandboxOperationError({
					operation: "process",
					cause: /* @__PURE__ */ new Error("Process ownership was lost")
				});
				if (value.state === "exited") return value;
				if ((yield* Clock.currentTimeMillis) >= deadline) return yield* new SandboxOperationError({
					operation: "process.wait",
					code: "ETIMEDOUT",
					cause: /* @__PURE__ */ new Error("Process observation timed out")
				});
			}
		});
		return {
			id: initial.id,
			status,
			kill: (signal = 15) => call("signalProcess", initial.id, initial.instanceId, signal),
			waitForExit: ({ timeout }) => wait(timeout, 0).pipe(Effect.map((value) => value.exit ?? { code: value.exitCode ?? -1 })),
			outputText: ({ timeout, maxBytes, acknowledge = true }) => wait(timeout, maxBytes).pipe(Effect.tap((value) => Effect.gen(function* () {
				terminal = value;
				if (acknowledge) yield* call("acknowledgeProcess", initial.id);
			})), Effect.map((value) => ({
				...value,
				exitCode: value.exitCode ?? -1
			}))),
			waitForPort: (port, options = {}) => call("waitForPort", initial.id, port, options.timeout ?? 3e4),
			waitForHttp: (port, options = {}) => call("waitForHttp", initial.id, port, options)
		};
	};
	const leases = /* @__PURE__ */ new Map();
	const readFileString = (path) => call("readFileString", path, defaults);
	return {
		describe: call("describe", defaults),
		ensure: (options = {}) => call("ensure", {
			...defaults,
			...options
		}),
		allocateProcess: Effect.fnUntraced(function* (resource) {
			const current = yield* call("ensure", defaults);
			return yield* call("allocateProcess", {
				...defaults,
				expectedInstanceId: current.instanceId,
				recovery: "exactWorkspaceRequired"
			}, resource ?? null, null);
		}),
		acknowledgeProcess: (id) => call("acknowledgeProcess", id),
		signalProcess: (id, signal = 15) => Effect.gen(function* () {
			const ticket = /^process:([^:]+):([1-9][0-9]*)$/u.exec(id);
			if (!ticket) return yield* new SandboxOperationError({
				operation: "signalProcess",
				cause: /* @__PURE__ */ new Error("Invalid process ticket")
			});
			return yield* call("signalProcess", id, ticket[1], signal);
		}),
		exec: (argv, options = {}) => {
			let id;
			let deadlineAt;
			let boundStart;
			return Effect.gen(function* () {
				if (options.processId !== void 0 && options.resource !== void 0) return yield* new SandboxOperationError({
					operation: "exec",
					cause: /* @__PURE__ */ new Error("Process ticket and resource identity are mutually exclusive")
				});
				if (boundStart === void 0) {
					const instanceId = defaults.expectedInstanceId ?? (yield* call("ensure", defaults)).instanceId;
					boundStart ??= {
						...defaults,
						expectedInstanceId: instanceId,
						recovery: "exactWorkspaceRequired"
					};
				}
				const exactStart = boundStart;
				id ??= options.processId ?? (yield* call("allocateProcess", exactStart, options.resource ?? null, options.nativeListener === true ? Number(argv[3]) : null));
				deadlineAt ??= options.deadlineAt ?? (options.timeout === void 0 ? null : (yield* Clock.currentTimeMillis) + options.timeout);
				return yield* call("exec", argv, {
					...options,
					processId: id,
					deadlineAt
				}, exactStart).pipe(Effect.map(handle));
			});
		},
		inspect: (argv, options) => {
			let boundStart;
			let deadlineAt;
			return Effect.gen(function* () {
				boundStart ??= {
					...defaults,
					expectedInstanceId: defaults.expectedInstanceId ?? (yield* call("ensure", defaults)).instanceId,
					recovery: "exactWorkspaceRequired"
				};
				deadlineAt ??= options.deadlineAt ?? (yield* Clock.currentTimeMillis) + options.timeout;
				return yield* call("inspect", argv, {
					...options,
					deadlineAt
				}, boundStart);
			});
		},
		observeProcess: (id, { maxBytes, waitMillis = 0 }) => waitMillis === 0 ? observe(id, maxBytes) : call("waitProcess", id, maxBytes, waitMillis),
		getProcess: (id) => observe(id).pipe(Effect.map((value) => value === null ? Option.none() : Option.some(handle(value)))),
		listProcesses: call("processes"),
		readFileString,
		readFile: (path) => call("readFile", path, defaults),
		lstat: (path) => call("lstat", path, defaults),
		writeFile: (path, content) => call("writeFile", path, content, defaults),
		readDirectory: (path) => call("readDirectory", path, defaults),
		mkdir: (path, options) => call("mkdir", path, options?.recursive ?? false, defaults),
		remove: (path, options) => call("remove", path, options?.recursive ?? false, options?.force ?? false, defaults),
		exists: (path) => call("exists", path, defaults),
		forwardPort: (port, request) => Effect.gen(function* () {
			const instanceId = defaults.expectedInstanceId ?? (yield* call("describe", defaults)).current?.instanceId;
			if (!instanceId) return yield* new SandboxOperationError({
				operation: "forwardPort",
				cause: /* @__PURE__ */ new Error("No active container owner")
			});
			const url = new URL(request.url);
			const headers = new Headers(request.headers);
			url.protocol = "http:";
			url.hostname = NATIVE_PORT_HOST;
			url.port = String(port);
			headers.set(NATIVE_INSTANCE_HEADER, instanceId);
			headers.delete("host");
			const stub = yield* resolve;
			return yield* namespace.fetch(stub, new Request(new Request(url, request), { headers })).pipe(Effect.catchCause((cause) => Effect.failCause(Cause.map(cause, (error) => new SandboxOperationError({
				operation: "forwardPort",
				cause: error
			})))));
		}),
		grantPreview: (input) => call("grantPreview", input),
		revokePreview: call("revokePreview"),
		checkpoint: (input) => call("checkpoint", input),
		configureOutboundAuthority: (authority) => call("configureOutboundAuthority", authority),
		setRepositoryAuthorization: (remote, credential, expiresAt, grantId) => {
			let grant;
			return Effect.gen(function* () {
				if (grant === void 0) {
					const current = yield* call("ensure", defaults);
					grant ??= {
						remote,
						grantId,
						instanceId: current.instanceId
					};
				}
				const owner = grant;
				yield* call("setRepositoryAuthorization", remote, credential, expiresAt, owner.instanceId, owner.grantId);
				return owner;
			});
		},
		clearRepositoryAuthorization: (grant) => call("setRepositoryAuthorization", grant.remote, null, null, grant.instanceId, grant.grantId),
		backupDirectory: (input) => call("backupDirectory", input, defaults),
		restoreDirectory: (receipt) => call("restoreDirectory", receipt, defaults),
		deleteDirectoryBackup: (receipt) => call("deleteDirectoryBackup", receipt),
		lease: (id, active, renewal = false) => Effect.gen(function* () {
			let instanceId = leases.get(id) ?? defaults.expectedInstanceId;
			if (!instanceId && active) {
				instanceId = (yield* call("ensure", defaults)).instanceId;
				leases.set(id, instanceId);
			}
			if (!instanceId) return;
			yield* call("lease", id, active, instanceId, renewal);
			if (!active) leases.delete(id);
		}),
		destroy: call("destroy"),
		park: call("park"),
		pinImage: (imageDigest) => call("pinImage", imageDigest)
	};
});
const previewTokenHash = (token) => Effect.tryPromise({
	try: async () => {
		const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(token));
		return Array.from(new Uint8Array(digest), (value) => value.toString(16).padStart(2, "0")).join("");
	},
	catch: (cause) => new SandboxOperationError({
		operation: "previewTokenHash",
		cause
	})
});
//#endregion
export { CAPACITY_NAME, InspectionOptions, NATIVE_INSTANCE_HEADER, NATIVE_PORT_HOST, NativeSandboxNamespace, PREVIEW_TOKEN_HEADER, PreviewRoute, ProcessResource, ProcessStatus, RepositoryAuthorizationGrant, SandboxOperationError, WorkspaceCheckpoint, decodeNativeReply, isSandboxFileMissing, makeNativeSandbox, previewTokenHash };
