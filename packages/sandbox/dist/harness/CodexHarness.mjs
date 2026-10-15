import { toJson } from "../Http.mjs";
import { CODEX_VERSION } from "../RuntimeVersions.mjs";
import { SessionSandbox } from "../SessionSandbox.mjs";
import { REPO_DIR } from "../workspace-paths.mjs";
import { Harness, HarnessTurnError } from "./Harness.mjs";
import { isSandboxFileMissing } from "../cloudflare/NativeSandbox.mjs";
import { NativeWorkspaceIdentity } from "../cloudflare/NativeWorkspaceIdentity.mjs";
import { SessionStateStore } from "../cloudflare/SessionStateStore.mjs";
import { acknowledgeNativeOperation, cancelNativeOperation, ensureNativeOperation, nativeOperationRequest } from "../cloudflare/NativeOperation.mjs";
import "./CodexEventBounds.mjs";
import { runCodexAppServerTurn } from "./CodexAppServer.mjs";
import { Cause, Clock, Effect, Exit, Layer, Predicate, Queue, Redacted, Stream } from "effect";
//#region src/harness/CodexHarness.ts
/** Isolated Codex configuration and thread-state directory. */
const CODEX_HOME_DIR = "/workspace/codex-home";
/** The outbound proxy replaces this placeholder with the real API key. */
const CODEX_PLACEHOLDER_API_KEY = "sandbox-agent-placeholder-not-a-real-key";
const CODEX_APP_SERVER_PORT = 4500;
const MAX_BUFFERED_BYTES = 16777216;
const encoder = new TextEncoder();
/** Explicit cancellation is separate from losing a Worker connection. */
const cancelCodexTurn = (turn) => cancelNativeOperation({
	id: turn.processId,
	token: turn.token,
	deadlineAt: turn.deadlineAt,
	port: turn.configuration.port
});
/** The sandbox owns the process; each DO incarnation attaches with its durable capability. */
const CodexHarnessLive = Layer.succeed(Harness)({
	name: "codex",
	binary: "codex",
	version: `codex-cli ${CODEX_VERSION}`,
	runTurn: Effect.fn("CodexHarness.runTurn")(function* (request) {
		const sandbox = yield* SessionSandbox;
		const store = yield* SessionStateStore;
		const deadlines = request.deadlinePolicy;
		const saved = yield* store.codexTurn.get;
		const recovering = saved?.id === request.turnId;
		const workspace = yield* store.pipelineTurn.get;
		const workspaceIdentity = (yield* NativeWorkspaceIdentity) ?? (workspace?.id === request.turnId ? {
			path: "/workspace/.sandbox-agent-materialization",
			id: workspace.materializationId
		} : null);
		const requireWorkspace = Effect.gen(function* () {
			if (workspace?.id !== request.turnId) return;
			if ((yield* sandbox.readFileString("/workspace/.sandbox-agent-materialization").pipe(Effect.map((content) => content.trim()), Effect.catchIf(isSandboxFileMissing, () => Effect.succeed(null)))) !== workspace.materializationId) return yield* HarnessTurnError.make({
				reason: "workspaceLost",
				message: "The active turn's sandbox workspace was replaced"
			});
		});
		const checkpoint = recovering ? saved : {
			id: request.turnId,
			processId: yield* sandbox.allocateProcess(),
			token: crypto.randomUUID(),
			configuration: {
				cwd: REPO_DIR,
				env: { CODEX_HOME: CODEX_HOME_DIR },
				...workspaceIdentity === null ? {} : { workspaceIdentity },
				port: CODEX_APP_SERVER_PORT,
				nativePort: 4501,
				mode: "codex",
				command: [
					"sh",
					"-c",
					request.resumeToken === null ? "set -eu; rm -rf -- \"$CODEX_HOME\"; mkdir -p -- \"$CODEX_HOME\"; exec \"$@\"" : "exec \"$@\"",
					"codex-app-server",
					"codex",
					"app-server",
					...request.restrictModelRequests === true ? [
						"-c",
						"model_provider=\"host-controlled\"",
						"-c",
						"model_providers.host-controlled.name=\"Host controlled\"",
						"-c",
						"model_providers.host-controlled.base_url=\"https://api.openai.com/v1\"",
						"-c",
						"model_providers.host-controlled.wire_api=\"responses\"",
						"-c",
						"model_providers.host-controlled.requires_openai_auth=true",
						"-c",
						"model_providers.host-controlled.supports_websockets=false",
						"-c",
						"features.enable_request_compression=false",
						"-c",
						"model_auto_compact_token_limit=160000",
						"-c",
						"web_search=\"disabled\""
					] : [],
					...request.sentryMcp !== void 0 ? [
						"-c",
						`mcp_servers.sentry.url=${toJson(request.sentryMcp.url)}`,
						"-c",
						`mcp_servers.sentry.enabled_tools=${toJson(request.sentryMcp.enabledTools)}`,
						"-c",
						"mcp_servers.sentry.default_tools_approval_mode=\"auto\""
					] : [],
					"--listen",
					"ws://127.0.0.1:4501",
					"-c",
					`model=${toJson(request.model)}`,
					"-c",
					`model_reasoning_effort=${toJson(request.reasoningEffort)}`,
					...request.commitCoauthorTrailer === void 0 ? [] : ["-c", `developer_instructions=${toJson(`Include this trailer exactly once in every Git commit you create, preserving any other co-authors:\n\n${request.commitCoauthorTrailer}`)}`],
					"-c",
					"model_reasoning_summary=\"detailed\"",
					"-c",
					"hide_agent_reasoning=false",
					"-c",
					"approval_policy=\"never\"",
					"-c",
					"sandbox_mode=\"danger-full-access\""
				]
			},
			threadParams: {
				cwd: REPO_DIR,
				model: request.model,
				approvalPolicy: "never",
				sandbox: "danger-full-access"
			},
			turnParams: {
				input: [{
					type: "text",
					text: request.prompt
				}],
				effort: request.reasoningEffort,
				summary: "detailed",
				serviceTier: request.serviceTier === void 0 ? request.fast ? "fast" : null : request.serviceTier,
				...request.outputSchema === void 0 ? {} : { outputSchema: request.outputSchema }
			},
			resumeToken: request.resumeToken,
			deadlineAt: Math.min(request.deadlineAtMs ?? Number.POSITIVE_INFINITY, (yield* Clock.currentTimeMillis) + deadlines.harnessTurnMillis + deadlines.harnessSetupMillis),
			threadId: null,
			turnId: null,
			previousTurnId: null,
			startDispatched: false,
			result: null
		};
		if (!recovering) yield* store.codexTurn.put(checkpoint);
		const stop = cancelCodexTurn(checkpoint).pipe(Effect.catchCause((cause) => Effect.logError("sandbox-agent Codex process cleanup failed", cause)));
		const operation = {
			id: checkpoint.processId,
			token: checkpoint.token,
			deadlineAt: checkpoint.deadlineAt,
			...checkpoint.configuration
		};
		if (checkpoint.result !== null) {
			yield* stop;
			yield* acknowledgeNativeOperation(operation);
			return checkpoint.result;
		}
		let activeSteer;
		let steeringPublished = false;
		const steer = (input) => Effect.suspend(() => activeSteer === void 0 ? Effect.succeed({
			_tag: "Unknown",
			reason: "The native connection is recovering"
		}) : activeSteer(input));
		const connect = Effect.gen(function* () {
			yield* requireWorkspace;
			yield* ensureNativeOperation(operation);
			const response = yield* sandbox.forwardPort(operation.port, nativeOperationRequest(operation));
			const socket = response.webSocket;
			if (socket === null) return yield* HarnessTurnError.make({
				reason: response.status >= 500 ? "connectionLost" : "turnFailed",
				message: `Codex WebSocket upgrade failed: HTTP ${response.status}`
			});
			const queue = yield* Queue.bounded(256);
			let bufferedBytes = 0;
			const fail = (reason, message) => {
				Queue.failCauseUnsafe(queue, Cause.fail(HarnessTurnError.make({
					reason,
					message
				})));
				socket.close(1009, "Codex stream failed");
			};
			const onMessage = (event) => {
				if (!Predicate.isString(event.data)) {
					fail("malformedEvent", "Codex sent a non-text WebSocket message");
					return;
				}
				if (event.data.length > 8388608) {
					fail("oversizedEvent", "Codex WebSocket exceeded the input byte limit");
					return;
				}
				const byteLength = encoder.encode(event.data).byteLength;
				if (byteLength > 8388608 || bufferedBytes + byteLength > MAX_BUFFERED_BYTES) {
					fail("oversizedEvent", "Codex WebSocket exceeded the input byte limit");
					return;
				}
				if (!Queue.offerUnsafe(queue, {
					text: event.data,
					byteLength
				})) {
					fail("oversizedEvent", "Codex WebSocket exceeded the input queue limit");
					return;
				}
				bufferedBytes += byteLength;
			};
			const onClose = () => {
				Queue.endUnsafe(queue);
			};
			const onError = () => fail("connectionLost", "Codex WebSocket connection failed");
			yield* Effect.acquireRelease(Effect.sync(() => {
				socket.addEventListener("message", onMessage);
				socket.addEventListener("close", onClose);
				socket.addEventListener("error", onError);
				socket.accept();
				retryCause = void 0;
			}), () => Effect.sync(() => {
				socket.removeEventListener("message", onMessage);
				socket.removeEventListener("close", onClose);
				socket.removeEventListener("error", onError);
				socket.close(1e3, "Codex turn ended");
			}));
			return yield* runCodexAppServerTurn({
				...request,
				onSteeringReady: (nativeSteer) => Effect.gen(function* () {
					activeSteer = nativeSteer;
					if (!steeringPublished && request.onSteeringReady !== void 0) {
						yield* request.onSteeringReady(steer);
						steeringPublished = true;
					}
				})
			}, {
				apiKey: Redacted.make(CODEX_PLACEHOLDER_API_KEY),
				messages: Stream.fromQueue(queue).pipe(Stream.map((message) => {
					bufferedBytes -= message.byteLength;
					return message.text;
				})),
				send: (message) => Effect.try({
					try: () => socket.send(toJson(message)),
					catch: () => HarnessTurnError.make({
						reason: "connectionLost",
						message: "Could not send a Codex request"
					})
				})
			});
		});
		const remaining = checkpoint.deadlineAt - (yield* Clock.currentTimeMillis);
		let retryCause;
		const deadlineExceeded = () => {
			const cause = Cause.fail(HarnessTurnError.make({
				reason: "deadlineExceeded",
				message: `Codex turn exceeded ${deadlines.harnessTurnMillis} milliseconds`
			}));
			return Effect.failCause(retryCause === void 0 ? cause : Cause.combine(cause, retryCause));
		};
		if (remaining <= 0) {
			yield* stop;
			return yield* deadlineExceeded();
		}
		return yield* Effect.gen(function* () {
			while (true) {
				const attempt = yield* Effect.scoped(connect).pipe(Effect.ensuring(Effect.sync(() => {
					activeSteer = void 0;
				})), Effect.exit);
				if (Exit.isSuccess(attempt)) return attempt.value;
				const cause = attempt.cause;
				if (Cause.hasDies(cause) || Cause.hasInterrupts(cause)) return yield* Effect.failCause(cause);
				if (cause.reasons.length > 0 && cause.reasons.every((reason) => Cause.isFailReason(reason) && (reason.error instanceof HarnessTurnError && (reason.error.reason === "missingTerminalEvent" || reason.error.reason === "connectionLost") || Predicate.isTagged(reason.error, "SandboxOperationError")))) {
					retryCause = cause;
					yield* Effect.sleep("1 second");
					continue;
				}
				yield* requireWorkspace.pipe(Effect.catchCause((observationCause) => Effect.failCause(Cause.combine(cause, observationCause))));
				return yield* Effect.failCause(cause);
			}
		}).pipe(Effect.timeoutOrElse({
			duration: remaining,
			orElse: deadlineExceeded
		}), Effect.tap((result) => Effect.gen(function* () {
			const current = yield* store.codexTurn.get;
			if (current?.id === request.turnId) yield* store.codexTurn.put({
				...current,
				result
			});
		})), Effect.tap(() => stop.pipe(Effect.andThen(acknowledgeNativeOperation(operation)))), Effect.tapError((error) => error instanceof HarnessTurnError ? stop : Effect.void));
	})
});
//#endregion
export { CODEX_HOME_DIR, CODEX_PLACEHOLDER_API_KEY, CodexHarnessLive, cancelCodexTurn };
