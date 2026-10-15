import { Cause, Clock, Effect, Exit, Layer, Predicate, Queue, Redacted, Stream } from "effect";

import {
  cancelNativeOperation,
  acknowledgeNativeOperation,
  ensureNativeOperation,
  nativeOperationRequest,
  type NativeOperation,
} from "../cloudflare/NativeOperation";
import { isSandboxFileMissing } from "../cloudflare/NativeSandbox";
import { NativeWorkspaceIdentity } from "../cloudflare/NativeWorkspaceIdentity";
import { SessionStateStore, type StoredCodexTurn } from "../cloudflare/SessionStateStore";
import { toJson } from "../Http";
import { CODEX_VERSION } from "../RuntimeVersions";
import { SessionSandbox } from "../SessionSandbox";
import { MATERIALIZATION_MARKER, REPO_DIR } from "../workspace-paths";
import { runCodexAppServerTurn } from "./CodexAppServer";
import { CODEX_MESSAGE_BYTE_LIMIT } from "./CodexEventBounds";
import {
  Harness,
  HarnessTurnError,
  type HarnessTurnRequest,
  type HarnessTurnSteer,
} from "./Harness";

/** Isolated Codex configuration and thread-state directory. */
export const CODEX_HOME_DIR = "/workspace/codex-home";

/** The outbound proxy replaces this placeholder with the real API key. */
export const CODEX_PLACEHOLDER_API_KEY = "sandbox-agent-placeholder-not-a-real-key";

const CODEX_APP_SERVER_PORT = 4500;
const MAX_BUFFERED_BYTES = 16 * 1_024 * 1_024;
const encoder = new TextEncoder();

/** Explicit cancellation is separate from losing a Worker connection. */
export const cancelCodexTurn = (turn: StoredCodexTurn) =>
  cancelNativeOperation({
    id: turn.processId,
    token: turn.token,
    deadlineAt: turn.deadlineAt,
    port: turn.configuration.port,
  });

/** The sandbox owns the process; each DO incarnation attaches with its durable capability. */
export const CodexHarnessLive: Layer.Layer<Harness> = Layer.succeed(Harness)({
  name: "codex",
  binary: "codex",
  version: `codex-cli ${CODEX_VERSION}`,

  runTurn: Effect.fn("CodexHarness.runTurn")(function* <EventError>(
    request: HarnessTurnRequest<EventError>,
  ) {
    const sandbox = yield* SessionSandbox;
    const store = yield* SessionStateStore;
    const deadlines = request.deadlinePolicy;
    const saved = yield* store.codexTurn.get;
    const recovering = saved?.id === request.turnId;
    const workspace = yield* store.pipelineTurn.get;
    const workspaceIdentity =
      (yield* NativeWorkspaceIdentity) ??
      (workspace?.id === request.turnId
        ? { path: MATERIALIZATION_MARKER, id: workspace.materializationId }
        : null);
    const requireWorkspace = Effect.gen(function* () {
      if (workspace?.id !== request.turnId) return;
      const marker = yield* sandbox.readFileString(MATERIALIZATION_MARKER).pipe(
        Effect.map((content) => content.trim()),
        Effect.catchIf(isSandboxFileMissing, () => Effect.succeed(null)),
      );

      if (marker !== workspace.materializationId)
        return yield* HarnessTurnError.make({
          reason: "workspaceLost",
          message: "The active turn's sandbox workspace was replaced",
        });
    });

    const checkpoint: StoredCodexTurn = recovering
      ? saved
      : {
          id: request.turnId,
          processId: yield* sandbox.allocateProcess(),
          token: crypto.randomUUID(),
          configuration: {
            cwd: REPO_DIR,
            env: { CODEX_HOME: CODEX_HOME_DIR },
            ...(workspaceIdentity === null ? {} : { workspaceIdentity }),
            port: CODEX_APP_SERVER_PORT,
            nativePort: 4501,
            mode: "codex",
            // Bootstrap belongs to this saved native dispatch; reattachment never resets home.
            command: [
              "sh",
              "-c",
              request.resumeToken === null
                ? 'set -eu; rm -rf -- "$CODEX_HOME"; mkdir -p -- "$CODEX_HOME"; exec "$@"'
                : 'exec "$@"',
              "codex-app-server",
              "codex",
              "app-server",
              ...(request.restrictModelRequests === true
                ? [
                    "-c",
                    'model_provider="host-controlled"',
                    "-c",
                    'model_providers.host-controlled.name="Host controlled"',
                    "-c",
                    'model_providers.host-controlled.base_url="https://api.openai.com/v1"',
                    "-c",
                    'model_providers.host-controlled.wire_api="responses"',
                    "-c",
                    "model_providers.host-controlled.requires_openai_auth=true",
                    "-c",
                    "model_providers.host-controlled.supports_websockets=false",
                    "-c",
                    "features.enable_request_compression=false",
                    "-c",
                    // Compact before the host's bounded JSON transport fills with tool history and reasoning.
                    "model_auto_compact_token_limit=160000",
                    "-c",
                    'web_search="disabled"',
                  ]
                : []),
              ...(request.sentryMcp !== undefined
                ? [
                    "-c",
                    `mcp_servers.sentry.url=${toJson(request.sentryMcp.url)}`,
                    "-c",
                    `mcp_servers.sentry.enabled_tools=${toJson(request.sentryMcp.enabledTools)}`,
                    "-c",
                    'mcp_servers.sentry.default_tools_approval_mode="auto"',
                  ]
                : []),
              "--listen",
              "ws://127.0.0.1:4501",
              "-c",
              `model=${toJson(request.model)}`,
              "-c",
              `model_reasoning_effort=${toJson(request.reasoningEffort)}`,
              ...(request.commitCoauthorTrailer === undefined
                ? []
                : [
                    "-c",
                    `developer_instructions=${toJson(`Include this trailer exactly once in every Git commit you create, preserving any other co-authors:\n\n${request.commitCoauthorTrailer}`)}`,
                  ]),
              "-c",
              'model_reasoning_summary="detailed"',
              "-c",
              "hide_agent_reasoning=false",
              "-c",
              'approval_policy="never"',
              "-c",
              'sandbox_mode="danger-full-access"',
            ],
          },
          threadParams: {
            cwd: REPO_DIR,
            model: request.model,
            approvalPolicy: "never",
            sandbox: "danger-full-access",
          },
          turnParams: {
            input: [{ type: "text", text: request.prompt }],
            effort: request.reasoningEffort,
            summary: "detailed",
            serviceTier:
              request.serviceTier === undefined
                ? request.fast
                  ? "fast"
                  : null
                : request.serviceTier,
            ...(request.outputSchema === undefined ? {} : { outputSchema: request.outputSchema }),
          },
          resumeToken: request.resumeToken,
          deadlineAt: Math.min(
            request.deadlineAtMs ?? Number.POSITIVE_INFINITY,
            (yield* Clock.currentTimeMillis) +
              deadlines.harnessTurnMillis +
              deadlines.harnessSetupMillis,
          ),
          threadId: null,
          turnId: null,
          previousTurnId: null,
          startDispatched: false,
          result: null,
        };

    if (!recovering) yield* store.codexTurn.put(checkpoint);
    const stop = cancelCodexTurn(checkpoint).pipe(
      Effect.catchCause((cause) =>
        Effect.logError("sandbox-agent Codex process cleanup failed", cause),
      ),
    );
    const operation: NativeOperation = {
      id: checkpoint.processId,
      token: checkpoint.token,
      deadlineAt: checkpoint.deadlineAt,
      ...checkpoint.configuration,
    };

    if (checkpoint.result !== null) {
      yield* stop;
      yield* acknowledgeNativeOperation(operation);

      return checkpoint.result;
    }
    let activeSteer: HarnessTurnSteer | undefined;
    let steeringPublished = false;
    const steer: HarnessTurnSteer = (input) =>
      Effect.suspend(() =>
        activeSteer === undefined
          ? Effect.succeed({
              _tag: "Unknown" as const,
              reason: "The native connection is recovering",
            })
          : activeSteer(input),
      );
    const connect = Effect.gen(function* () {
      yield* requireWorkspace;
      yield* ensureNativeOperation(operation);
      const response = yield* sandbox.forwardPort(
        operation.port,
        nativeOperationRequest(operation),
      );
      const socket = response.webSocket;

      if (socket === null) {
        return yield* HarnessTurnError.make({
          reason: response.status >= 500 ? "connectionLost" : "turnFailed",
          message: `Codex WebSocket upgrade failed: HTTP ${response.status}`,
        });
      }
      const queue = yield* Queue.bounded<
        { readonly text: string; readonly byteLength: number },
        HarnessTurnError | Cause.Done
      >(256);
      let bufferedBytes = 0;
      const fail = (reason: HarnessTurnError["reason"], message: string) => {
        Queue.failCauseUnsafe(queue, Cause.fail(HarnessTurnError.make({ reason, message })));
        socket.close(1009, "Codex stream failed");
      };
      const onMessage = (event: MessageEvent) => {
        if (!Predicate.isString(event.data)) {
          fail("malformedEvent", "Codex sent a non-text WebSocket message");

          return;
        }
        if (event.data.length > CODEX_MESSAGE_BYTE_LIMIT) {
          fail("oversizedEvent", "Codex WebSocket exceeded the input byte limit");

          return;
        }
        const byteLength = encoder.encode(event.data).byteLength;

        if (
          byteLength > CODEX_MESSAGE_BYTE_LIMIT ||
          bufferedBytes + byteLength > MAX_BUFFERED_BYTES
        ) {
          fail("oversizedEvent", "Codex WebSocket exceeded the input byte limit");

          return;
        }
        if (!Queue.offerUnsafe(queue, { text: event.data, byteLength })) {
          fail("oversizedEvent", "Codex WebSocket exceeded the input queue limit");

          return;
        }
        bufferedBytes += byteLength;
      };
      const onClose = () => {
        Queue.endUnsafe(queue);
      };
      const onError = () => fail("connectionLost", "Codex WebSocket connection failed");

      yield* Effect.acquireRelease(
        Effect.sync(() => {
          socket.addEventListener("message", onMessage);
          socket.addEventListener("close", onClose);
          socket.addEventListener("error", onError);
          socket.accept();
          retryCause = undefined;
        }),
        () =>
          Effect.sync(() => {
            socket.removeEventListener("message", onMessage);
            socket.removeEventListener("close", onClose);
            socket.removeEventListener("error", onError);
            socket.close(1000, "Codex turn ended");
          }),
      );

      return yield* runCodexAppServerTurn(
        {
          ...request,
          onSteeringReady: (nativeSteer) =>
            Effect.gen(function* () {
              activeSteer = nativeSteer;
              if (!steeringPublished && request.onSteeringReady !== undefined) {
                yield* request.onSteeringReady(steer);
                steeringPublished = true;
              }
            }),
        },
        {
          apiKey: Redacted.make(CODEX_PLACEHOLDER_API_KEY),
          messages: Stream.fromQueue(queue).pipe(
            Stream.map((message) => {
              bufferedBytes -= message.byteLength;

              return message.text;
            }),
          ),
          send: (message) =>
            Effect.try({
              try: () => socket.send(toJson(message)),
              catch: () =>
                HarnessTurnError.make({
                  reason: "connectionLost",
                  message: "Could not send a Codex request",
                }),
            }),
        },
      );
    });

    const remaining = checkpoint.deadlineAt - (yield* Clock.currentTimeMillis);
    let retryCause: Cause.Cause<Effect.Error<typeof connect>> | undefined;
    const deadlineExceeded = () => {
      const cause = Cause.fail(
        HarnessTurnError.make({
          reason: "deadlineExceeded",
          message: `Codex turn exceeded ${deadlines.harnessTurnMillis} milliseconds`,
        }),
      );

      return Effect.failCause(retryCause === undefined ? cause : Cause.combine(cause, retryCause));
    };

    if (remaining <= 0) {
      yield* stop;

      return yield* deadlineExceeded();
    }
    const attach = Effect.gen(function* () {
      while (true) {
        const attempt = yield* Effect.scoped(connect).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              activeSteer = undefined;
            }),
          ),
          Effect.exit,
        );

        if (Exit.isSuccess(attempt)) return attempt.value;
        const cause = attempt.cause;

        if (Cause.hasDies(cause) || Cause.hasInterrupts(cause))
          return yield* Effect.failCause(cause);

        if (
          cause.reasons.length > 0 &&
          cause.reasons.every(
            (reason) =>
              Cause.isFailReason(reason) &&
              ((reason.error instanceof HarnessTurnError &&
                (reason.error.reason === "missingTerminalEvent" ||
                  reason.error.reason === "connectionLost")) ||
                Predicate.isTagged(reason.error, "SandboxOperationError")),
          )
        ) {
          // The next connection verifies the workspace before dispatch. Its marker
          // may be temporarily unreadable while the sandbox transport recovers.
          // Retain only the latest attempt for the terminal deadline diagnostic.
          retryCause = cause;
          yield* Effect.sleep("1 second");
          continue;
        }

        yield* requireWorkspace.pipe(
          Effect.catchCause((observationCause) =>
            Effect.failCause(Cause.combine(cause, observationCause)),
          ),
        );

        return yield* Effect.failCause(cause);
      }
    });

    return yield* attach.pipe(
      Effect.timeoutOrElse({ duration: remaining, orElse: deadlineExceeded }),
      Effect.tap((result) =>
        Effect.gen(function* () {
          const current = yield* store.codexTurn.get;

          if (current?.id === request.turnId) yield* store.codexTurn.put({ ...current, result });
        }),
      ),
      // Losing the owning DO does not own cancellation. Explicit task cancellation
      // stops this durable process; the sandbox also enforces the original deadline.
      Effect.tap(() => stop.pipe(Effect.andThen(acknowledgeNativeOperation(operation)))),
      Effect.tapError((error) => (error instanceof HarnessTurnError ? stop : Effect.void)),
    );
  }),
});
