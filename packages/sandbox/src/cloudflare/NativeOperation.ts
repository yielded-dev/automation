import { Cause, Effect, Option, Predicate, Schema } from "effect";

import { sanitizeCause } from "../observability/FailureDiagnostic";

import { HarnessTurnError } from "../harness/Harness";
import { NATIVE_GATEWAY_SOURCE } from "../harness/NativeGatewaySource";
import { toJson } from "../Http";
import { SessionSandbox } from "../SessionSandbox";
import { NATIVE_RECEIPTS_DIRECTORY, WORKSPACE_DIR } from "../workspace-paths";
import {
  SandboxOperationError,
  type ProcessResource,
  type SandboxProcessHandle,
} from "./NativeSandbox";
import type { NativeWorkspaceIdentityData } from "./NativeWorkspaceIdentity";

export interface NativeOperationIdentity {
  readonly id: string;
  readonly token: string;
  readonly port: number;
  readonly deadlineAt: number;
}

/** Keep this input at the operation's durable authority before calling ensure. */
export interface NativeOperation extends NativeOperationIdentity {
  readonly command: readonly [string, ...Array<string>];
  readonly resource?: ProcessResource;
  readonly cwd: string;
  readonly env?: Readonly<Record<string, string>>;
  /** Native dispatch must still own this exact disposable workspace. */
  readonly workspaceIdentity?: NativeWorkspaceIdentityData;
  readonly mode: "command" | "codex";
  readonly nativePort?: number;
  readonly maxOutputBytes?: number;
}

const NativeState = Schema.Literals(["running", "completed", "cancelled", "quarantined"]);
const NativeCommandStatus = Schema.Struct({
  state: NativeState,
  stdout: Schema.String,
  stderr: Schema.String,
  exitCode: Schema.NullOr(Schema.Int),
  timedOut: Schema.Boolean,
  truncated: Schema.Boolean,
});

export type NativeCommandStatus = typeof NativeCommandStatus.Type;
const Reply = Schema.Struct({
  id: Schema.String,
  result: Schema.optionalKey(Schema.Unknown),
  error: Schema.optionalKey(Schema.Struct({ message: Schema.String })),
});

export const nativeOperationRequest = (operation: NativeOperationIdentity) =>
  new Request(`http://localhost:${operation.port}/${encodeURIComponent(operation.id)}`, {
    headers: { Upgrade: "websocket", Authorization: `Bearer ${operation.token}` },
  });

const expectedTransportFailure = <E>(cause: Cause.Cause<E>) =>
  cause.reasons.length > 0 &&
  cause.reasons.every(
    (reason) =>
      Cause.isFailReason(reason) && Predicate.isTagged(reason.error, "SandboxOperationError"),
  );

const connect = Effect.fn("NativeOperation.connect")(
  function* (operation: NativeOperationIdentity, bootstrap = true) {
    const sandbox = yield* SessionSandbox;
    const directory = NATIVE_RECEIPTS_DIRECTORY;

    while (true) {
      // Discover the file-backed listener before forwarding to its private native port.
      const listeningProcess = (yield* sandbox.listProcesses).find(
        ({ state, command }) =>
          state === "running" &&
          command[0] === "bun" &&
          command[1] === "-e" &&
          command.at(-2) === String(operation.port) &&
          command.at(-1) === directory,
      );

      let listener = listeningProcess
        ? yield* sandbox.getProcess(listeningProcess.id)
        : Option.none<SandboxProcessHandle>();

      if (Option.isNone(listener)) {
        if (!bootstrap)
          return yield* HarnessTurnError.make({
            reason: "connectionLost",
            message: "Native listener has stopped",
          });
        // Binding elects one owner. A lost acknowledgement can safely retry startup.
        // The listener serves operations with their own immutable deadlines.
        listener = yield* sandbox
          .exec(["bun", "-e", NATIVE_GATEWAY_SOURCE, String(operation.port), directory], {
            cwd: WORKSPACE_DIR,
            nativeListener: true,
          })
          .pipe(
            Effect.map(Option.some),
            Effect.catchCause((cause) =>
              expectedTransportFailure(cause)
                ? Effect.logWarning(
                    "Native gateway bootstrap acknowledgement was lost",
                    sanitizeCause(cause, { operation: "NativeOperation.bootstrap" }),
                  ).pipe(Effect.as(Option.none<SandboxProcessHandle>()))
                : Effect.failCause(cause),
            ),
          );
      }
      if (Option.isNone(listener)) {
        yield* Effect.sleep("100 millis");
        continue;
      }
      // Process admission precedes socket binding. Probe inside the container so
      // normal bootstrap does not throw through the host's HTTP reporting boundary.
      const response = yield* listener.value.waitForPort(operation.port, { timeout: 10_000 }).pipe(
        Effect.andThen(sandbox.forwardPort(operation.port, nativeOperationRequest(operation))),
        Effect.catchCause((cause) =>
          expectedTransportFailure(cause)
            ? Effect.logWarning(
                "Native gateway connection failed",
                sanitizeCause(cause, { operation: "NativeOperation.forwardPort" }),
              ).pipe(Effect.as(null))
            : Effect.failCause(cause),
        ),
      );

      if (response !== null && response.webSocket !== null) return response.webSocket;
      yield* Effect.sleep("100 millis");
    }
  },
  Effect.timeoutOrElse({
    duration: "10 seconds",
    orElse: () =>
      HarnessTurnError.make({
        reason: "connectionLost",
        message: "Native gateway could not be reached",
      }),
  }),
);

const open = Effect.fn("NativeOperation.open")(function* (
  operation: NativeOperationIdentity,
  bootstrap = true,
) {
  const socket = yield* Effect.acquireRelease(
    connect(operation, bootstrap),
    (socket) => Effect.sync(() => socket.close(1000, "Native operation receipt read")),
    { interruptible: true },
  );

  yield* Effect.try({
    try: () => socket.accept(),
    catch: () =>
      HarnessTurnError.make({
        reason: "connectionLost",
        message: "Native gateway disconnected",
      }),
  });

  return Effect.fn("NativeOperation.rpc")(function* (method: string, params: unknown) {
    const text = yield* Effect.callback<string, HarnessTurnError>((resume) => {
      const cleanup = () => {
        socket.removeEventListener("message", received);
        socket.removeEventListener("error", failed);
        socket.removeEventListener("close", failed);
      };
      const failed = () => {
        cleanup();
        resume(
          HarnessTurnError.make({
            reason: "connectionLost",
            message: "Native gateway disconnected",
          }),
        );
      };
      const received = (event: MessageEvent) => {
        if (typeof event.data !== "string" || event.data.length > 8 * 1_024 * 1_024) failed();
        else {
          cleanup();
          resume(Effect.succeed(event.data));
        }
      };

      socket.addEventListener("message", received);
      socket.addEventListener("error", failed);
      socket.addEventListener("close", failed);
      try {
        socket.send(toJson({ id: method, method, params }));
      } catch {
        failed();
      }

      // Callback cleanup runs only on interruption; settlement releases listeners above.
      return Effect.sync(cleanup);
    }).pipe(
      Effect.timeoutOrElse({
        duration: "10 seconds",
        orElse: () =>
          HarnessTurnError.make({
            reason: "connectionLost",
            message: "Native gateway did not reply",
          }),
      }),
    );
    const reply = yield* Schema.decodeEffect(Schema.fromJsonString(Reply))(text).pipe(
      Effect.mapError(() =>
        HarnessTurnError.make({
          reason: "malformedEvent",
          message: "Invalid native gateway receipt",
        }),
      ),
    );

    if (reply.id !== method)
      return yield* HarnessTurnError.make({
        reason: "malformedEvent",
        message: "Unexpected native gateway receipt",
      });
    if (reply.error !== undefined)
      return yield* HarnessTurnError.make({ reason: "turnFailed", message: reply.error.message });

    return reply.result;
  });
});

const ensureConnection = Effect.fn("NativeOperation.ensure")(function* (
  operation: NativeOperation,
) {
  const rpc = yield* open(operation);
  const { token: _token, port: _port, ...configuration } = operation;
  const result = yield* rpc("gateway/ensure", configuration);
  // Native WebSocket JSON is an untyped process boundary.
  // oxlint-disable-next-line no-restricted-properties
  const receipt = yield* Schema.decodeUnknownEffect(
    Schema.Struct({ state: NativeState, timedOut: Schema.Boolean }),
  )(result).pipe(
    Effect.mapError(() =>
      HarnessTurnError.make({
        reason: "malformedEvent",
        message: "Invalid native operation state",
      }),
    ),
  );

  if (receipt.state === "quarantined" || receipt.state === "cancelled")
    return yield* HarnessTurnError.make({
      reason: receipt.timedOut ? "deadlineExceeded" : "turnFailed",
      message: `Native operation is ${receipt.state}; its input was not replayed`,
    });

  return rpc;
});

export const ensureNativeOperation = (operation: NativeOperation) =>
  ensureConnection(operation).pipe(Effect.asVoid, Effect.scoped);

/** Status is authoritative at the native owner, including completed output after reconnect. */
export const readNativeCommand = Effect.fn("NativeOperation.readCommand")(function* (
  operation: NativeOperation,
) {
  const rpc = yield* ensureConnection(operation);
  const result = yield* rpc("gateway/status", {});

  // Native WebSocket JSON is an untyped process boundary.
  // oxlint-disable-next-line no-restricted-properties
  return yield* Schema.decodeUnknownEffect(NativeCommandStatus)(result).pipe(
    Effect.mapError(() =>
      HarnessTurnError.make({
        reason: "malformedEvent",
        message: "Invalid native command receipt",
      }),
    ),
  );
}, Effect.scoped);

const listeningProcesses = Effect.fnUntraced(function* (port: number) {
  const sandbox = yield* SessionSandbox;

  return yield* sandbox.listProcesses.pipe(
    Effect.map((jobs) =>
      jobs.filter(
        (job) =>
          job.state === "running" &&
          job.command[0] === "bun" &&
          job.command[1] === "-e" &&
          job.command.at(-2) === String(port) &&
          job.command.at(-1) === NATIVE_RECEIPTS_DIRECTORY,
      ),
    ),
  );
});
const retiredProcess = (error: SandboxOperationError) => error.code === "PROCESS_RETIRED";
const existingListenerCall = Effect.fnUntraced(function* (
  operation: NativeOperationIdentity,
  method: "gateway/cancel" | "gateway/acknowledge",
) {
  if ((yield* listeningProcesses(operation.port)).length === 0) return false;
  const observation = yield* Effect.gen(function* () {
    const rpc = yield* open(operation, false);

    yield* rpc(method, {});
  }).pipe(Effect.scoped, Effect.exit);

  if (observation._tag === "Success") return true;
  if (
    !observation.cause.reasons.every(
      (reason) =>
        Cause.isFailReason(reason) &&
        Predicate.isTagged(reason.error, "HarnessTurnError") &&
        reason.error.reason === "connectionLost",
    )
  )
    return yield* Effect.failCause(observation.cause);
  const listeners = yield* listeningProcesses(operation.port).pipe(Effect.exit);

  if (listeners._tag === "Failure")
    return yield* Effect.failCause(Cause.combine(observation.cause, listeners.cause));
  if (listeners.value.length !== 0) return yield* Effect.failCause(observation.cause);

  return false;
});

/** Fences an operation even when its ensure request has not arrived yet. */
export const cancelNativeOperation = Effect.fn("NativeOperation.cancel")(function* (
  operation: NativeOperationIdentity,
) {
  const sandbox = yield* SessionSandbox;

  // The reserved process ticket fences cancellation before any listener exists.
  yield* sandbox
    .signalProcess(operation.id)
    .pipe(Effect.catchIf(retiredProcess, () => Effect.void));
  yield* existingListenerCall(operation, "gateway/cancel");
}, Effect.scoped);

/** The caller must retain the terminal result before releasing native output. */
export const acknowledgeNativeOperation = Effect.fn("NativeOperation.acknowledge")(function* (
  operation: NativeOperationIdentity,
) {
  const sandbox = yield* SessionSandbox;
  const process = yield* sandbox
    .getProcess(operation.id)
    .pipe(
      Effect.catchCause((cause) =>
        cause.reasons.length > 0 &&
        cause.reasons.every((reason) => Cause.isFailReason(reason) && retiredProcess(reason.error))
          ? Effect.succeed(Option.none())
          : Effect.failCause(cause),
      ),
    );

  if (Option.isSome(process)) yield* process.value.waitForExit({ timeout: 10_000 });
  if (yield* existingListenerCall(operation, "gateway/acknowledge")) return;
  // A completed gateway deadline can precede child dispatch. Close that
  // reservation before acknowledgement if its listener has already stopped.
  if (Option.isNone(process))
    yield* sandbox
      .signalProcess(operation.id)
      .pipe(Effect.catchIf(retiredProcess, () => Effect.void));
  yield* sandbox.acknowledgeProcess(operation.id);
}, Effect.scoped);

/** Close only an idle listener; completed operation receipts remain replayable. */
export const quiesceNativeOperation = Effect.fn("NativeOperation.quiesce")(function* (
  operation: NativeOperationIdentity,
) {
  const sandbox = yield* SessionSandbox;
  const listeners = yield* listeningProcesses(operation.port);

  if (listeners.length === 0) return;
  const observed = yield* Effect.gen(function* () {
    const rpc = yield* open(operation, false);
    const result = yield* rpc("gateway/quiesce", {});

    // Native gateway JSON is an untyped process boundary.
    // oxlint-disable-next-line no-restricted-properties
    const reply = yield* Schema.decodeUnknownEffect(Schema.Struct({ quiesced: Schema.Boolean }))(
      result,
    ).pipe(
      Effect.catchCause((cause) =>
        Effect.failCause(
          Cause.map(
            cause,
            (cause) => new SandboxOperationError({ operation: "NativeOperation.quiesce", cause }),
          ),
        ),
      ),
    );

    return { ...reply, listeners };
  }).pipe(Effect.scoped);

  if (!observed.quiesced) return;
  for (const listener of observed.listeners) {
    const process = yield* sandbox.getProcess(listener.id);

    if (Option.isSome(process)) yield* process.value.waitForExit({ timeout: 10_000 });
  }
});
