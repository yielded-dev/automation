import { Cause, Clock, Context, Effect, Layer, Predicate, Schema } from "effect";
import { DurableObjectContainer } from "effect-cf";

import { NATIVE_GATEWAY_SOURCE } from "../harness/NativeGatewaySource";
import { NATIVE_LISTENER_PORTS } from "../NativeExecutionLimits";
import { NATIVE_RECEIPTS_DIRECTORY, WORKSPACE_DIR } from "../workspace-paths";
import { ContainerLifecycle } from "./ContainerLifecycle";
import {
  ContainerState,
  error,
  ReadinessProcessError,
  NativeProcessError,
  failureCode,
} from "./ContainerState";
import { ContainerSupervisor } from "./ContainerSupervisor";
import {
  type EnsureOptions,
  type ExecOptions,
  type HttpReadinessOptions,
  InspectionOptions,
  ProcessStatus,
  ProcessResource,
  type SandboxCommand,
} from "./NativeSandbox";

const make = Effect.gen(function* () {
  const container = yield* DurableObjectContainer.DurableObjectContainer;

  const { identity, admitted } = yield* ContainerState;
  const { inspectProcess } = yield* ContainerSupervisor;
  const { ensure } = yield* ContainerLifecycle;
  const allocateProcess = Effect.fn("SandboxContainer.allocateProcess")(function* (
    start: EnsureOptions,
    resource: ProcessResource | null,
    listenerPort: number | null,
  ) {
    if (start.expectedInstanceId === undefined)
      return yield* error(
        "allocateProcess",
        "Process allocation requires an exact container instance",
      );
    if (resource !== null && !Schema.is(ProcessResource)(resource))
      return yield* error("allocateProcess", "Invalid process resource");
    if (
      listenerPort !== null &&
      (resource !== null || !NATIVE_LISTENER_PORTS.some((port) => port === listenerPort))
    )
      return yield* error("allocateProcess", "Invalid native listener reservation");
    yield* ensure({ ...start, recovery: "exactWorkspaceRequired" });

    return yield* inspectProcess("allocateTicket", [resource, listenerPort]).pipe(
      // oxlint-disable-next-line no-restricted-properties -- authenticated process ticket boundary.
      Effect.flatMap(Schema.decodeUnknownEffect(Schema.String)),
    );
  });
  const processBelongsToBoot = Effect.fnUntraced(function* (id: string) {
    const current = yield* identity;

    if (current === null && (yield* container.running))
      return yield* error("processStatus", "Running container has no ownership receipt");

    return current !== null && id.startsWith(`process:${current.instanceId}:`);
  });
  const processStatus = (id: string, maxBytes: number) =>
    processBelongsToBoot(id).pipe(
      Effect.flatMap((owned) =>
        owned ? inspectProcess("observeJob", [id, maxBytes]) : Effect.succeed(null),
      ),
      Effect.catchIf(
        (cause) => cause instanceof NativeProcessError && cause.code === "PROCESS_RETIRED",
        () => Effect.succeed(null),
      ),
      // oxlint-disable-next-line no-restricted-properties -- native process JSON boundary.
      Effect.flatMap(Schema.decodeUnknownEffect(Schema.NullOr(ProcessStatus))),
    );
  const waitProcess = (id: string, maxBytes: number, waitMillis: number) =>
    processBelongsToBoot(id).pipe(
      Effect.flatMap((owned) =>
        owned
          ? inspectProcess("waitJob", [id, maxBytes, Math.min(20_000, Math.max(0, waitMillis))])
          : Effect.succeed(null),
      ),
      Effect.catchIf(
        (cause) => cause instanceof NativeProcessError && cause.code === "PROCESS_RETIRED",
        () => Effect.succeed(null),
      ),
      // oxlint-disable-next-line no-restricted-properties -- native process JSON boundary.
      Effect.flatMap(Schema.decodeUnknownEffect(Schema.NullOr(ProcessStatus))),
    );
  const processes = inspectProcess("listJobs", []).pipe(
    // oxlint-disable-next-line no-restricted-properties -- native process JSON boundary.
    Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(ProcessStatus))),
  );
  const exec = Effect.fn("SandboxContainer.exec")(function* (
    argv: SandboxCommand,
    input: ExecOptions,
    start: EnsureOptions,
  ) {
    if (start.expectedInstanceId === undefined)
      return yield* error("exec", "Process admission requires an exact container instance");
    const listenerPort = input.nativeListener === true ? Number(argv[3]) : undefined;

    if (
      input.nativeListener !== undefined &&
      (input.nativeListener !== true ||
        argv.length !== 5 ||
        argv[0] !== "bun" ||
        argv[1] !== "-e" ||
        argv[2] !== NATIVE_GATEWAY_SOURCE ||
        argv[4] !== NATIVE_RECEIPTS_DIRECTORY ||
        !NATIVE_LISTENER_PORTS.some((port) => port === listenerPort) ||
        input.cwd !== WORKSPACE_DIR ||
        input.env !== undefined ||
        input.timeout !== undefined ||
        input.resource !== undefined ||
        input.workspaceIdentity !== undefined)
    )
      return yield* error("exec", "Invalid native listener bootstrap");
    const current = yield* ensure({ ...start, recovery: "exactWorkspaceRequired" });

    if (input.processId === undefined)
      return yield* error("exec", "Process dispatch requires an issued ticket for this boot");
    const config = {
      id: input.processId,
      resource: input.resource,
      argv,
      cwd: input.cwd,
      env: input.env,
      instanceId: current.instanceId,
      deadlineAt: input.deadlineAt ?? null,
      workspaceIdentity: input.workspaceIdentity,
      nativeListenerPort: listenerPort,
    };

    // The daemon commits dispatch ownership before launching the command group.
    // Neither job configuration nor environment appears in launcher argv.
    return yield* inspectProcess("launchJob", [config]).pipe(
      // oxlint-disable-next-line no-restricted-properties -- authenticated native supervisor boundary.
      Effect.flatMap(Schema.decodeUnknownEffect(ProcessStatus)),
    );
  });
  const inspect = Effect.fn("SandboxContainer.inspect")(function* (
    argv: SandboxCommand,
    options: InspectionOptions,
    start: EnsureOptions,
  ) {
    // oxlint-disable-next-line no-restricted-properties -- native inspection request boundary.
    const input = yield* Schema.decodeEffect(InspectionOptions)(options);
    const deadlineAt = Math.min(
      input.deadlineAt ?? Infinity,
      (yield* Clock.currentTimeMillis) + input.timeout,
    );
    let completed = false;

    return yield* Effect.acquireUseRelease(
      admitted(allocateProcess(start, null, null)),
      (id) =>
        Effect.gen(function* () {
          yield* admitted(exec(argv, { cwd: input.cwd, processId: id, deadlineAt }, start));
          while (true) {
            const remaining = deadlineAt - (yield* Clock.currentTimeMillis);
            const result = yield* waitProcess(id, input.maxBytes, Math.max(0, remaining));

            if (result === null || result.state === "lost")
              return yield* error("inspect", "Inspection process ownership was lost");
            if (result.state === "exited") {
              completed = true;
              return {
                stdout: result.stdout,
                stderr: result.stderr,
                exitCode: result.exitCode ?? -1,
                timedOut: result.timedOut,
                truncated: result.truncated,
              };
            }
            if (remaining <= 0)
              return yield* new NativeProcessError({
                operation: "inspect",
                code: "ETIMEDOUT",
                cause: new Error("Inspection observation timed out"),
              });
          }
        }),
      (id) =>
        Effect.gen(function* () {
          if (!(yield* processBelongsToBoot(id))) return;
          if (!completed) {
            yield* inspectProcess("signalJob", [id, 9]);
            const result = yield* waitProcess(id, 0, 20_000);

            if (result !== null && result.state !== "exited")
              return yield* error("inspect", "Inspection process retirement is unconfirmed");
          }
          yield* inspectProcess("acknowledgeJob", [id]);
        }),
    );
  });

  return {
    allocateProcess: (
      start: EnsureOptions,
      resource: ProcessResource | null,
      listenerPort: number | null,
    ) => admitted(allocateProcess(start, resource, listenerPort)),
    exec: (argv: SandboxCommand, input: ExecOptions, start: EnsureOptions) =>
      admitted(exec(argv, input, start)),
    inspect,
    processes,
    processStatus,
    waitProcess,
    acknowledgeProcess: (id: string) =>
      processBelongsToBoot(id).pipe(
        Effect.flatMap((owned) =>
          owned ? inspectProcess("acknowledgeJob", [id]).pipe(Effect.asVoid) : Effect.void,
        ),
      ),
    signalProcess: (id: string, instanceId: string, signal: number) =>
      Effect.gen(function* () {
        const current = yield* identity;

        if (current?.instanceId !== instanceId) {
          if (current === null && (yield* container.running))
            return yield* error("signalProcess", "Running container has no ownership receipt");

          // A replacement boot closes the original process fence.
          return;
        }
        yield* inspectProcess("signalJob", [id, signal]);
      }),
    waitForHttp: (id: string, port: number, input: HttpReadinessOptions) =>
      Effect.gen(function* () {
        const before = yield* processStatus(id, 0);

        if (before?.state !== "running")
          return yield* new ReadinessProcessError({
            processId: id,
            code: before?.state === "exited" ? "PROCESS_EXITED" : "PROCESS_LOST",
          });
        yield* container.waitForHttp(port, input).pipe(
          Effect.catchCause((cause) =>
            Effect.gen(function* () {
              if (
                !cause.reasons.every(
                  (reason) =>
                    Cause.isFailReason(reason) &&
                    (Predicate.isTagged(reason.error, "ContainerReadinessTimeoutError") ||
                      Predicate.isTagged(reason.error, "ContainerError")),
                )
              )
                return yield* Effect.failCause(cause);
              const observation = yield* processStatus(id, 0).pipe(Effect.exit);

              if (observation._tag === "Failure")
                return yield* Effect.failCause(Cause.combine(cause, observation.cause));

              const state = observation.value?.state;

              if (state === "running") return yield* Effect.failCause(cause);

              return yield* Effect.failCause(
                Cause.map(
                  cause,
                  (original) =>
                    new ReadinessProcessError({
                      processId: id,
                      code: state === "exited" ? "PROCESS_EXITED" : "PROCESS_LOST",
                      cause: original,
                    }),
                ),
              );
            }),
          ),
        );
        const after = yield* processStatus(id, 0);

        if (after?.state !== "running")
          return yield* new ReadinessProcessError({
            processId: id,
            code: after?.state === "exited" ? "PROCESS_EXITED" : "PROCESS_LOST",
          });
      }),
    waitForPort: Effect.fn("SandboxContainer.waitForPort")(function* (
      id: string,
      port: number,
      timeout: number,
    ) {
      yield* Effect.annotateCurrentSpan({
        "sandbox.process.id": id,
        "sandbox.port": port,
      });
      const deadline = (yield* Clock.currentTimeMillis) + timeout;

      while (true) {
        const remaining = deadline - (yield* Clock.currentTimeMillis);

        const result = yield* inspectProcess("waitForPort", [
          id,
          port,
          Math.max(1, Math.min(20_000, remaining)),
        ]).pipe(Effect.exit);

        if (result._tag === "Success") return;
        if (
          failureCode(result.cause) !== "ETIMEDOUT" ||
          (yield* Clock.currentTimeMillis) >= deadline
        )
          return yield* Effect.failCause(result.cause);
      }
    }),
  };
});

export class ContainerProcesses extends Context.Service<
  ContainerProcesses,
  Effect.Success<typeof make>
>()("kommunikasie/ContainerProcesses") {
  static readonly layer = Layer.effect(ContainerProcesses, make);
}
