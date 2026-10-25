import * as Crypto from "effect/Crypto";
import { Cause, Clock, Effect, Option, Schema, Semaphore } from "effect";

import {
  NativeContainerIdentity,
  NativeDirectoryEntries,
  NativeFileStat,
  type SandboxTaskNativeRequest,
} from "./sandbox-contracts/sandbox-task-native";
import { RepairSandboxTaskClient } from "./RepairSandboxTaskClient";
import {
  ProcessStatus,
  RepositoryAuthorizationGrant,
  SandboxOperationError,
  SandboxCheckpoint,
  type EnsureOptions,
  type ExecOptions,
  type NativeSandbox,
  type SandboxCommand,
  type SandboxProcessHandle,
} from "@yielded/sandbox/cloudflare/NativeSandbox";
import { MATERIALIZATION_MARKER } from "@yielded/sandbox/workspace-paths";

const Description = Schema.Struct({
  imageDigest: Schema.String,
  classIdentity: Schema.String,
  current: Schema.NullOr(NativeContainerIdentity),
  running: Schema.Boolean,
  checkpoint: Schema.NullOr(SandboxCheckpoint),
});
const Forward = Schema.Struct({ operationId: Schema.String, instanceId: Schema.String });

/** The application retains policy; native work always crosses the shared task's capacity and receipt boundary. */
export const remoteRepairSandbox = Effect.fn("remoteRepairSandbox")(function* (
  taskId: string,
  purpose: SandboxTaskNativeRequest["purpose"],
  deadlineAtMillis: number,
  start: EnsureOptions = {},
) {
  const client = yield* RepairSandboxTaskClient;
  const crypto = yield* Crypto.Crypto;
  const convert = <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ): Effect.Effect<A, SandboxOperationError, R> =>
    effect.pipe(
      Effect.catchCause((cause) =>
        Effect.failCause(
          Cause.map(
            cause,
            (cause) => new SandboxOperationError({ operation: "shared-task", cause }),
          ),
        ),
      ),
    );
  const call = <A, I>(
    action: SandboxTaskNativeRequest["action"],
    schema: Schema.Codec<A, I>,
    operationId?: string,
  ) => {
    // One constructed request keeps its receipt identity across retries and reuse.
    let acceptedId = operationId;
    const identityLock = Semaphore.makeUnsafe(1);
    return Effect.gen(function* () {
      const id = yield* Semaphore.withPermit(
        identityLock,
        Effect.gen(function* () {
          if (acceptedId === undefined)
            acceptedId = yield* crypto.randomUUIDv4().pipe(Effect.orDie);
          return acceptedId;
        }),
      );
      return yield* convert(
        client.native(
          taskId,
          {
            purpose,
            operationId: id,
            action,
            ...(start.expectedInstanceId === undefined
              ? {}
              : { expectedInstanceId: start.expectedInstanceId }),
          },
          schema,
        ),
      );
    });
  };
  const voidCall = (action: SandboxTaskNativeRequest["action"]) =>
    call(action, Schema.Null).pipe(Effect.asVoid);
  const ensure: NativeSandbox["ensure"] = (options = {}) =>
    call({ _tag: "Ensure", start: { ...start, ...options } }, NativeContainerIdentity);
  const status = (id: string, waitMillis = 0, maxBytes = 1_000_000) =>
    call(
      waitMillis === 0
        ? { _tag: "Process", processId: id, maxBytes }
        : { _tag: "WaitProcess", processId: id, waitMillis, maxBytes },
      Schema.NullOr(ProcessStatus),
    ).pipe(
      Effect.filterOrElse(
        (status) => !(status === null),
        () =>
          Effect.fail(
            new SandboxOperationError({
              operation: "process",
              code: "ENOENT",
              cause: "Process receipt is absent",
            }),
          ),
      ),
    );
  const process = (id: string): SandboxProcessHandle => ({
    id,
    status: status(id),
    kill: (signal = 15) => voidCall({ _tag: "SignalProcess", processId: id, signal }),
    waitForExit: ({ timeout }) =>
      Effect.gen(function* () {
        const until = (yield* Clock.currentTimeMillis) + timeout;

        while (true) {
          const observed = yield* status(id, Math.min(timeout, 20_000));

          if (observed.state === "exited" && observed.exitCode !== null)
            return {
              code: observed.exitCode,
              ...(observed.exit?.signal === undefined ? {} : { signal: observed.exit.signal }),
            };
          if ((yield* Clock.currentTimeMillis) >= until || observed.state === "lost")
            return yield* new SandboxOperationError({
              operation: "waitForExit",
              code: "ETIMEDOUT",
              cause: "Terminal output unavailable",
            });
        }
      }),
    outputText: ({ timeout, maxBytes, acknowledge = true }) =>
      Effect.gen(function* () {
        const until = (yield* Clock.currentTimeMillis) + timeout;

        while (true) {
          const observed = yield* status(id, Math.min(timeout, 20_000), maxBytes);

          if (observed.state === "exited" && observed.exitCode !== null) {
            if (acknowledge) yield* voidCall({ _tag: "AcknowledgeProcess", processId: id });

            return {
              stdout: observed.stdout,
              stderr: observed.stderr,
              exitCode: observed.exitCode,
              timedOut: observed.timedOut,
              truncated: observed.truncated,
            };
          }
          if ((yield* Clock.currentTimeMillis) >= until || observed.state === "lost")
            return yield* new SandboxOperationError({
              operation: "outputText",
              code: "ETIMEDOUT",
              cause: "Terminal output unavailable",
            });
        }
      }),
    waitForPort: (port, options = {}) =>
      voidCall({ _tag: "WaitPort", processId: id, port, timeout: options.timeout ?? 30_000 }),
    waitForHttp: (port, options = {}) =>
      voidCall({
        _tag: "WaitHttp",
        processId: id,
        port,
        timeout: options.timeout ?? 30_000,
        ...(options.path === undefined ? {} : { path: options.path }),
        ...(options.status === undefined ? {} : { status: options.status }),
      }),
  });
  const file = (path: string, access: "read" | "write", original: Request) =>
    Effect.gen(function* () {
      const described = yield* call({ _tag: "Describe" }, Description);
      const owner = described.current;

      if (owner === null)
        return yield* new SandboxOperationError({
          operation: "fileCapability",
          code: "ESTALE",
          cause: "The native incarnation is unavailable",
        });
      const nativeToken = yield* crypto.randomUUIDv4().pipe(Effect.orDie);
      const operationId = yield* crypto.randomUUIDv4().pipe(Effect.orDie);

      yield* call(
        { _tag: "PrepareFile", path, access, nativeToken, expectedInstanceId: owner.instanceId },
        Forward,
        operationId,
      );

      const response = yield* convert(
        client.forward(taskId, purpose, operationId, nativeToken, original),
      );

      if (response.status !== (access === "read" ? 200 : 204))
        return yield* new SandboxOperationError({
          operation: access === "read" ? "readFile" : "writeFile",
          cause: { status: response.status },
          ...(response.status === 404 ? { code: "ENOENT" } : {}),
        });

      return response;
    });
  const unsupported = (operation: string) =>
    Effect.fail(
      new SandboxOperationError({
        operation,
        code: "EPERM",
        cause: "Application cannot change shared runtime authority",
      }),
    );
  const exec = Effect.fnUntraced(function* (argv: SandboxCommand, options: ExecOptions = {}) {
    const deadlineAt = Math.min(
      deadlineAtMillis,
      options.deadlineAt ?? (yield* Clock.currentTimeMillis) + (options.timeout ?? 120_000),
    );
    const accepted = yield* call(
      { _tag: "Exec", argv, options: { ...options, deadlineAt } },
      ProcessStatus,
    );

    return process(accepted.id);
  });

  return {
    describe: call({ _tag: "Describe" }, Description),
    ensure,
    allocateProcess: (resource) =>
      call(
        { _tag: "AllocateProcess", ...(resource === undefined ? {} : { resource }) },
        Schema.String,
      ),
    acknowledgeProcess: (processId) => voidCall({ _tag: "AcknowledgeProcess", processId }),
    signalProcess: (processId, signal = 15) =>
      voidCall({ _tag: "SignalProcess", processId, signal }),
    exec,
    inspect: (argv, options) =>
      exec(argv, {
        cwd: options.cwd,
        timeout: options.timeout,
        deadlineAt: options.deadlineAt,
      }).pipe(Effect.flatMap((process) => process.outputText(options))),
    observeProcess: (id, { maxBytes, waitMillis = 0 }) =>
      call(
        waitMillis === 0
          ? { _tag: "Process", processId: id, maxBytes }
          : { _tag: "WaitProcess", processId: id, maxBytes, waitMillis },
        Schema.NullOr(ProcessStatus),
      ),
    getProcess: (id) =>
      call({ _tag: "Process", processId: id, maxBytes: 0 }, Schema.NullOr(ProcessStatus)).pipe(
        Effect.map((value) => (value === null ? Option.none() : Option.some(process(id)))),
      ),
    listProcesses: call({ _tag: "Processes" }, Schema.Array(ProcessStatus)),
    readFileString: (path) => call({ _tag: "ReadFileString", path }, Schema.String),
    readFile: (path) => file(path, "read", new Request("https://native-file", { method: "GET" })),
    lstat: (path) =>
      call({ _tag: "Lstat", path }, NativeFileStat).pipe(
        Effect.map((stat) => ({
          ...stat,
          size: BigInt(stat.size),
          accessedAt: new Date(stat.accessedAt),
          modifiedAt: new Date(stat.modifiedAt),
          changedAt: new Date(stat.changedAt),
        })),
      ),
    readDirectory: (path) => call({ _tag: "ReadDirectory", path }, NativeDirectoryEntries),
    writeFile: (path, content) =>
      typeof content === "string"
        ? voidCall({ _tag: "WriteFile", path, content })
        : file(
            path,
            "write",
            new Request("https://native-file", {
              method: "PUT",
              body: ArrayBuffer.isView(content)
                ? new Uint8Array(content.buffer, content.byteOffset, content.byteLength).slice()
                : content,
            }),
          ).pipe(
            Effect.flatMap((response) =>
              response.status === 204 ? Effect.void : unsupported("writeFile"),
            ),
          ),
    mkdir: (path, options = {}) =>
      voidCall({ _tag: "Mkdir", path, recursive: options.recursive ?? false }),
    remove: (path, options = {}) =>
      voidCall({
        _tag: "Remove",
        path,
        recursive: options.recursive ?? false,
        force: options.force ?? false,
      }),
    exists: (path) => call({ _tag: "Exists", path }, Schema.Boolean),
    forwardPort: (port, original) =>
      Effect.gen(function* () {
        if (port !== 4502) return yield* unsupported("forwardPort");
        const nativeToken = original.headers.get("authorization")?.replace(/^Bearer /, "");

        if (!nativeToken) return yield* unsupported("forwardPort");
        const described = yield* call({ _tag: "Describe" }, Description);
        const owner = described.current;

        if (owner === null)
          return yield* new SandboxOperationError({
            operation: "fileCapability",
            code: "ESTALE",
            cause: "The native incarnation is unavailable",
          });
        const materializationId = yield* call(
          { _tag: "ReadFileString", path: MATERIALIZATION_MARKER },
          Schema.String,
        );
        const operationId = yield* crypto.randomUUIDv4().pipe(Effect.orDie);

        yield* call(
          {
            _tag: "PrepareForward",
            port,
            nativeToken,
            nativeOperationId: decodeURIComponent(
              new URL(original.url).pathname.replace(/^\//, ""),
            ),
            expectedInstanceId: owner.instanceId,
            materializationId: materializationId.trim(),
          },
          Forward,
          operationId,
        );

        return yield* convert(client.forward(taskId, purpose, operationId, nativeToken, original));
      }),
    setRepositoryAuthorization: (remote, credential, expiresAt, grantId) =>
      call(
        { _tag: "RepositoryAuthorization", remote, credential, expiresAt, grantId },
        RepositoryAuthorizationGrant,
      ),
    clearRepositoryAuthorization: (grant) =>
      voidCall({ _tag: "ClearRepositoryAuthorization", ...grant }),
    lease: (id, active, renewal = false) => voidCall({ _tag: "Lease", id, active, renewal }),
    destroy: voidCall({ _tag: "Destroy" }),
    park: voidCall({ _tag: "Park" }),
    checkpoint: () => unsupported("checkpoint"),
    restore: () => unsupported("restore"),
    grantPreview: () => unsupported("grantPreview"),
    revokePreview: unsupported("revokePreview"),
    configureOutboundAuthority: () => unsupported("configureOutboundAuthority"),
    backupDirectory: () => unsupported("backupDirectory"),
    restoreDirectory: () => unsupported("restoreDirectory"),
    deleteDirectoryBackup: () => unsupported("deleteDirectoryBackup"),
    pinImage: () => unsupported("pinImage"),
  } satisfies NativeSandbox;
});
