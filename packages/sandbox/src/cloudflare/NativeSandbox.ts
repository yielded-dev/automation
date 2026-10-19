import type {
  DirectoryBackupRecord,
  FileContent,
  SandboxDirectoryEntry,
} from "@cloudflare/sandbox";
import { Cause, Clock, Context, Data, Effect, Option, Schema } from "effect";
import { DurableObjectNamespace, type ContainerFiles } from "effect-cf";

import { causeFromFailureDiagnostic, FailureDiagnostic } from "../observability/FailureDiagnostic";
import { NATIVE_OUTPUT_LIMIT_BYTES } from "../NativeExecutionLimits";

export type SandboxCommand = readonly [string, ...Array<string>];
export type SandboxFileStat = ContainerFiles.FileStat;
export const ProcessResource = Schema.Struct({
  key: Schema.String.check(Schema.isPattern(/^[a-z][a-z0-9-]{0,63}$/u)),
  generation: Schema.Natural.check(Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)),
  materializationId: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
});
export type ProcessResource = typeof ProcessResource.Type;
export interface ExecOptions {
  /** An issued ticket retained by the caller before dispatch. */
  readonly processId?: string;
  /** Latest desired generation of one resource in this dedicated sandbox owner. */
  readonly resource?: ProcessResource;
  readonly cwd?: string;
  readonly env?: Readonly<Record<string, string>>;
  readonly timeout?: number;
  /** Bound once by the adapter; retries cannot extend an accepted deadline. */
  readonly deadlineAt?: number | null;
  readonly workspaceIdentity?: { readonly path: string; readonly id: string };
  /** Only the fixed internal gateway bootstrap may use recoverable listener capacity. */
  readonly nativeListener?: true;
}
export const InspectionOptions = Schema.Struct({
  cwd: Schema.optionalKey(Schema.String),
  timeout: Schema.Finite.check(Schema.isGreaterThan(0)),
  maxBytes: Schema.Natural.check(Schema.isLessThanOrEqualTo(NATIVE_OUTPUT_LIMIT_BYTES)),
  deadlineAt: Schema.optionalKey(Schema.Finite),
});
export type InspectionOptions = typeof InspectionOptions.Type;
/** A filesystem checkpoint. Running processes are not restored. */
export const SandboxCheckpoint = Schema.Struct({
  kind: Schema.Literal("container"),
  snapshotId: Schema.String,
  imageDigest: Schema.String,
  classIdentity: Schema.String,
  instanceId: Schema.String,
  capturedAt: Schema.Finite,
  expiresAt: Schema.Finite,
});
export type SandboxCheckpoint = typeof SandboxCheckpoint.Type;
export const WorkspaceCheckpoint = Schema.Struct({
  ...SandboxCheckpoint.fields,
  setupFingerprint: Schema.String,
  head: Schema.String,
  materializationId: Schema.String,
});
export type WorkspaceCheckpoint = typeof WorkspaceCheckpoint.Type;
export const CheckpointInput = Schema.Struct({
  instanceId: Schema.String,
  /** Increase for each save in this boot. Only the latest receipt is retained. */
  sequence: Schema.Natural.check(Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)),
});
export type CheckpointInput = typeof CheckpointInput.Type;
export interface EnsureOptions {
  readonly expectedInstanceId?: string;
  readonly size?: "standard" | "big";
  readonly instance?: ContainerStartupOptions["instance"];
  readonly imageKey?: string;
  readonly checkpoint?: SandboxCheckpoint;
  readonly recovery?: "freshAllowed" | "exactWorkspaceRequired";
  readonly idleMillis?: 300_000 | 1_800_000;
}
export interface ContainerIdentity {
  readonly instanceId: string;
  readonly imageDigest: string;
  readonly restoredSnapshotId: string | null;
  readonly timings: {
    /** Legacy boot-intent boundary, before capacity reservation. Use the nativeStart span for the call. */
    readonly nativeStartRequestedAt: number;
    /** Synchronous start() returned; the container may still be starting. */
    readonly nativeStartReturnedAt: number | null;
    /** Authenticated process supervisor readiness, independent of application services. */
    readonly ownerReadyAt: number | null;
  };
}
export const RepositoryAuthorizationGrant = Schema.Struct({
  remote: Schema.String,
  instanceId: Schema.String,
  grantId: Schema.String,
});
export type RepositoryAuthorizationGrant = typeof RepositoryAuthorizationGrant.Type;
export interface ContainerDescription {
  readonly imageDigest: string;
  readonly classIdentity: string;
  readonly current: ContainerIdentity | null;
  readonly running: boolean;
  readonly checkpoint: SandboxCheckpoint | null;
}
export const ProcessStatus = Schema.Struct({
  id: Schema.String,
  command: Schema.Array(Schema.String),
  instanceId: Schema.String,
  state: Schema.Literals(["running", "exited", "lost"]),
  exit: Schema.optionalKey(
    Schema.Struct({ code: Schema.Int, signal: Schema.optionalKey(Schema.String) }),
  ),
  stdout: Schema.String,
  stderr: Schema.String,
  exitCode: Schema.NullOr(Schema.Int),
  timedOut: Schema.Boolean,
  truncated: Schema.Boolean,
  deadlineAt: Schema.NullOr(Schema.Finite),
  completionRevision: Schema.NullOr(Schema.Natural),
});
export type ProcessStatus = typeof ProcessStatus.Type;
export interface ProcessOutput<A = string> {
  readonly stdout: A;
  readonly stderr: A;
  readonly exitCode: number;
  readonly timedOut: boolean;
  readonly truncated: boolean;
}
export interface ProcessExit {
  readonly code: number;
  readonly signal?: string;
}
export interface HttpReadinessOptions {
  readonly timeout?: number;
  readonly path?: string;
  readonly status?: { readonly min: number; readonly max: number };
}
export class SandboxOperationError extends Data.TaggedError("SandboxOperationError")<{
  readonly operation: string;
  readonly cause: unknown;
  readonly code?: string;
  readonly binding?: string;
  readonly instance?: string;
}> {
  override get message() {
    return `Sandbox operation ${this.operation} failed`;
  }
}
export const isSandboxFileMissing = (error: SandboxOperationError): boolean =>
  error.code === "ENOENT";
export type NativeReply<A> =
  | { readonly _tag: "Success"; readonly value: A }
  | { readonly _tag: "Failure"; readonly diagnostic: FailureDiagnostic; readonly code?: string };

/** The internal native RPC boundary. Native streams are deliberately transferred, never JSON encoded. */
export interface PreviewGrant {
  readonly port: 5173;
  readonly token: string;
  readonly materializationId: string;
  readonly generation: number;
  readonly instanceId: string;
  readonly processId: string;
  readonly expiresAt?: number;
}
export const PreviewRoute = Schema.Struct({
  sandboxId: Schema.String,
  port: Schema.Literal(5173),
  materializationId: Schema.String,
  generation: Schema.Int,
});
export type PreviewRoute = typeof PreviewRoute.Type;
export const PREVIEW_TOKEN_HEADER = "x-sandbox-preview-token";
export const NATIVE_PORT_HOST = "sandbox-native.internal";
export const NATIVE_INSTANCE_HEADER = "x-sandbox-native-instance";
export const CAPACITY_NAME = "sandbox-capacity";
export interface NativeSandboxRpc {
  fetch(request: Request): Promise<Response>;
  locate(name: string): Promise<NativeReply<void>>;
  describe(options: EnsureOptions): Promise<NativeReply<ContainerDescription>>;
  ensure(options: EnsureOptions): Promise<NativeReply<ContainerIdentity>>;
  allocateProcess(
    start: EnsureOptions,
    resource: ProcessResource | null,
    listenerPort: number | null,
  ): Promise<NativeReply<string>>;
  exec(
    argv: SandboxCommand,
    options: ExecOptions,
    start: EnsureOptions,
  ): Promise<NativeReply<ProcessStatus>>;
  inspect(
    argv: SandboxCommand,
    options: InspectionOptions,
    start: EnsureOptions,
  ): Promise<NativeReply<ProcessOutput>>;
  process(id: string, maxBytes: number): Promise<NativeReply<ProcessStatus | null>>;
  waitProcess(
    id: string,
    maxBytes: number,
    waitMillis: number,
  ): Promise<NativeReply<ProcessStatus | null>>;
  acknowledgeProcess(id: string): Promise<NativeReply<void>>;
  processes(): Promise<NativeReply<ReadonlyArray<ProcessStatus>>>;
  signalProcess(id: string, instanceId: string, signal: number): Promise<NativeReply<void>>;
  waitForHttp(id: string, port: number, options: HttpReadinessOptions): Promise<NativeReply<void>>;
  exists(path: string, start: EnsureOptions): Promise<NativeReply<boolean>>;
  waitForPort(id: string, port: number, timeout: number): Promise<NativeReply<void>>;
  grantPreview(input: PreviewGrant): Promise<NativeReply<void>>;
  revokePreview(): Promise<NativeReply<void>>;
  registerPreview(
    hash: string,
    route: PreviewRoute | null,
    expiresAt: number,
  ): Promise<NativeReply<void>>;
  resolvePreview(hash: string): Promise<NativeReply<PreviewRoute | null>>;
  readFileString(path: string, start: EnsureOptions): Promise<NativeReply<string>>;
  readFile(path: string, start: EnsureOptions): Promise<NativeReply<Response>>;
  lstat(path: string, start: EnsureOptions): Promise<NativeReply<SandboxFileStat>>;
  writeFile(path: string, content: FileContent, start: EnsureOptions): Promise<NativeReply<void>>;
  readDirectory(
    path: string,
    start: EnsureOptions,
  ): Promise<NativeReply<ReadonlyArray<SandboxDirectoryEntry>>>;
  mkdir(path: string, recursive: boolean, start: EnsureOptions): Promise<NativeReply<void>>;
  remove(
    path: string,
    recursive: boolean,
    force: boolean,
    start: EnsureOptions,
  ): Promise<NativeReply<void>>;
  checkpoint(input: CheckpointInput): Promise<NativeReply<SandboxCheckpoint>>;
  restore(
    checkpoint: SandboxCheckpoint,
    start: EnsureOptions,
  ): Promise<NativeReply<ContainerIdentity>>;
  configureOutboundAuthority(authority: unknown): Promise<NativeReply<void>>;
  setRepositoryAuthorization(
    remote: string,
    credential: string | null,
    expiresAt: number | null,
    instanceId: string,
    grantId: string,
  ): Promise<NativeReply<void>>;
  backupDirectory(
    input: {
      readonly dir: string;
      readonly name?: string;
    },
    start: EnsureOptions,
  ): Promise<NativeReply<DirectoryBackupRecord>>;
  restoreDirectory(
    receipt: DirectoryBackupRecord,
    start: EnsureOptions,
  ): Promise<NativeReply<void>>;
  deleteDirectoryBackup(receipt: DirectoryBackupRecord): Promise<NativeReply<void>>;
  destroy(): Promise<NativeReply<void>>;
  park(): Promise<NativeReply<void>>;
  pinImage(imageDigest: string | null): Promise<NativeReply<void>>;
  lease(
    id: string,
    active: boolean,
    instanceId: string,
    renewal: boolean,
  ): Promise<NativeReply<void>>;
  capacityInUse(instanceId: string): Promise<NativeReply<boolean>>;
  reserveCapacity(id: string, instanceId: string): Promise<NativeReply<boolean>>;
  releaseCapacity(id: string, instanceId: string): Promise<NativeReply<void>>;
}
export interface SandboxProcessHandle {
  readonly id: string;
  readonly status: Effect.Effect<ProcessStatus, SandboxOperationError>;
  readonly kill: (signal?: number) => Effect.Effect<void, SandboxOperationError>;
  readonly waitForExit: (options: {
    readonly timeout: number;
  }) => Effect.Effect<ProcessExit, SandboxOperationError>;
  readonly outputText: (options: {
    readonly timeout: number;
    readonly maxBytes: number;
    /** Durable owners acknowledge only after committing the terminal result. */
    readonly acknowledge?: boolean;
  }) => Effect.Effect<ProcessOutput, SandboxOperationError>;
  readonly waitForPort: (
    port: number,
    options?: { readonly timeout?: number },
  ) => Effect.Effect<void, SandboxOperationError>;
  readonly waitForHttp: (
    port: number,
    options?: HttpReadinessOptions,
  ) => Effect.Effect<void, SandboxOperationError>;
}
export interface NativeSandbox {
  readonly describe: Effect.Effect<ContainerDescription, SandboxOperationError>;
  readonly ensure: (
    options?: EnsureOptions,
  ) => Effect.Effect<ContainerIdentity, SandboxOperationError>;
  /** Allocation does not dispatch a child or consume an execution slot. */
  readonly allocateProcess: (
    resource?: ProcessResource,
  ) => Effect.Effect<string, SandboxOperationError>;
  readonly acknowledgeProcess: (id: string) => Effect.Effect<void, SandboxOperationError>;
  readonly signalProcess: (
    id: string,
    signal?: number,
  ) => Effect.Effect<void, SandboxOperationError>;
  readonly exec: (
    argv: SandboxCommand,
    options?: ExecOptions,
  ) => Effect.Effect<SandboxProcessHandle, SandboxOperationError>;
  /** Bounded, replayable host inspections; mutations must retain an exec receipt. */
  readonly inspect: (
    argv: SandboxCommand,
    options: InspectionOptions,
  ) => Effect.Effect<ProcessOutput, SandboxOperationError>;
  readonly getProcess: (
    id: string,
  ) => Effect.Effect<Option.Option<SandboxProcessHandle>, SandboxOperationError>;
  /** Observe bounded bytes without acknowledging output or requiring process completion. */
  readonly observeProcess: (
    id: string,
    options: { readonly maxBytes: number; readonly waitMillis?: number },
  ) => Effect.Effect<ProcessStatus | null, SandboxOperationError>;
  readonly listProcesses: Effect.Effect<ReadonlyArray<ProcessStatus>, SandboxOperationError>;
  readonly readFileString: (path: string) => Effect.Effect<string, SandboxOperationError>;
  readonly readFile: (path: string) => Effect.Effect<Response, SandboxOperationError>;
  readonly lstat: (path: string) => Effect.Effect<SandboxFileStat, SandboxOperationError>;
  readonly writeFile: (
    path: string,
    content: FileContent,
  ) => Effect.Effect<void, SandboxOperationError>;
  readonly readDirectory: (
    path: string,
  ) => Effect.Effect<ReadonlyArray<SandboxDirectoryEntry>, SandboxOperationError>;
  readonly mkdir: (
    path: string,
    options?: { readonly recursive?: boolean },
  ) => Effect.Effect<void, SandboxOperationError>;
  readonly remove: (
    path: string,
    options?: { readonly recursive?: boolean; readonly force?: boolean },
  ) => Effect.Effect<void, SandboxOperationError>;
  readonly exists: (path: string) => Effect.Effect<boolean, SandboxOperationError>;
  readonly forwardPort: (
    port: number,
    request: Request,
  ) => Effect.Effect<Response, SandboxOperationError>;
  readonly grantPreview: (input: PreviewGrant) => Effect.Effect<void, SandboxOperationError>;
  readonly revokePreview: Effect.Effect<void, SandboxOperationError>;
  readonly checkpoint: (
    input: CheckpointInput,
  ) => Effect.Effect<SandboxCheckpoint, SandboxOperationError>;
  /** Restore this checkpoint exactly; never fall back to an empty image. */
  readonly restore: (
    checkpoint: SandboxCheckpoint,
  ) => Effect.Effect<ContainerIdentity, SandboxOperationError>;
  readonly configureOutboundAuthority: (
    authority: unknown,
  ) => Effect.Effect<void, SandboxOperationError>;
  readonly setRepositoryAuthorization: (
    remote: string,
    credential: string,
    expiresAt: number,
    grantId: string,
  ) => Effect.Effect<RepositoryAuthorizationGrant, SandboxOperationError>;
  readonly clearRepositoryAuthorization: (
    grant: RepositoryAuthorizationGrant,
  ) => Effect.Effect<void, SandboxOperationError>;
  readonly backupDirectory: (input: {
    readonly dir: string;
    readonly name?: string;
  }) => Effect.Effect<DirectoryBackupRecord, SandboxOperationError>;
  readonly restoreDirectory: (
    receipt: DirectoryBackupRecord,
  ) => Effect.Effect<void, SandboxOperationError>;
  readonly deleteDirectoryBackup: (
    receipt: DirectoryBackupRecord,
  ) => Effect.Effect<void, SandboxOperationError>;
  readonly lease: (
    id: string,
    active: boolean,
    renewal?: boolean,
  ) => Effect.Effect<void, SandboxOperationError>;
  readonly destroy: Effect.Effect<void, SandboxOperationError>;
  readonly park: Effect.Effect<void, SandboxOperationError>;
  readonly pinImage: (imageDigest: string | null) => Effect.Effect<void, SandboxOperationError>;
}

export class NativeSandboxNamespace extends Context.Service<
  NativeSandboxNamespace,
  DurableObjectNamespace.DurableObjectNamespaceEffectClient<NativeSandboxRpc>
>()("sandbox-agent/NativeSandboxNamespace") {
  static readonly layer = (options: { readonly binding: string }) =>
    DurableObjectNamespace.layer(NativeSandboxNamespace, options);
}

export const decodeNativeReply = <A>(
  operation: string,
  reply: NativeReply<A>,
): Effect.Effect<A, SandboxOperationError> =>
  reply._tag === "Success"
    ? Effect.succeed(reply.value)
    : Effect.failCause(
        Cause.map(
          causeFromFailureDiagnostic(reply.diagnostic),
          (cause) =>
            new SandboxOperationError({
              operation,
              cause,
              ...(reply.code === undefined ? {} : { code: reply.code }),
            }),
        ),
      );

type NativeRpcMethod = Exclude<keyof NativeSandboxRpc, "fetch" | "locate">;
type NativeRpcArgs<Method extends NativeRpcMethod> = NativeSandboxRpc[Method] extends (
  ...args: infer Args
) => Promise<any>
  ? Args
  : never;
type NativeRpcValue<Method extends NativeRpcMethod> =
  ReturnType<NativeSandboxRpc[Method]> extends Promise<NativeReply<infer A>> ? A : never;

/** Resolve native channels within each invocation; only the address survives durable retries. */
export const makeNativeSandbox = Effect.fnUntraced(function* (
  name: string,
  defaults: EnsureOptions = {},
): Effect.fn.Return<NativeSandbox, never, NativeSandboxNamespace> {
  const namespace = yield* NativeSandboxNamespace;
  // An exact boot receipt already names an instantiated owner. Its native
  // operations still enforce that receipt; they cannot create a replacement.
  let located =
    defaults.recovery === "exactWorkspaceRequired" && defaults.expectedInstanceId !== undefined;
  const resolve = Effect.gen(function* () {
    if (!located) {
      // Place new owners from the same origin before even describe/grant/inspect
      // can resolve them. Keep no native stub across invocations.
      const coordinator = yield* namespace.getByName(CAPACITY_NAME);
      const reply = yield* namespace
        .call(coordinator, "locate", name)
        .pipe(
          Effect.mapError(
            (cause) =>
              new SandboxOperationError({ operation: "locate", cause, code: "RPC_TRANSPORT" }),
          ),
        );

      yield* decodeNativeReply("locate", reply);
      located = true;
    }
    return yield* namespace.getByName(name);
  });
  const call = Effect.fnUntraced(function* <Method extends NativeRpcMethod>(
    method: Method,
    ...args: NativeRpcArgs<Method>
  ): Effect.fn.Return<NativeRpcValue<Method>, SandboxOperationError> {
    const stub = yield* resolve;
    const reply = yield* namespace.call(stub, method, ...args).pipe(
      Effect.catchCause((cause) =>
        Effect.failCause(
          Cause.map(
            cause,
            (error) =>
              new SandboxOperationError({
                operation: method,
                cause: error,
                code: "RPC_TRANSPORT",
              }),
          ),
        ),
      ),
    );

    // Method fixes both the native arguments and the value in its tagged reply.
    return yield* decodeNativeReply(method, reply as NativeReply<NativeRpcValue<Method>>);
  });
  const observe = (id: string, maxBytes = 0) => call("process", id, maxBytes);
  const handle = (initial: ProcessStatus): SandboxProcessHandle => {
    let terminal: ProcessStatus | undefined;
    const status = observe(initial.id).pipe(
      Effect.flatMap((value) =>
        value === null
          ? Effect.fail(
              new SandboxOperationError({
                operation: "process",
                cause: new Error("Process receipt is missing"),
              }),
            )
          : Effect.succeed(value),
      ),
    );
    const wait = Effect.fnUntraced(function* (timeout: number, maxBytes: number) {
      const deadline = (yield* Clock.currentTimeMillis) + timeout;

      while (true) {
        const remaining = deadline - (yield* Clock.currentTimeMillis);
        const value =
          terminal ??
          (yield* call(
            "waitProcess",
            initial.id,
            maxBytes,
            Math.max(0, Math.min(20_000, remaining)),
          ));

        if (value === null || value.state === "lost")
          return yield* new SandboxOperationError({
            operation: "process",
            cause: new Error("Process ownership was lost"),
          });
        if (value.state === "exited") return value;
        if ((yield* Clock.currentTimeMillis) >= deadline)
          return yield* new SandboxOperationError({
            operation: "process.wait",
            code: "ETIMEDOUT",
            cause: new Error("Process observation timed out"),
          });
      }
    });

    return {
      id: initial.id,
      status,
      kill: (signal = 15) => call("signalProcess", initial.id, initial.instanceId, signal),
      waitForExit: ({ timeout }) =>
        wait(timeout, 0).pipe(Effect.map((value) => value.exit ?? { code: value.exitCode ?? -1 })),
      outputText: ({ timeout, maxBytes, acknowledge = true }) =>
        wait(timeout, maxBytes).pipe(
          Effect.tap((value) =>
            Effect.gen(function* () {
              // Preserve this handle's terminal bytes before releasing retention.
              // A failed acknowledgment leaves the native output recoverable.
              terminal = value;
              if (acknowledge) yield* call("acknowledgeProcess", initial.id);
            }),
          ),
          Effect.map((value) => ({ ...value, exitCode: value.exitCode ?? -1 })),
        ),
      waitForPort: (port, options = {}) =>
        call("waitForPort", initial.id, port, options.timeout ?? 30_000),
      waitForHttp: (port, options = {}) => call("waitForHttp", initial.id, port, options),
    };
  };
  const leases = new Map<string, string>();
  const readFileString = (path: string) => call("readFileString", path, defaults);

  return {
    describe: call("describe", defaults),
    ensure: (options = {}) => call("ensure", { ...defaults, ...options }),
    allocateProcess: Effect.fnUntraced(function* (resource?: ProcessResource) {
      const current = yield* call("ensure", defaults);

      return yield* call(
        "allocateProcess",
        {
          ...defaults,
          expectedInstanceId: current.instanceId,
          recovery: "exactWorkspaceRequired",
        },
        resource ?? null,
        null,
      );
    }),
    acknowledgeProcess: (id) => call("acknowledgeProcess", id),
    signalProcess: (id, signal = 15) =>
      Effect.gen(function* () {
        const ticket = /^process:([^:]+):([1-9][0-9]*)$/u.exec(id);

        if (!ticket)
          return yield* new SandboxOperationError({
            operation: "signalProcess",
            cause: new Error("Invalid process ticket"),
          });

        return yield* call("signalProcess", id, ticket[1]!, signal);
      }),
    exec: (argv, options = {}) => {
      let id: string | undefined;
      let deadlineAt: number | null | undefined;
      let boundStart: EnsureOptions | undefined;

      return Effect.gen(function* () {
        if (options.processId !== undefined && options.resource !== undefined)
          return yield* new SandboxOperationError({
            operation: "exec",
            cause: new Error("Process ticket and resource identity are mutually exclusive"),
          });
        if (boundStart === undefined) {
          const instanceId =
            defaults.expectedInstanceId ?? (yield* call("ensure", defaults)).instanceId;

          // Retrying this accepted operation must never select a replacement boot.
          boundStart ??= {
            ...defaults,
            expectedInstanceId: instanceId,
            recovery: "exactWorkspaceRequired",
          };
        }

        const exactStart = boundStart;

        id ??=
          options.processId ??
          (yield* call(
            "allocateProcess",
            exactStart,
            options.resource ?? null,
            options.nativeListener === true ? Number(argv[3]) : null,
          ));
        deadlineAt ??=
          options.deadlineAt ??
          (options.timeout === undefined
            ? null
            : (yield* Clock.currentTimeMillis) + options.timeout);

        return yield* call(
          "exec",
          argv,
          { ...options, processId: id, deadlineAt },
          exactStart,
        ).pipe(Effect.map(handle));
      });
    },
    inspect: (argv, options) => {
      let boundStart: EnsureOptions | undefined;
      let deadlineAt: number | undefined;

      return Effect.gen(function* () {
        boundStart ??= {
          ...defaults,
          expectedInstanceId:
            defaults.expectedInstanceId ?? (yield* call("ensure", defaults)).instanceId,
          recovery: "exactWorkspaceRequired",
        };
        deadlineAt ??= options.deadlineAt ?? (yield* Clock.currentTimeMillis) + options.timeout;

        return yield* call("inspect", argv, { ...options, deadlineAt }, boundStart);
      });
    },
    observeProcess: (id, { maxBytes, waitMillis = 0 }) =>
      waitMillis === 0 ? observe(id, maxBytes) : call("waitProcess", id, maxBytes, waitMillis),
    getProcess: (id) =>
      observe(id).pipe(
        Effect.map((value) => (value === null ? Option.none() : Option.some(handle(value)))),
      ),
    listProcesses: call("processes"),
    readFileString,
    readFile: (path) => call("readFile", path, defaults),
    lstat: (path) => call("lstat", path, defaults),
    writeFile: (path, content) => call("writeFile", path, content, defaults),
    readDirectory: (path) => call("readDirectory", path, defaults),
    mkdir: (path, options) => call("mkdir", path, options?.recursive ?? false, defaults),
    remove: (path, options) =>
      call("remove", path, options?.recursive ?? false, options?.force ?? false, defaults),
    exists: (path) => call("exists", path, defaults),
    forwardPort: (port, request) =>
      Effect.gen(function* () {
        const instanceId =
          defaults.expectedInstanceId ?? (yield* call("describe", defaults)).current?.instanceId;

        if (!instanceId)
          return yield* new SandboxOperationError({
            operation: "forwardPort",
            cause: new Error("No active container owner"),
          });
        const url = new URL(request.url);
        const headers = new Headers(request.headers);

        url.protocol = "http:";
        url.hostname = NATIVE_PORT_HOST;
        url.port = String(port);
        headers.set(NATIVE_INSTANCE_HEADER, instanceId);
        headers.delete("host");

        // WebSocket upgrades cross a Durable Object's fetch boundary, not RPC values.
        const stub = yield* resolve;

        return yield* namespace
          .fetch(stub, new Request(new Request(url, request), { headers }))
          .pipe(
            Effect.catchCause((cause) =>
              Effect.failCause(
                Cause.map(
                  cause,
                  (error) => new SandboxOperationError({ operation: "forwardPort", cause: error }),
                ),
              ),
            ),
          );
      }),
    grantPreview: (input) => call("grantPreview", input),
    revokePreview: call("revokePreview"),
    checkpoint: (input) => call("checkpoint", input),
    restore: (checkpoint) => call("restore", checkpoint, defaults),
    configureOutboundAuthority: (authority) => call("configureOutboundAuthority", authority),
    setRepositoryAuthorization: (remote, credential, expiresAt, grantId) => {
      let grant: RepositoryAuthorizationGrant | undefined;

      return Effect.gen(function* () {
        if (grant === undefined) {
          const current = yield* call("ensure", defaults);

          grant ??= { remote, grantId, instanceId: current.instanceId };
        }
        const owner = grant;

        yield* call(
          "setRepositoryAuthorization",
          remote,
          credential,
          expiresAt,
          owner.instanceId,
          owner.grantId,
        );

        return owner;
      });
    },
    clearRepositoryAuthorization: (grant) =>
      call("setRepositoryAuthorization", grant.remote, null, null, grant.instanceId, grant.grantId),
    backupDirectory: (input) => call("backupDirectory", input, defaults),
    restoreDirectory: (receipt) => call("restoreDirectory", receipt, defaults),
    deleteDirectoryBackup: (receipt) => call("deleteDirectoryBackup", receipt),
    lease: (id, active, renewal = false) =>
      Effect.gen(function* () {
        let instanceId = leases.get(id) ?? defaults.expectedInstanceId;

        // Native lease admission validates an existing exact boot receipt itself.
        if (!instanceId && active) {
          instanceId = (yield* call("ensure", defaults)).instanceId;
          leases.set(id, instanceId);
        }
        if (!instanceId) return;
        const owner = instanceId;

        yield* call("lease", id, active, owner, renewal);
        if (!active) leases.delete(id);
      }),
    destroy: call("destroy"),
    park: call("park"),
    pinImage: (imageDigest) => call("pinImage", imageDigest),
  };
});

export const previewTokenHash = (token: string) =>
  Effect.tryPromise({
    try: async () => {
      const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(token));

      return Array.from(new Uint8Array(digest), (value) =>
        value.toString(16).padStart(2, "0"),
      ).join("");
    },
    catch: (cause) => new SandboxOperationError({ operation: "previewTokenHash", cause }),
  });
