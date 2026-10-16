import {
  DirectoryBackupGateway,
  type DirectoryBackupRecord,
  type FileContent,
} from "@cloudflare/sandbox";
import { Context, Effect, Layer, Schema } from "effect";
import {
  DurableObject,
  DurableObjectAlarm,
  DurableObjectContainer,
  DurableObjectState,
  Worker,
} from "effect-cf";

import { reportSanitizedCause, withErrorReportingScope } from "../observability/RuntimeDiagnostics";

import { ContainerLifecycle } from "./ContainerLifecycle";
import { ContainerOutbound, type SandboxAgentContainerOptions } from "./ContainerOutbound";
import { ContainerPreview } from "./ContainerPreview";
import { ContainerProcesses } from "./ContainerProcesses";
import { ContainerState, error, reply } from "./ContainerState";
import { ContainerSupervisor } from "./ContainerSupervisor";
import { ContainerWorkspace } from "./ContainerWorkspace";
import {
  CAPACITY_NAME,
  decodeNativeReply,
  type EnsureOptions,
  type ExecOptions,
  type HttpReadinessOptions,
  type InspectionOptions,
  NativeSandboxNamespace,
  type PreparedWorkspace,
  ProcessResource,
  NATIVE_PORT_HOST,
  type PreviewGrant,
  PreviewRoute,
  type SandboxCommand,
} from "./NativeSandbox";
export { makeSandboxAgentOutbound, OPENAI_HOST, HOST_OPERATION_HOST } from "./ContainerOutbound";
export type { SandboxAgentContainerOptions } from "./ContainerOutbound";
export { SANDBOX_PACKAGE_VERSION } from "../RuntimeVersions";
export { DirectoryBackupGateway };
const AlarmHandlers = DurableObjectAlarm.define({ "sandbox-preview": Schema.Null });

export const makeSandboxAgentContainer = <
  Environment extends Cloudflare.Env,
  EventServices = never,
  EventError = never,
>(
  options: SandboxAgentContainerOptions<Environment, EventServices, EventError>,
) => {
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
      ensure: (input: EnsureOptions = {}) => admitted(lifecycle.ensure(input)),
      revokePreview: admitted(lifecycle.revokePreview),
    };
  });

  class Runtime extends Context.Service<Runtime, Effect.Success<typeof make>>()(
    "sandbox-agent/ContainerRuntime",
  ) {}
  const stateLayer = ContainerState.layer(options);
  const supervisorLayer = ContainerSupervisor.layer.pipe(Layer.provideMerge(stateLayer));
  const outboundLayer = ContainerOutbound.layer.pipe(Layer.provideMerge(stateLayer));
  const lifecycleLayer = ContainerLifecycle.layer.pipe(
    Layer.provideMerge(Layer.merge(supervisorLayer, outboundLayer)),
  );
  const processesLayer = ContainerProcesses.layer.pipe(Layer.provideMerge(lifecycleLayer));
  const operationsLayer = Layer.merge(ContainerWorkspace.layer, ContainerPreview.layer).pipe(
    Layer.provideMerge(processesLayer),
  );
  const layer = Layer.effect(Runtime, make).pipe(
    Layer.provideMerge(operationsLayer),
    Layer.provideMerge(
      Layer.merge(
        DurableObjectContainer.layer,
        NativeSandboxNamespace.layer({ binding: options.namespaceBinding }),
      ),
    ),
  );

  return DurableObject.make(layer, {
    eventLayer: options.eventLayer ?? Layer.empty,
    initialize: Effect.flatMap(Runtime, (runtime) => runtime.initialize).pipe(
      Effect.tapCause((cause) =>
        reportSanitizedCause(cause, { operation: "SandboxContainer.initialize" }),
      ),
      withErrorReportingScope,
    ),
    alarms: AlarmHandlers.handlers({
      "sandbox-preview": () => Effect.flatMap(Runtime, (runtime) => runtime.expirePreviews),
    }).pipe(
      Effect.tapCause((cause) =>
        reportSanitizedCause(cause, { operation: "SandboxContainer.alarm" }),
      ),
      withErrorReportingScope,
    ),
    fetch: Effect.gen(function* () {
      const runtime = yield* Runtime;
      const request = yield* Worker.NativeRequest;

      return yield* new URL(request.url).hostname === NATIVE_PORT_HOST
        ? runtime.fetchNative
        : runtime.fetchPreview;
    }),
    rpc: {
      locate: (name: string) =>
        reply(
          "SandboxContainer.locate",
          Effect.gen(function* () {
            const namespace = yield* NativeSandboxNamespace;
            const state = yield* DurableObjectState.DurableObjectState;
            const target = yield* namespace.idFromName(name);

            if (target.toString() === state.id.toString()) return;
            if ((yield* namespace.idFromName(CAPACITY_NAME)).toString() !== state.id.toString())
              return yield* error("locate", "Only the capacity owner can locate another sandbox");
            // No lifecycle permit: the child's initialization can call back to
            // this coordinator to release a stopped boot's capacity receipt.
            const child = yield* namespace.getByName(name);
            const result = yield* namespace.call(child, "locate", name);

            yield* decodeNativeReply("locate", result);
          }),
        ),
      describe: (input: EnsureOptions) =>
        reply(
          "SandboxContainer.describe",
          Effect.flatMap(Runtime, (r) => r.describe(input)),
        ),
      grantPreview: (input: PreviewGrant) =>
        reply(
          "SandboxContainer.grantPreview",
          Effect.flatMap(Runtime, (r) => r.grantPreview(input)),
        ),
      revokePreview: () =>
        reply(
          "SandboxContainer.revokePreview",
          Effect.flatMap(Runtime, (r) => r.revokePreview),
        ),
      registerPreview: (hash: string, route: PreviewRoute | null, expiresAt: number) =>
        reply(
          "SandboxContainer.registerPreview",
          Effect.flatMap(Runtime, (r) => r.registerPreview(hash, route, expiresAt)),
        ),
      resolvePreview: (hash: string) =>
        reply(
          "SandboxContainer.resolvePreview",
          Effect.flatMap(Runtime, (r) => r.resolvePreview(hash)),
        ),
      exists: (path: string, start: EnsureOptions) =>
        reply(
          "SandboxContainer.exists",
          Effect.flatMap(Runtime, (r) => r.exists(path, start)),
        ),
      waitForPort: (id: string, port: number, timeout: number) =>
        reply(
          "SandboxContainer.waitForPort",
          Effect.flatMap(Runtime, (r) => r.waitForPort(id, port, timeout)),
        ),
      ensure: (input: EnsureOptions) =>
        reply(
          "SandboxContainer.ensure",
          Effect.flatMap(Runtime, (r) => r.ensure(input)),
        ),
      exec: (argv: SandboxCommand, input: ExecOptions, start: EnsureOptions) =>
        reply(
          "SandboxContainer.exec",
          Effect.flatMap(Runtime, (r) => r.exec(argv, input, start)),
        ),
      inspect: (argv: SandboxCommand, input: InspectionOptions, start: EnsureOptions) =>
        reply(
          "SandboxContainer.inspect",
          Effect.flatMap(Runtime, (r) => r.inspect(argv, input, start)),
        ),
      allocateProcess: (
        start: EnsureOptions,
        resource: ProcessResource | null,
        listenerPort: number | null,
      ) =>
        reply(
          "SandboxContainer.allocateProcess",
          Effect.flatMap(Runtime, (r) => r.allocateProcess(start, resource, listenerPort)),
        ),
      process: (id: string, maxBytes: number) =>
        reply(
          "SandboxContainer.process",
          Effect.flatMap(Runtime, (r) => r.processStatus(id, maxBytes)),
        ),
      waitProcess: (id: string, maxBytes: number, waitMillis: number) =>
        reply(
          "SandboxContainer.waitProcess",
          Effect.flatMap(Runtime, (r) => r.waitProcess(id, maxBytes, waitMillis)),
        ),
      acknowledgeProcess: (id: string) =>
        reply(
          "SandboxContainer.acknowledgeProcess",
          Effect.flatMap(Runtime, (r) => r.acknowledgeProcess(id)),
        ),
      processes: () =>
        reply(
          "SandboxContainer.processes",
          Effect.flatMap(Runtime, (r) => r.processes),
        ),
      signalProcess: (id: string, instanceId: string, signal: number) =>
        reply(
          "SandboxContainer.signalProcess",
          Effect.flatMap(Runtime, (r) => r.signalProcess(id, instanceId, signal)),
        ),
      waitForHttp: (id: string, port: number, input: HttpReadinessOptions) =>
        reply(
          "SandboxContainer.waitForHttp",
          Effect.flatMap(Runtime, (r) => r.waitForHttp(id, port, input)),
        ),
      readFileString: (path: string, start: EnsureOptions) =>
        reply(
          "SandboxContainer.readFileString",
          Effect.flatMap(Runtime, (r) => r.readFileString(path, start)),
        ),
      readFile: (path: string, start: EnsureOptions) =>
        reply(
          "SandboxContainer.readFile",
          Effect.flatMap(Runtime, (r) => r.readFile(path, start)),
        ),
      lstat: (path: string, start: EnsureOptions) =>
        reply(
          "SandboxContainer.lstat",
          Effect.flatMap(Runtime, (r) => r.lstat(path, start)),
        ),
      writeFile: (path: string, content: FileContent, start: EnsureOptions) =>
        reply(
          "SandboxContainer.writeFile",
          Effect.flatMap(Runtime, (r) => r.writeFile(path, content, start)),
        ),
      readDirectory: (path: string, start: EnsureOptions) =>
        reply(
          "SandboxContainer.readDirectory",
          Effect.flatMap(Runtime, (r) => r.readDirectory(path, start)),
        ),
      mkdir: (path: string, recursive: boolean, start: EnsureOptions) =>
        reply(
          "SandboxContainer.mkdir",
          Effect.flatMap(Runtime, (r) => r.mkdir(path, recursive, start)),
        ),
      remove: (path: string, recursive: boolean, force: boolean, start: EnsureOptions) =>
        reply(
          "SandboxContainer.remove",
          Effect.flatMap(Runtime, (r) => r.remove(path, recursive, force, start)),
        ),
      checkpoint: (input: PreparedWorkspace) =>
        reply(
          "SandboxContainer.checkpoint",
          Effect.flatMap(Runtime, (r) => r.checkpoint(input)),
        ),
      configureOutboundAuthority: (authority: unknown) =>
        reply(
          "SandboxContainer.configureOutboundAuthority",
          Effect.flatMap(Runtime, (r) => r.configureOutboundAuthority(authority)),
        ),
      setRepositoryAuthorization: (
        remote: string,
        credential: string | null,
        expiresAt: number | null,
        instanceId: string,
        grantId: string,
      ) =>
        reply(
          "SandboxContainer.setRepositoryAuthorization",
          Effect.flatMap(Runtime, (r) =>
            r.setRepositoryAuthorization(remote, credential, expiresAt, instanceId, grantId),
          ),
        ),
      backupDirectory: (
        input: { readonly dir: string; readonly name?: string },
        start: EnsureOptions,
      ) =>
        reply(
          "SandboxContainer.backupDirectory",
          Effect.flatMap(Runtime, (r) => r.backupDirectory(input, start)),
        ),
      restoreDirectory: (receipt: DirectoryBackupRecord, start: EnsureOptions) =>
        reply(
          "SandboxContainer.restoreDirectory",
          Effect.flatMap(Runtime, (r) => r.restoreDirectory(receipt, start)),
        ),
      deleteDirectoryBackup: (receipt: DirectoryBackupRecord) =>
        reply(
          "SandboxContainer.deleteDirectoryBackup",
          Effect.flatMap(Runtime, (r) => r.deleteDirectoryBackup(receipt)),
        ),
      destroy: () =>
        reply(
          "SandboxContainer.destroy",
          Effect.flatMap(Runtime, (r) => r.destroy),
        ),
      park: () =>
        reply(
          "SandboxContainer.park",
          Effect.flatMap(Runtime, (r) => r.park),
        ),
      pinImage: (imageDigest: string | null) =>
        reply(
          "SandboxContainer.pinImage",
          Effect.flatMap(Runtime, (r) => r.pinImage(imageDigest)),
        ),
      lease: (id: string, active: boolean, instanceId: string, renewal: boolean) =>
        reply(
          "SandboxContainer.lease",
          Effect.flatMap(Runtime, (r) => r.lease(id, active, instanceId, renewal)),
        ),
      capacityInUse: (instanceId: string) =>
        reply(
          "SandboxContainer.capacityInUse",
          Effect.flatMap(Runtime, (r) => r.capacityInUse(instanceId)),
        ),
      reserveCapacity: (id: string, instanceId: string) =>
        reply(
          "SandboxContainer.reserveCapacity",
          Effect.flatMap(Runtime, (r) => r.reserveCapacity(id, instanceId)),
        ),
      releaseCapacity: (id: string, instanceId: string) =>
        reply(
          "SandboxContainer.releaseCapacity",
          Effect.flatMap(Runtime, (r) => r.releaseCapacity(id, instanceId)),
        ),
    },
  });
};
