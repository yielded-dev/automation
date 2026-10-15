import { Files, SandboxFileError } from "@cloudflare/sandbox";
import { Cause, Context, Data, Effect, Layer, Predicate, Schema, Semaphore } from "effect";
import {
  ContainerFiles,
  DurableObjectContainer,
  DurableObjectState,
  WorkerEnvironment,
} from "effect-cf";

import { failureDiagnostic } from "../observability/FailureDiagnostic";

import type { SandboxAgentContainerOptions } from "./ContainerOutbound";
import { type NativeReply, PreviewRoute, WorkspaceCheckpoint } from "./NativeSandbox";
export const RepositoryAuthorization = Schema.Struct({
  remote: Schema.String,
  credential: Schema.String,
  expiresAt: Schema.Finite,
});
export const OutboundProps = Schema.Struct({
  containerId: Schema.String,
  authority: Schema.Unknown,
  repositoryAuthorizations: Schema.Array(RepositoryAuthorization),
});

export type OutboundProps = typeof OutboundProps.Type;
export const Identity = Schema.Struct({
  instanceId: Schema.String,
  imageDigest: Schema.String,
  restoredSnapshotId: Schema.NullOr(Schema.String),
  timings: Schema.Struct({
    nativeStartRequestedAt: Schema.Finite,
    nativeStartReturnedAt: Schema.NullOr(Schema.Finite),
    ownerReadyAt: Schema.NullOr(Schema.Finite),
  }),
});
export const CapacityReservation = Schema.Struct({
  id: Schema.String,
  instanceId: Schema.String,
});
export const PreviewIndexRecord = Schema.Struct({ route: PreviewRoute, expiresAt: Schema.Finite });
export const samePreviewRoute = Schema.toEquivalence(PreviewRoute);
export const PreviewRecord = Schema.Struct({
  hash: Schema.String,
  route: PreviewRoute,
  expiresAt: Schema.Finite,
  instanceId: Schema.String,
  processIds: Schema.Array(Schema.String),
});
export const samePreviewRecord = Schema.toEquivalence(PreviewRecord);
export const SupervisorReply = Schema.Union([
  Schema.Struct({ ok: Schema.Literal(true), value: Schema.Unknown }),
  Schema.Struct({
    ok: Schema.Literal(false),
    error: Schema.Struct({ code: Schema.String, message: Schema.String }),
  }),
]);
export const CheckpointRecord = Schema.Struct({
  input: Schema.Struct({
    captureId: Schema.String,
    instanceId: Schema.String,
    setupFingerprint: Schema.String,
    head: Schema.String,
    materializationId: Schema.String,
    snapshotAllowed: Schema.Boolean,
  }),
  result: Schema.NullOr(WorkspaceCheckpoint),
});
export const sameCheckpointInput = Schema.toEquivalence(CheckpointRecord.fields.input);
export const error = (operation: string, message: string) =>
  new DurableObjectContainer.ContainerError({ operation, cause: new Error(message) });

export class ReadinessProcessError extends Data.TaggedError("ReadinessProcessError")<{
  readonly processId: string;
  readonly code: "PROCESS_EXITED" | "PROCESS_LOST";
  readonly cause?: unknown;
}> {}
export class LeaseLostError extends Data.TaggedError("LeaseLostError")<{
  readonly instanceId: string;
}> {}
export class NativeProcessError extends Data.TaggedError("NativeProcessError")<{
  readonly operation: string;
  readonly code: string;
  readonly cause: unknown;
}> {}
export const failureCode = <E>(cause: Cause.Cause<E>): string | undefined => {
  const reason = cause.reasons[0];

  if (cause.reasons.length !== 1 || !reason || !Cause.isFailReason(reason)) return undefined;
  const value = reason.error;

  if (Predicate.isTagged(value, "ContainerReadinessTimeoutError")) return "ETIMEDOUT";
  if (value instanceof ReadinessProcessError) return value.code;
  if (value instanceof LeaseLostError) return "LEASE_LOST";
  if (value instanceof NativeProcessError) return value.code;

  return Predicate.isTagged(value, "ContainerError") &&
    Predicate.hasProperty(value, "cause") &&
    SandboxFileError.is(value.cause)
    ? value.cause.code
    : undefined;
};
export const reply = <A, E, R>(
  operation: string,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<NativeReply<A>, never, R> =>
  effect.pipe(
    Effect.map((value): NativeReply<A> => ({ _tag: "Success", value })),
    Effect.catchCause((cause) =>
      Effect.succeed({
        _tag: "Failure" as const,
        diagnostic: failureDiagnostic(cause, { operation }),
        ...(failureCode(cause) === undefined ? {} : { code: failureCode(cause) }),
      }),
    ),
  );

const make = <Environment extends Cloudflare.Env, EventServices, EventError>(
  options: SandboxAgentContainerOptions<Environment, EventServices, EventError>,
) =>
  Effect.gen(function* () {
    const container = yield* DurableObjectContainer.DurableObjectContainer;
    const state = yield* DurableObjectState.DurableObjectState;
    const environment = (yield* WorkerEnvironment) as Environment;
    const cache: {
      revision: number;
      mutations: number;
    } = { revision: 0, mutations: 0 };
    const files = ContainerFiles.fromFiles(new Files(container.raw));
    // Keep cancellation attached until the SDK finishes both native exec streams.
    // Acquiring the Response separately leaves its late completion outside that signal.
    const readFileString = (path: string) =>
      Effect.tryPromise({
        try: async (signal) => {
          const response = await files.raw.readFile(path, { signal });

          return await response.text();
        },
        catch: (cause) =>
          new DurableObjectContainer.ContainerError({ operation: "files.readFileString", cause }),
      }).pipe(Effect.timeout("30 seconds"));
    const boot = yield* Semaphore.make(1);
    const admission = yield* Semaphore.make(1);
    const previewValidation = yield* Semaphore.make(1);
    const admitted = <A, E, R>(effect: Effect.Effect<A, E, R>) => admission.withPermits(1)(effect);
    const mutateFiles = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      Effect.acquireUseRelease(
        Effect.sync(() => {
          cache.revision++;
          cache.mutations++;
        }),
        () => effect,
        () =>
          Effect.sync(() => {
            cache.revision++;
            cache.mutations--;
          }),
      );
    const read = <A, I>(key: string, schema: Schema.Codec<A, I>) =>
      state.storage.kv.get(key).pipe(
        // oxlint-disable-next-line no-restricted-properties -- decode private DO storage once at its owner.
        Effect.flatMap((value) =>
          value === undefined
            ? Effect.succeed(null)
            : // oxlint-disable-next-line no-restricted-properties -- private Durable Object storage is untyped.
              Schema.decodeUnknownEffect(schema)(value),
        ),
      );
    const identity = () => read("container-identity", Identity);

    return {
      read,
      identity,
      files,
      readFileString,
      admitted,
      mutateFiles,
      boot,
      previewValidation,
      cache,
      settings: {
        imageKey: options.imageKey ?? "harness",
        idleMillis: options.idleMillis ?? 300_000,
        capacityBinding: options.capacityBinding ?? "SANDBOX_MAX_INSTANCES",
        backupBucketBinding: options.backupBucketBinding,
        outboundEntrypoint: options.outboundEntrypoint ?? "SandboxOutbound",
        environmentVariables: options.environmentVariables,
        allowedHosts: options.allowedHosts(environment),
        hostOperationEnabled: options.hostOperationFetch !== undefined,
        sentryMcpHostname:
          options.sentryMcpFetch === undefined ? undefined : options.sentryMcpHostname,
      },
    };
  });

export class ContainerState extends Context.Service<
  ContainerState,
  Effect.Success<ReturnType<typeof make>>
>()("kommunikasie/ContainerState") {
  static readonly layer = <Environment extends Cloudflare.Env, EventServices, EventError>(
    options: SandboxAgentContainerOptions<Environment, EventServices, EventError>,
  ) => Layer.effect(ContainerState, make(options));
}
