import * as Crypto from "effect/Crypto";
import { Cause, Clock, Config, Context, Effect, Layer, Predicate, Schema } from "effect";
import { DurableObjectContainer, DurableObjectState } from "effect-cf";

import { reportSanitizedCause, withErrorReportingScope } from "../observability/RuntimeDiagnostics";
import { FailureClassification, failureDiagnostic } from "../observability/FailureDiagnostic";

import { ContainerOutbound } from "./ContainerOutbound";
import {
  ContainerState,
  CapacityReservation,
  PreviewRecord,
  CheckpointRecord,
  CheckpointUnavailableError,
  error,
  LeaseLostError,
} from "./ContainerState";
import { ContainerSupervisor } from "./ContainerSupervisor";
import {
  CAPACITY_NAME,
  decodeNativeReply,
  type ContainerIdentity,
  type EnsureOptions,
  NativeSandboxNamespace,
  SandboxCheckpoint,
} from "./NativeSandbox";
import { PROCESS_BOOTSTRAP_SOURCE } from "./ProcessSupervisorSource";

const EnsureFailureReason = Schema.Literals([
  "idle-policy",
  "owner-receipt-missing",
  "retirement-pending",
  "instance-replaced",
  "workspace-lost",
  "startup-backoff",
  "image-not-configured",
]);
const ensureError = (reason: typeof EnsureFailureReason.Type, message: string) =>
  Object.assign(error("ensure", message), {
    [FailureClassification]: { type: "ContainerError", reason },
  });

const make = Effect.gen(function* () {
  const container = yield* DurableObjectContainer.DurableObjectContainer;
  const state = yield* DurableObjectState.DurableObjectState;
  const namespace = yield* NativeSandboxNamespace;
  const { read, identity, admitted, boot, settings: options } = yield* ContainerState;
  const { inspectProcess, ensureSupervisor, protect } = yield* ContainerSupervisor;
  const outboundOwner = yield* ContainerOutbound;
  const configureOutbound = outboundOwner.configureOutbound;
  const maximum = yield* Config.Int(options.capacityBinding).pipe(Config.withDefault(20));

  if (maximum < 1)
    return yield* error("capacity.configuration", "Container capacity must be positive");
  const idle = options.idleMillis ?? 300_000;
  let monitored: string | null = null;
  const refreshInactivity = Effect.gen(function* () {
    if (yield* container.running) yield* container.setInactivityTimeout("6 hours");
  }).pipe(Effect.withSpan("SandboxContainer.refreshInactivity"));
  const describe = Effect.fn("SandboxContainer.describe")(function* (input: EnsureOptions = {}) {
    const images = yield* container.images;
    const pinned = yield* read("pinned-image", Schema.String);
    const imageDigest = pinned ?? images[input.imageKey ?? options.imageKey ?? "harness"];

    if (!imageDigest) return yield* error("describe", "Requested image is not configured");

    return {
      imageDigest,
      classIdentity: (yield* namespace.idFromName(CAPACITY_NAME)).toString(),
      running: yield* container.running,
      current: (yield* container.running) ? yield* identity : null,
      checkpoint: (yield* read("workspace-checkpoint", CheckpointRecord))?.result ?? null,
    };
  });
  const preserveCapacityFailure =
    (operation: string) =>
    <E>(cause: Cause.Cause<E>) =>
      Effect.failCause(
        Cause.map(
          cause,
          (cause) => new DurableObjectContainer.ContainerError({ operation, cause }),
        ),
      );
  const release = Effect.fnUntraced(function* (instanceId: string) {
    const coordinator = yield* namespace.getByName(CAPACITY_NAME);
    const reply = yield* namespace
      .call(coordinator, "releaseCapacity", state.id.toString(), instanceId)
      .pipe(Effect.catchCause(preserveCapacityFailure("capacity.release")));

    return yield* decodeNativeReply("capacity.release", reply);
  });
  const reserve = Effect.fn("SandboxContainer.reserve")(function* (instanceId: string) {
    const coordinator = yield* namespace.getByName(CAPACITY_NAME);
    const reply = yield* namespace
      .call(coordinator, "reserveCapacity", state.id.toString(), instanceId)
      .pipe(Effect.catchCause(preserveCapacityFailure("capacity.reserve")));

    return yield* decodeNativeReply("capacity.reserve", reply);
  });
  const backoff = Effect.fnUntraced(function* (now: number) {
    const failures = ((yield* read("start-failures", Schema.Int)) ?? 0) + 1;

    yield* state.storage.kv.put("start-failures", failures);
    yield* state.storage.kv.put(
      "start-retry-at",
      now +
        Math.min(60_000, 1_000 * 2 ** Math.min(failures, 6)) +
        Math.floor(Math.random() * 1_000),
    );
  });
  const touch = Effect.gen(function* () {
    const millis = (yield* read("idle-millis", Schema.Finite)) ?? idle;

    yield* inspectProcess("touch", [millis]);
  });
  const monitor = Effect.fnUntraced(function* (current: ContainerIdentity) {
    if (monitored === current.instanceId) return;
    monitored = current.instanceId;
    yield* state.waitUntil(
      container.monitor.pipe(
        Effect.exit,
        Effect.flatMap((exit) =>
          Effect.gen(function* () {
            if (monitored === current.instanceId) monitored = null;
            const stopping =
              (yield* read("stopping-instance", Schema.String)) === current.instanceId;
            const latest = yield* identity;

            if (latest?.instanceId !== current.instanceId) return;
            const observation = yield* container.running.pipe(Effect.exit);

            if (observation._tag === "Failure")
              return yield* Effect.failCause(
                exit._tag === "Failure"
                  ? Cause.combine(exit.cause, observation.cause)
                  : observation.cause,
              );

            if (
              exit._tag === "Failure" &&
              (!stopping ||
                !exit.cause.reasons.every(
                  (reason) =>
                    Cause.isFailReason(reason) &&
                    Predicate.isTagged(reason.error, "ContainerError"),
                ))
            )
              yield* reportSanitizedCause(exit.cause, { operation: "SandboxContainer.monitor" });
            // A failed/ended monitor is not itself a stopped-container receipt.
            // Retain the exact reservation while native state is live.
            if (observation.value) {
              return;
            }
            const receipt = {
              instanceId: current.instanceId,
              at: yield* Clock.currentTimeMillis,
              ...(exit._tag === "Failure"
                ? {
                    diagnostic: failureDiagnostic(exit.cause, {
                      operation: "SandboxContainer.monitor",
                    }),
                  }
                : {}),
            };

            yield* state.storage.kv.put("last-container-stop", receipt);
            yield* release(current.instanceId);
          }),
        ),
        Effect.tapCause((cause) =>
          reportSanitizedCause(cause, { operation: "SandboxContainer.monitor" }),
        ),
        withErrorReportingScope,
      ),
    );
  });
  const protectNativeIO = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.acquireUseRelease(
      Effect.gen(function* () {
        const current = yield* identity;

        if (!current || current.timings.ownerReadyAt === null || !(yield* container.running))
          return null;
        const id = yield* (yield* Crypto.Crypto).randomUUIDv4().pipe(Effect.orDie);

        yield* protect(id, 20 * 60_000);

        return { id, instanceId: current.instanceId };
      }),
      () => effect.pipe(Effect.timeout("20 minutes")),
      (protection) =>
        Effect.gen(function* () {
          if (
            protection &&
            (yield* identity)?.instanceId === protection.instanceId &&
            (yield* container.running)
          )
            yield* protect(protection.id, null);
        }),
    );
  const ensureAttempt = Effect.fn("SandboxContainer.ensure")(function* (
    input: EnsureOptions,
    checkpointRequired = false,
  ) {
    if (input.idleMillis !== undefined) {
      if (input.idleMillis !== 300_000 && input.idleMillis !== 1_800_000)
        return yield* ensureError("idle-policy", "Idle policy is not approved");
      yield* state.storage.kv.put("idle-millis", input.idleMillis);
    }
    // Admission and boot permits serialize startup without blocking other DO events.
    // Native I/O can outlast blockConcurrencyWhile's 30-second limit.
    const start = Effect.gen(function* () {
      if (yield* container.running) {
        const current = yield* identity;

        if (!current)
          return yield* ensureError(
            "owner-receipt-missing",
            "Running container has no ownership receipt",
          );
        yield* Effect.annotateCurrentSpan({
          "sandbox.container.instanceId": current.instanceId,
          "sandbox.container.start": "existing",
          "sandbox.container.snapshotId": current.restoredSnapshotId,
        });
        if ((yield* read("stopping-instance", Schema.String)) === current.instanceId)
          return yield* ensureError("retirement-pending", "Container retirement is pending");
        if (checkpointRequired && current.restoredSnapshotId !== input.checkpoint?.snapshotId)
          return yield* error("restore", "A different workspace is already running");
        if (
          input.expectedInstanceId !== undefined &&
          input.expectedInstanceId !== current.instanceId
        )
          return yield* ensureError(
            "instance-replaced",
            "Workspace belongs to a replaced container",
          );

        return { current };
      }
      if (input.recovery === "exactWorkspaceRequired")
        return yield* ensureError(
          "workspace-lost",
          "The accepted workspace was lost; native work was not replayed",
        );
      const retryAt = yield* read("start-retry-at", Schema.Finite);
      const now = yield* Clock.currentTimeMillis;

      if (retryAt !== null && retryAt > now)
        return yield* ensureError("startup-backoff", "Container startup is backing off");
      const images = yield* container.images;
      const pinned = yield* read("pinned-image", Schema.String);
      const image = pinned ?? images[input.imageKey ?? options.imageKey ?? "harness"];

      if (!image)
        return yield* ensureError("image-not-configured", "Requested image is not configured");
      const saved = input.checkpoint ?? null;
      const classIdentity = (yield* namespace.idFromName(CAPACITY_NAME)).toString();

      if (checkpointRequired && saved) {
        if (saved.expiresAt <= now + 5 * 60_000)
          return yield* new CheckpointUnavailableError({ reason: "expired" });
        if (saved.imageDigest !== image)
          return yield* new CheckpointUnavailableError({ reason: "image" });
        if (saved.classIdentity !== classIdentity)
          return yield* new CheckpointUnavailableError({ reason: "class" });
      }
      const restore =
        saved &&
        saved.expiresAt > now + 5 * 60_000 &&
        saved.imageDigest === image &&
        saved.classIdentity === classIdentity
          ? saved
          : null;
      const previous = yield* identity;

      if (previous) yield* stop;
      yield* state.storage.kv.delete("process-supervisor-token");
      yield* state.storage.kv.delete("supervisor-protocol");
      yield* outboundOwner.clear;
      yield* state.storage.kv.delete("repository-authorizations");
      const instanceId = yield* (yield* Crypto.Crypto).randomUUIDv4().pipe(Effect.orDie);
      let current: ContainerIdentity = {
        instanceId,
        imageDigest: image,
        restoredSnapshotId: restore?.snapshotId ?? null,
        timings: {
          nativeStartRequestedAt: yield* Clock.currentTimeMillis,
          nativeStartReturnedAt: null,
          ownerReadyAt: null,
        },
      };

      yield* Effect.annotateCurrentSpan({
        "sandbox.container.instanceId": instanceId,
        "sandbox.container.start": restore === null ? "image" : "snapshot",
        "sandbox.container.snapshotId": restore?.snapshotId ?? null,
      });
      // A reservation remains attached to this exact boot until native stop is observed.
      yield* state.storage.kv.put("container-identity", current);
      yield* state.storage.kv.put("bootstrap-deadline", now + 20 * 60_000);
      // Observe the output gate before the RPC, instead of charging it to capacity.
      yield* state.storage.sync().pipe(Effect.withSpan("SandboxContainer.bootOutputGate"));
      if (!(yield* reserve(instanceId))) {
        yield* backoff(now);
        yield* state.storage.kv.delete("container-identity");

        return yield* error("capacity.reserve", "Container capacity is exhausted");
      }
      const token = yield* (yield* Crypto.Crypto).randomUUIDv4().pipe(Effect.orDie);

      yield* state.storage.kv.put("process-supervisor-token", token);
      const startOptions = {
        instance:
          input.instance ??
          (input.size === "big"
            ? ("standard-4" as const)
            : { vcpu: 2, memoryMib: 12_288, diskMb: 20_000 }),
        enableInternet: options.enableInternet,
        labels: { instance: instanceId },
        env: {
          HOME: "/root",
          CI: "true",
          NODE_EXTRA_CA_CERTS: "/etc/cloudflare/certs/cloudflare-containers-ca.crt",
          ...options.environmentVariables,
          KOMMUNIKASIE_CONTAINER_INSTANCE: instanceId,
          KOMMUNIKASIE_SUPERVISOR_TOKEN: token,
        },
        entrypoint: ["/usr/bin/tini", "-s", "--", "bun", "-e", PROCESS_BOOTSTRAP_SOURCE],
      };

      yield* state.storage.sync().pipe(Effect.withSpan("SandboxContainer.startOutputGate"));
      yield* container
        .start({
          ...startOptions,
          ...(restore ? { containerSnapshot: { id: restore.snapshotId } } : { image }),
        })
        .pipe(
          // Native start returns after validating options; it does not establish readiness.
          Effect.withSpan("SandboxContainer.nativeStart"),
          Effect.catchCause((cause) =>
            Effect.gen(function* () {
              const cleanup = yield* Effect.gen(function* () {
                yield* backoff(now);
                yield* container.destroy();
                if (yield* container.running)
                  return yield* error("start.cleanup", "Native stop is not confirmed");
                yield* release(instanceId);
                // Keep the failed boot receipt for the outer restore classifier.
              }).pipe(Effect.exit);

              return yield* Effect.failCause(
                cleanup._tag === "Failure" ? Cause.combine(cause, cleanup.cause) : cause,
              );
            }),
          ),
        );
      current = {
        ...current,
        timings: { ...current.timings, nativeStartReturnedAt: yield* Clock.currentTimeMillis },
      };
      yield* state.storage.kv.put("container-identity", current);
      yield* refreshInactivity;
      yield* state.storage.kv.delete("start-retry-at");

      return { current };
    });

    const started = yield* start;

    if (started.current.timings.ownerReadyAt === null) {
      // Rehydration and explicit authority changes install their own outbound handlers.
      yield* configureOutbound;
      yield* ensureSupervisor.pipe(Effect.withSpan("SandboxContainer.ensureSupervisor"));
      started.current = {
        ...started.current,
        timings: { ...started.current.timings, ownerReadyAt: yield* Clock.currentTimeMillis },
      };
      yield* state.storage.kv.put("container-identity", started.current);
      yield* state.storage.kv.delete("start-failures");
      yield* state.storage.kv.delete("bootstrap-deadline");
    }
    yield* touch;
    yield* monitor(started.current);

    return started.current;
  });
  const ensure = (input: EnsureOptions = {}) =>
    boot.withPermits(1)(
      protectNativeIO(
        ensureAttempt(input).pipe(
          Effect.catchCause((cause) =>
            Effect.gen(function* () {
              const current = yield* identity;
              const snapshotId = current?.restoredSnapshotId;

              // Native start may return before the restored filesystem becomes
              // usable. A disposable restore can fail during owner bootstrap,
              // before any workspace operation is admitted. Retry the image
              // once at that boundary; accepted work never falls back.
              if (
                input.recovery === "exactWorkspaceRequired" ||
                !snapshotId ||
                cause.reasons.length === 0 ||
                !cause.reasons.every(
                  (reason) =>
                    Cause.isFailReason(reason) &&
                    Predicate.isTagged(reason.error, "ContainerError"),
                ) ||
                current.timings.ownerReadyAt !== null
              )
                return yield* Effect.failCause(cause);

              return yield* Effect.gen(function* () {
                yield* state.storage.kv.put(
                  "last-restore-failure",
                  failureDiagnostic(cause, { operation: "SandboxContainer.restore" }),
                );
                yield* container.destroy();
                if (yield* container.running)
                  return yield* error("restore.cleanup", "Native stop is not confirmed");
                if (current) yield* release(current.instanceId);
                yield* state.storage.kv.delete("workspace-checkpoint");
                yield* state.storage.kv.delete("container-identity");
                yield* state.storage.kv.delete("start-retry-at");
                const { checkpoint: _checkpoint, ...fresh } = input;

                return yield* ensureAttempt(fresh).pipe(
                  Effect.tap(() =>
                    reportSanitizedCause(cause, { operation: "SandboxContainer.restore" }),
                  ),
                );
              }).pipe(
                Effect.catchCause((freshCause) =>
                  Effect.failCause(Cause.combine(cause, freshCause)),
                ),
              );
            }),
          ),
        ),
      ),
    );
  const revokePreview = Effect.gen(function* () {
    const saved = yield* read("preview", PreviewRecord);

    if (!saved) return;
    const coordinator = yield* namespace.getByName(CAPACITY_NAME);
    const reply = yield* namespace
      .call(coordinator, "registerPreview", saved.hash, null, 0)
      .pipe(Effect.catchCause(preserveCapacityFailure("preview.revoke")));

    yield* decodeNativeReply("preview.revoke", reply);
    yield* state.storage.kv.delete("preview");
  });
  const stop = Effect.gen(function* () {
    const current = yield* identity;

    yield* revokePreview;
    yield* outboundOwner.clear;
    if (current) yield* state.storage.kv.put("stopping-instance", current.instanceId);
    yield* container.destroy();
    if (yield* container.running) return yield* error("stop", "Native stop is not confirmed");
    if (current) yield* release(current.instanceId);
    yield* state.storage.kv.delete("container-identity");
    yield* state.storage.kv.delete("process-supervisor-token");
    yield* state.storage.kv.delete("supervisor-protocol");
    yield* state.storage.kv.delete("bootstrap-deadline");
    monitored = null;
  });
  const destroy = stop.pipe(
    Effect.andThen(state.storage.kv.delete("workspace-checkpoint")),
    Effect.asVoid,
  );
  const initialize = Effect.gen(function* () {
    // An evicted owner never rehydrates repository bearer credentials.
    yield* outboundOwner.clear;
    yield* state.storage.kv.delete("repository-authorizations");
    const current = yield* identity;

    if (!(yield* container.running)) {
      if (current) yield* stop;

      return;
    }
    if (!current) return yield* error("initialize", "Running container has no identity");
    yield* refreshInactivity;
    yield* configureOutbound;
    yield* monitor(current);
  });
  const park = Effect.gen(function* () {
    if (!(yield* container.running)) return;
    yield* inspectProcess("park", []);
    yield* stop;
  });

  return {
    initialize: admitted(initialize),
    destroy: admitted(destroy),
    park: admitted(park),
    revokePreview,
    lease: (id: string, active: boolean, instanceId: string, renewal: boolean) =>
      Effect.gen(function* () {
        const current = yield* identity;

        if (current?.instanceId !== instanceId || !(yield* container.running)) {
          if (active) return yield* new LeaseLostError({ instanceId });

          return;
        }
        yield* inspectProcess("lease", [id, active, renewal]);
      }),
    pinImage: (imageDigest: string | null) =>
      admitted(
        Effect.gen(function* () {
          if (imageDigest === null) {
            yield* state.storage.kv.delete("pinned-image");

            return;
          }
          const images = yield* container.images;

          if (
            !Object.values(images).includes(imageDigest) ||
            !/@sha256:[a-f0-9]{64}$/.test(imageDigest)
          )
            return yield* error("pinImage", "Only a configured immutable image may be pinned");
          yield* state.storage.kv.put("pinned-image", imageDigest);
        }),
      ),
    capacityInUse: (instanceId: string) =>
      Effect.gen(function* () {
        const current = yield* identity;

        if (yield* container.running) return current === null || current.instanceId === instanceId;

        return (
          current?.instanceId === instanceId &&
          (yield* read("stopping-instance", Schema.String)) !== instanceId &&
          ((yield* read("bootstrap-deadline", Schema.Finite)) ?? 0) >
            (yield* Clock.currentTimeMillis)
        );
      }),
    reserveCapacity: Effect.fn("SandboxContainer.reserveCapacity")(function* (
      id: string,
      instanceId: string,
    ) {
      yield* Effect.annotateCurrentSpan("sandbox.container.instanceId", instanceId);
      const reservations =
        (yield* read("capacity-leases", Schema.Array(CapacityReservation))) ?? [];

      // Reclaim on demand after the exact owner's native stopped observation.
      // Timestamps alone never release a live container's reservation.
      if (
        reservations.length >= maximum ||
        reservations.some((entry) => entry.id === id && entry.instanceId !== instanceId)
      ) {
        for (const reservation of reservations) {
          const owner = yield* namespace.get(yield* namespace.idFromString(reservation.id));
          const reply = yield* namespace
            .call(owner, "capacityInUse", reservation.instanceId)
            .pipe(Effect.catchCause(preserveCapacityFailure("capacity.observe")));
          const inUse = yield* decodeNativeReply("capacity.observe", reply);

          if (!inUse)
            yield* state.storage.transactionSync(() =>
              Effect.gen(function* () {
                const latest =
                  (yield* read("capacity-leases", Schema.Array(CapacityReservation))) ?? [];

                yield* state.storage.kv.put(
                  "capacity-leases",
                  latest.filter(
                    (entry) =>
                      entry.id !== reservation.id || entry.instanceId !== reservation.instanceId,
                  ),
                );
              }),
            );
        }
      }

      const reserved = yield* state.storage.transactionSync(() =>
        Effect.gen(function* () {
          const previous =
            (yield* read("capacity-leases", Schema.Array(CapacityReservation))) ?? [];

          if (previous.some((entry) => entry.id === id && entry.instanceId !== instanceId))
            return false;
          const entries = previous.filter((entry) => entry.id !== id);

          if (entries.length >= maximum) return false;
          yield* state.storage.kv.put("capacity-leases", [...entries, { id, instanceId }]);

          return true;
        }),
      );
      // Keep reply durability inside the measured operation, including declined requests.
      yield* state.storage
        .sync()
        .pipe(Effect.withSpan("SandboxContainer.capacityReceiptOutputGate"));

      return reserved;
    }),
    releaseCapacity: (id: string, instanceId: string) =>
      state.storage.transactionSync(() =>
        Effect.gen(function* () {
          const entries = (yield* read("capacity-leases", Schema.Array(CapacityReservation))) ?? [];

          yield* state.storage.kv.put(
            "capacity-leases",
            entries.filter((entry) => entry.id !== id || entry.instanceId !== instanceId),
          );
        }),
      ),
    ensure,
    restore: (checkpoint: SandboxCheckpoint, input: EnsureOptions = {}) =>
      boot.withPermits(1)(
        protectNativeIO(
          Schema.decodeEffect(SandboxCheckpoint)(checkpoint).pipe(
            Effect.flatMap((saved) =>
              ensureAttempt({ ...input, checkpoint: saved, recovery: "freshAllowed" }, true),
            ),
          ),
        ),
      ),
    describe,
    touch,
    protectNativeIO,
  };
});

export class ContainerLifecycle extends Context.Service<
  ContainerLifecycle,
  Effect.Success<typeof make>
>()("kommunikasie/ContainerLifecycle") {
  static readonly layer = Layer.effect(ContainerLifecycle, make);
}
