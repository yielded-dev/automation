import { Clock, Context, Effect, Layer, Result, Schema } from "effect";
import { DurableObjectContainer, DurableObjectState } from "effect-cf";

import { ContainerState, SupervisorReply, error, NativeProcessError } from "./ContainerState";
import { PROCESS_SUPERVISOR_PORT } from "./ProcessSupervisorSource";

/** Supervisors before protocol 2 answer `/health` with plain text. */
const HealthReply = Schema.Struct({ protocol: Schema.Int });

const make = Effect.gen(function* () {
  const container = yield* DurableObjectContainer.DurableObjectContainer;
  const state = yield* DurableObjectState.DurableObjectState;

  const { read } = yield* ContainerState;
  const supervisorCall = Effect.fnUntraced(function* (
    method:
      | "allocateTicket"
      | "launchJob"
      | "observeJob"
      | "listJobs"
      | "signalJob"
      | "waitJob"
      | "acknowledgeJob"
      | "touch"
      | "protect"
      | "lease"
      | "park"
      | "waitForPort",
    args: ReadonlyArray<unknown>,
    token: string,
  ) {
    const response = yield* (yield* container.getTcpPort(PROCESS_SUPERVISOR_PORT)).fetch(
      `http://localhost:${PROCESS_SUPERVISOR_PORT}/rpc`,
      {
        method: "POST",
        headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
        // oxlint-disable-next-line no-restricted-properties -- private supervisor request carries job configuration over its authenticated socket.
        body: JSON.stringify({ method, args }),
      },
    );
    const body = yield* Effect.tryPromise({
      try: () => response.text(),
      catch: (cause) => new DurableObjectContainer.ContainerError({ operation: method, cause }),
    });
    const result = yield* Schema.decodeEffect(Schema.fromJsonString(SupervisorReply))(body);

    if (!result.ok)
      return yield* new NativeProcessError({
        operation: method,
        code: result.error.code,
        cause: result.error,
      });

    return result.value;
  });
  // A container that outlives a Worker deployment keeps its boot-installed
  // supervisor, so the owner learns the protocol from the running supervisor
  // rather than assuming its own source version.
  const readProtocol = Effect.gen(function* () {
    const response = yield* (yield* container.getTcpPort(PROCESS_SUPERVISOR_PORT)).fetch(
      `http://localhost:${PROCESS_SUPERVISOR_PORT}/health`,
    );
    const body = yield* Effect.tryPromise({
      try: () => response.text(),
      catch: (cause) => new DurableObjectContainer.ContainerError({ operation: "health", cause }),
    });
    const decoded = yield* Schema.decodeEffect(Schema.fromJsonString(HealthReply))(body).pipe(
      Effect.result,
    );
    const protocol = Result.isSuccess(decoded) ? decoded.success.protocol : 1;

    yield* state.storage.kv.put("supervisor-protocol", protocol);

    return protocol;
  });
  const ensureSupervisor = Effect.gen(function* () {
    yield* container
      .waitForHttp(PROCESS_SUPERVISOR_PORT, {
        path: "/health",
        timeout: 30_000,
        status: { min: 200, max: 299 },
      })
      .pipe(Effect.withSpan("SandboxContainer.supervisorHttpReady"));
    yield* readProtocol.pipe(Effect.withSpan("SandboxContainer.supervisorProtocol"));
    // Readiness alone cannot establish this boot's authenticated ownership.
    yield* inspectProcess("listJobs", []).pipe(
      Effect.withSpan("SandboxContainer.supervisorAuthenticated"),
    );
  });
  const inspectProcess = Effect.fnUntraced(function* (
    method: Parameters<typeof supervisorCall>[0],
    args: ReadonlyArray<unknown>,
  ) {
    if (!(yield* container.running)) {
      if (method === "listJobs") return [];
      if (["observeJob", "waitJob", "signalJob", "acknowledgeJob"].includes(method)) return null;

      return yield* new NativeProcessError({
        operation: method,
        code: method === "lease" ? "LEASE_LOST" : "PROCESS_LOST",
        cause: new Error("Container boot stopped"),
      });
    }
    const token = yield* read("process-supervisor-token", Schema.String);

    if (!token) return yield* error(method, "Process supervisor boot capability is unavailable");

    return yield* supervisorCall(method, args, token);
  });
  /** Protect one native operation for `millis`, or release it with `null`. */
  const protect = Effect.fnUntraced(function* (id: string, millis: number | null) {
    if (millis === null) return yield* inspectProcess("protect", [id, null]);
    const protocol = (yield* read("supervisor-protocol", Schema.Int)) ?? (yield* readProtocol);

    // Protocol 2 applies the window on the container's own clock. Earlier
    // supervisors only accept a deadline on the Worker clock, which rejects
    // the operation when the VM clock lags; keep their behaviour unchanged.
    return yield* inspectProcess("protect", [
      id,
      protocol >= 2 ? millis : (yield* Clock.currentTimeMillis) + millis,
    ]);
  });

  return {
    inspectProcess,
    ensureSupervisor,
    protect,
  };
});

export class ContainerSupervisor extends Context.Service<
  ContainerSupervisor,
  Effect.Success<typeof make>
>()("kommunikasie/ContainerSupervisor") {
  static readonly layer = Layer.effect(ContainerSupervisor, make);
}
