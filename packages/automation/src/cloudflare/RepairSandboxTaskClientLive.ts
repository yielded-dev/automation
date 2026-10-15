import { Cause, Effect, Layer, Schema } from "effect";
import {
  RepairSandboxTaskClient,
  RepairSandboxTaskTransportError,
} from "../server/RepairSandboxTaskClient";
import { RepairSandboxTaskObject, repairSandboxTaskObjectName } from "./RepairSandboxTaskObject";
import type { ConflictError, ServiceUnavailableError } from "../domain/errors";

const unpack = <A>(
  outcome:
    | { readonly _tag: "Success"; readonly value: A }
    | ConflictError
    | ServiceUnavailableError,
) => (outcome._tag === "Success" ? Effect.succeed(outcome.value) : Effect.fail(outcome));

/** Connects the engine directly to its own deployment's coordinator namespace. */
export const RepairSandboxTaskClientLive = (options: {
  readonly binding: string;
  /** Best-effort native placement preference for new task objects. */
  readonly locationHint?: DurableObjectNamespaceGetDurableObjectOptions["locationHint"];
}) =>
  Layer.effect(
    RepairSandboxTaskClient,
    Effect.gen(function* () {
      const objects = yield* RepairSandboxTaskObject;
      const object = (id: string) =>
        objects.byName(
          repairSandboxTaskObjectName(id),
          options.locationHint === undefined ? undefined : { locationHint: options.locationHint },
        );
      const call = <A, E>(operation: string, effect: Effect.Effect<A, E>) =>
        effect.pipe(
          Effect.catchCause((cause) =>
            Effect.failCause(
              Cause.map(cause, (error) =>
                RepairSandboxTaskTransportError.make({ operation, cause: error }),
              ),
            ),
          ),
        );
      return RepairSandboxTaskClient.of({
        create: (request) =>
          call("create", object(request.id).create(request).pipe(Effect.flatMap(unpack))),
        inspect: (id) => call("inspect", object(id).inspect(id).pipe(Effect.flatMap(unpack))),
        snapshot: (id) => call("snapshot", object(id).snapshot(id).pipe(Effect.flatMap(unpack))),
        events: (id, after, limit = 1000) =>
          call("events", object(id).events(id, after, limit).pipe(Effect.flatMap(unpack))),
        cancel: (id, commandId, admission) =>
          call(
            "cancel",
            object(id)
              .cancel(id, commandId, admission ?? null)
              .pipe(Effect.flatMap(unpack)),
          ),
        native: (id, request, schema) =>
          call(
            "native",
            object(id)
              .native(id, request)
              .pipe(Effect.flatMap(unpack), Effect.flatMap(Schema.decodeUnknownEffect(schema))),
          ),
        forward: (id, purpose, operationId, nativeToken, original) => {
          const headers = new Headers(original.headers);
          headers.set("x-native-authorization", nativeToken);
          return call(
            "forward",
            object(id).forward(id, purpose, operationId, new Request(original, { headers })),
          );
        },
      });
    }),
  ).pipe(Layer.provide(RepairSandboxTaskObject.layer(options)));
