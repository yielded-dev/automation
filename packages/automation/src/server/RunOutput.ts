import { Effect, Schema } from "effect";
import { RunEvent } from "../domain/RunOutput";
import { RepairSandboxTaskClient } from "./RepairSandboxTaskClient";
import { RepairExecutionContext } from "./RepairExecutionContext";

export const readRunEvents = Effect.fn("RepairExecution.events")(function* (
  after = 0,
  limit = 1000,
) {
  const client = yield* RepairSandboxTaskClient;
  const runId = yield* RepairExecutionContext;
  const events = yield* client.events(runId, after, limit);

  return yield* Effect.forEach(events, (event) =>
    Effect.gen(function* () {
      let value: Schema.Json;

      if (event.kind === "codex") {
        // oxlint-disable-next-line no-restricted-properties -- reconstruct the existing codex event from the decoded transport payload.
        const record = yield* Schema.decodeUnknownEffect(Schema.Record(Schema.String, Schema.Json))(
          event.payload,
        );
        value = { type: "codex.event", event: { type: event.method, ...record } };
      } else {
        // oxlint-disable-next-line no-restricted-properties -- shared SandboxTaskEvent payload is intentionally unknown at this transport boundary.
        const payload = yield* Schema.decodeUnknownEffect(Schema.Json)(event.payload);
        value = { type: event.method, value: payload };
      }

      let body = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(value);
      let bytes = new TextEncoder().encode(body).byteLength;

      if (bytes > 131_072) {
        body = '{"type":"repair.output_truncated"}';
        bytes = body.length;
      }

      return RunEvent.make({
        sequence: event.sequence,
        occurredAt: Date.parse(event.createdAt),
        body,
        bytes,
      });
    }),
  );
});
