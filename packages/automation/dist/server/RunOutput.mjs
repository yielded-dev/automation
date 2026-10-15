import { RunEvent } from "../domain/RunOutput.mjs";
import { RepairSandboxTaskClient } from "./RepairSandboxTaskClient.mjs";
import { RepairExecutionContext } from "./RepairExecutionContext.mjs";
import { Effect, Schema } from "effect";
//#region src/server/RunOutput.ts
const readRunEvents = Effect.fn("RepairExecution.events")(function* (after = 0, limit = 1e3) {
	const client = yield* RepairSandboxTaskClient;
	const runId = yield* RepairExecutionContext;
	const events = yield* client.events(runId, after, limit);
	return yield* Effect.forEach(events, (event) => Effect.gen(function* () {
		const payload = yield* Schema.decodeUnknownEffect(Schema.Json)(event.payload);
		const record = yield* Schema.decodeUnknownEffect(Schema.Record(Schema.String, Schema.Json))(event.payload);
		const value = event.kind === "codex" ? {
			type: "codex.event",
			event: {
				type: event.method,
				...record
			}
		} : {
			type: event.method,
			value: payload
		};
		let body = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(value);
		let bytes = new TextEncoder().encode(body).byteLength;
		if (bytes > 131072) {
			body = "{\"type\":\"repair.output_truncated\"}";
			bytes = body.length;
		}
		return RunEvent.make({
			sequence: event.sequence,
			occurredAt: Date.parse(event.createdAt),
			body,
			bytes
		});
	}));
});
//#endregion
export { readRunEvents };
