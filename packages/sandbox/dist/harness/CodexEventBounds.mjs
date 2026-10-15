import { toJson } from "../Http.mjs";
import { HarnessTurnError } from "./Harness.mjs";
import { Effect, Predicate } from "effect";
//#region src/harness/CodexEventBounds.ts
/** Maximum record forwarded into the durable event stream. */
const CODEX_EVENT_BYTE_LIMIT = 196608;
/** Maximum record decoded even when its tool result can be compacted. */
const CODEX_MESSAGE_BYTE_LIMIT = 8388608;
const encoder = new TextEncoder();
const truncationMarker = (itemType, byteLength) => `[sandbox-agent truncated oversized ${itemType} tool result; original event was ${byteLength} bytes]`;
const recoverToolResult = (event, byteLength) => {
	if (event.type !== "item.completed") return null;
	const item = event["item"];
	if (!Predicate.isObject(item) || !Predicate.hasProperty(item, "type")) return null;
	if (item.type === "command_execution") {
		if (!Predicate.hasProperty(item, "aggregated_output") || !Predicate.isString(item.aggregated_output)) return null;
		return {
			event: {
				...event,
				item: {
					...item,
					aggregated_output: truncationMarker("command_execution", byteLength)
				}
			},
			itemType: "command_execution"
		};
	}
	if (item.type !== "mcp_tool_call") return null;
	if (Predicate.hasProperty(item, "result") && item.result !== null) return {
		event: {
			...event,
			item: {
				...item,
				result: {
					content: [{
						type: "text",
						text: truncationMarker("mcp_tool_call", byteLength)
					}],
					structured_content: null
				}
			}
		},
		itemType: "mcp_tool_call"
	};
	if (Predicate.hasProperty(item, "error") && Predicate.isObject(item.error) && Predicate.hasProperty(item.error, "message") && Predicate.isString(item.error.message)) return {
		event: {
			...event,
			item: {
				...item,
				error: {
					...item.error,
					message: truncationMarker("mcp_tool_call", byteLength)
				}
			}
		},
		itemType: "mcp_tool_call"
	};
	return null;
};
const oversizedEvent = (message) => HarnessTurnError.make({
	reason: "oversizedEvent",
	message
});
const boundCodexEvent = Effect.fn("CodexEventBounds.boundCodexEvent")(function* (event) {
	const byteLength = encoder.encode(toJson(event)).byteLength;
	if (byteLength <= 196608) return event;
	const recovered = recoverToolResult(event, byteLength);
	if (recovered === null) return yield* oversizedEvent(`Codex event exceeded ${CODEX_EVENT_BYTE_LIMIT} bytes and was not a recoverable tool result`);
	const compactedBytes = encoder.encode(toJson(recovered.event)).byteLength;
	if (compactedBytes > 196608) return yield* oversizedEvent(`Codex ${recovered.itemType} record remained oversized after result compaction`);
	yield* Effect.logWarning("sandbox-agent compacted oversized Codex tool result").pipe(Effect.annotateLogs({
		itemType: recovered.itemType,
		originalEventBytes: byteLength,
		compactedEventBytes: compactedBytes
	}));
	return recovered.event;
});
//#endregion
export { CODEX_EVENT_BYTE_LIMIT, CODEX_MESSAGE_BYTE_LIMIT, boundCodexEvent };
