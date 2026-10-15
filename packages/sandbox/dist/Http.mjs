import { SESSION_ID_PATTERN_SOURCE } from "./Contract.mjs";
import { MAX_AGENT_EVENT_BYTES, jsonByteLengthWithin } from "./RunRecord.mjs";
import { Effect, Schema } from "effect";
//#region src/Http.ts
const routePattern = new RegExp(`^\\/sessions\\/(${SESSION_ID_PATTERN_SOURCE})(?:\\/(messages|restart|simulate-replacement))?$`, "u");
/** Parses method + path into a route; `null` is an ordinary 404. */
const parseSessionRoute = (method, pathname) => {
	const match = routePattern.exec(pathname);
	if (match === null) return null;
	const sessionId = match[1];
	if (sessionId === void 0) return null;
	const action = match[2];
	if (action === void 0) {
		if (method === "GET") return {
			kind: "state",
			sessionId
		};
		if (method === "DELETE") return {
			kind: "delete",
			sessionId
		};
		return null;
	}
	if (method !== "POST") return null;
	if (action === "messages") return {
		kind: "message",
		sessionId
	};
	if (action === "restart") return {
		kind: "restart",
		sessionId
	};
	return {
		kind: "simulateReplacement",
		sessionId
	};
};
/** Single JSON wire encoder so the encoding exception lives in one place. */
const toJson = (value) => JSON.stringify(value);
/** Encodes one event as exactly one newline-terminated NDJSON record. */
const ndjsonLine = (event) => `${toJson(event)}\n`;
/** Creates a newline-terminated JSON response with the canonical content type. */
const jsonResponse = (value, status = 200) => new Response(`${toJson(value)}\n`, {
	status,
	headers: { "content-type": "application/json" }
});
/** Creates the protocol's `{ error }` JSON response shape. */
const errorResponse = (status, message) => jsonResponse({ error: message }, status);
/** Failure while consuming the bounded request stream. */
var RequestBodyReadError = class extends Schema.TaggedError()("RequestBodyReadError", {
	operation: Schema.Literals([
		"getReader",
		"read",
		"cancel",
		"releaseLock"
	]),
	cause: Schema.Defect()
}) {};
const bodyReadError = (operation, cause) => RequestBodyReadError.make({
	operation,
	cause
});
/**
* Reads at most `limit` body bytes; `null` means the limit was exceeded.
* The stream is capped while reading — a missing or lying Content-Length
* cannot smuggle an oversized body past the check — and an oversized body is
* rejected without ever being fully buffered.
*/
const readBodyBounded = (request, limit) => Effect.gen(function* () {
	const declared = request.headers.get("content-length");
	const body = request.body;
	if (declared !== null && Number(declared) > limit) {
		if (body !== null) yield* Effect.tryPromise({
			try: () => body.cancel("declared request body exceeded configured limit"),
			catch: (cause) => bodyReadError("cancel", cause)
		}).pipe(Effect.catch((error) => Effect.logError("sandbox-agent declared oversized request body cancel failed", error.cause)));
		return null;
	}
	if (body === null) return "";
	const reader = yield* Effect.try({
		try: () => body.getReader(),
		catch: (cause) => bodyReadError("getReader", cause)
	});
	const releaseReader = Effect.try({
		try: () => reader.releaseLock(),
		catch: (cause) => bodyReadError("releaseLock", cause)
	}).pipe(Effect.catch((error) => Effect.logError("sandbox-agent request body reader release failed", error.cause)));
	const cancelReader = (reason) => Effect.tryPromise({
		try: () => reader.cancel(reason),
		catch: (cause) => bodyReadError("cancel", cause)
	}).pipe(Effect.catch((error) => Effect.logError("sandbox-agent request body reader cancel failed", error.cause)));
	return yield* Effect.gen(function* () {
		const chunks = [];
		let total = 0;
		for (;;) {
			const { done, value } = yield* Effect.tryPromise({
				try: () => reader.read(),
				catch: (cause) => bodyReadError("read", cause)
			});
			if (done) break;
			total += value.byteLength;
			if (total > limit) {
				yield* cancelReader("request body exceeded configured limit");
				return null;
			}
			chunks.push(value);
		}
		const joined = new Uint8Array(total);
		let offset = 0;
		for (const chunk of chunks) {
			joined.set(chunk, offset);
			offset += chunk.byteLength;
		}
		return new TextDecoder().decode(joined);
	}).pipe(Effect.onError(() => cancelReader("request body read failed")), Effect.ensuring(releaseReader));
});
const MAX_NDJSON_QUEUE_BYTES = 262144;
const makeBoundedNdjsonTransport = (signal) => {
	const encoder = new TextEncoder();
	let controller = null;
	let state = "open";
	const detachAbort = () => signal.removeEventListener("abort", disconnect);
	const finish = (next, cause) => {
		if (state !== "open") return;
		state = next;
		detachAbort();
		const active = controller;
		controller = null;
		if (active === null) return;
		try {
			if (cause === void 0) active.close();
			else active.error(cause);
		} catch {}
	};
	function disconnect() {
		finish("disconnected");
	}
	const readable = new ReadableStream({
		start: (next) => {
			controller = next;
			if (signal.aborted) disconnect();
		},
		cancel: () => disconnect()
	}, {
		highWaterMark: MAX_NDJSON_QUEUE_BYTES,
		size: (chunk) => chunk.byteLength
	});
	if (state === "open") signal.addEventListener("abort", disconnect, { once: true });
	return {
		readable,
		write: (event) => {
			if (state !== "open" || controller === null) return "disconnected";
			const eventBytes = jsonByteLengthWithin(event, MAX_AGENT_EVENT_BYTES - 1);
			if (eventBytes === null) {
				finish("disconnected", /* @__PURE__ */ new Error("sandbox-agent NDJSON event exceeded its bounded record size"));
				return "overflow";
			}
			const chunk = encoder.encode(ndjsonLine(event));
			const available = controller.desiredSize;
			if (chunk.byteLength !== eventBytes + 1 || available === null || available < chunk.byteLength) {
				finish("disconnected", /* @__PURE__ */ new Error("sandbox-agent NDJSON response exceeded its bounded live queue"));
				return "overflow";
			}
			controller.enqueue(chunk);
			return "written";
		},
		close: () => finish("closed"),
		disconnect
	};
};
//#endregion
export { MAX_NDJSON_QUEUE_BYTES, RequestBodyReadError, errorResponse, jsonResponse, makeBoundedNdjsonTransport, ndjsonLine, parseSessionRoute, readBodyBounded, toJson };
