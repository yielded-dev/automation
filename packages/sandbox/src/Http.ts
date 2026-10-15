import { Effect, Schema } from "effect";

import { SESSION_ID_PATTERN_SOURCE, type AgentEvent, type SessionId } from "./Contract";
import { jsonByteLengthWithin, MAX_AGENT_EVENT_BYTES } from "./RunRecord";

/**
 * HTTP surface helpers for the sandbox-agent protocol: session route parsing
 * and the JSON/NDJSON encoders shared by hosts and clients.
 */

/** Route recognized by a session host after method and path validation. */
export type SessionRoute =
  | { readonly kind: "state"; readonly sessionId: SessionId }
  | { readonly kind: "message"; readonly sessionId: SessionId }
  | { readonly kind: "restart"; readonly sessionId: SessionId }
  | { readonly kind: "simulateReplacement"; readonly sessionId: SessionId }
  | { readonly kind: "delete"; readonly sessionId: SessionId };

const routePattern = new RegExp(
  `^\\/sessions\\/(${SESSION_ID_PATTERN_SOURCE})(?:\\/(messages|restart|simulate-replacement))?$`,
  "u",
);

/** Parses method + path into a route; `null` is an ordinary 404. */
export const parseSessionRoute = (method: string, pathname: string): SessionRoute | null => {
  const match = routePattern.exec(pathname);

  if (match === null) return null;
  const sessionId: SessionId | undefined = match[1];

  if (sessionId === undefined) return null;
  const action = match[2];

  if (action === undefined) {
    if (method === "GET") return { kind: "state", sessionId };
    if (method === "DELETE") return { kind: "delete", sessionId };

    return null;
  }
  if (method !== "POST") return null;
  if (action === "messages") return { kind: "message", sessionId };
  if (action === "restart") return { kind: "restart", sessionId };

  return { kind: "simulateReplacement", sessionId };
};

/** Single JSON wire encoder so the encoding exception lives in one place. */
// oxlint-disable-next-line no-restricted-properties -- NDJSON/JSON wire encoding boundary.
export const toJson = (value: unknown): string => JSON.stringify(value);

/** Encodes one event as exactly one newline-terminated NDJSON record. */
export const ndjsonLine = (event: AgentEvent): string => `${toJson(event)}\n`;

/** Creates a newline-terminated JSON response with the canonical content type. */
export const jsonResponse = (value: unknown, status = 200): Response =>
  new Response(`${toJson(value)}\n`, {
    status,
    headers: { "content-type": "application/json" },
  });

/** Creates the protocol's `{ error }` JSON response shape. */
export const errorResponse = (status: number, message: string): Response =>
  jsonResponse({ error: message }, status);

/** Failure while consuming the bounded request stream. */
export class RequestBodyReadError extends Schema.TaggedError<RequestBodyReadError>()(
  "RequestBodyReadError",
  {
    operation: Schema.Literals(["getReader", "read", "cancel", "releaseLock"]),
    cause: Schema.Defect(),
  },
) {}

const bodyReadError = (
  operation: "getReader" | "read" | "cancel" | "releaseLock",
  cause: unknown,
) => RequestBodyReadError.make({ operation, cause });

/**
 * Reads at most `limit` body bytes; `null` means the limit was exceeded.
 * The stream is capped while reading — a missing or lying Content-Length
 * cannot smuggle an oversized body past the check — and an oversized body is
 * rejected without ever being fully buffered.
 */
export const readBodyBounded = (
  request: Request,
  limit: number,
): Effect.Effect<string | null, RequestBodyReadError> =>
  Effect.gen(function* () {
    const declared = request.headers.get("content-length");
    const body = request.body;

    if (declared !== null && Number(declared) > limit) {
      if (body !== null) {
        yield* Effect.tryPromise({
          try: () => body.cancel("declared request body exceeded configured limit"),
          catch: (cause) => bodyReadError("cancel", cause),
        }).pipe(
          Effect.catch((error) =>
            Effect.logError(
              "sandbox-agent declared oversized request body cancel failed",
              error.cause,
            ),
          ),
        );
      }

      return null;
    }

    if (body === null) return "";
    const reader = yield* Effect.try({
      try: () => body.getReader(),
      catch: (cause) => bodyReadError("getReader", cause),
    });
    const releaseReader = Effect.try({
      try: () => reader.releaseLock(),
      catch: (cause) => bodyReadError("releaseLock", cause),
    }).pipe(
      Effect.catch((error) =>
        Effect.logError("sandbox-agent request body reader release failed", error.cause),
      ),
    );
    const cancelReader = (reason: string) =>
      Effect.tryPromise({
        try: () => reader.cancel(reason),
        catch: (cause) => bodyReadError("cancel", cause),
      }).pipe(
        Effect.catch((error) =>
          Effect.logError("sandbox-agent request body reader cancel failed", error.cause),
        ),
      );

    return yield* Effect.gen(function* () {
      const chunks: Array<Uint8Array> = [];
      let total = 0;

      for (;;) {
        const { done, value } = yield* Effect.tryPromise({
          try: () => reader.read(),
          catch: (cause) => bodyReadError("read", cause),
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
    }).pipe(
      Effect.onError(() => cancelReader("request body read failed")),
      Effect.ensuring(releaseReader),
    );
  });

export const MAX_NDJSON_QUEUE_BYTES = 256 * 1024;

export type NdjsonWriteResult = "written" | "disconnected" | "overflow";

/** A live NDJSON body with a byte-counted native queue. Client cancellation
 * clears the controller immediately while the caller's background work keeps
 * running independently. */
export interface BoundedNdjsonTransport {
  readonly readable: ReadableStream<Uint8Array>;
  readonly write: (event: AgentEvent) => NdjsonWriteResult;
  readonly close: () => void;
  readonly disconnect: () => void;
}

export const makeBoundedNdjsonTransport = (signal: AbortSignal): BoundedNdjsonTransport => {
  const encoder = new TextEncoder();
  let controller: ReadableStreamDefaultController<Uint8Array> | null = null;
  let state: "open" | "disconnected" | "closed" = "open";
  const detachAbort = () => signal.removeEventListener("abort", disconnect);
  const finish = (next: "disconnected" | "closed", cause?: unknown): void => {
    if (state !== "open") return;
    state = next;
    detachAbort();
    const active = controller;

    controller = null;
    if (active === null) return;

    try {
      if (cause === undefined) active.close();
      else active.error(cause);
    } catch {
      // Cancellation can race a final close. State already prevents further
      // writes, so there is no remaining resource to clean up.
    }
  };

  function disconnect(): void {
    finish("disconnected");
  }
  const readable = new ReadableStream<Uint8Array>(
    {
      start: (next) => {
        controller = next;
        if (signal.aborted) disconnect();
      },
      cancel: () => disconnect(),
    },
    {
      highWaterMark: MAX_NDJSON_QUEUE_BYTES,
      size: (chunk) => chunk.byteLength,
    },
  );

  if (state === "open") signal.addEventListener("abort", disconnect, { once: true });

  return {
    readable,
    write: (event) => {
      if (state !== "open" || controller === null) return "disconnected";
      const eventBytes = jsonByteLengthWithin(event, MAX_AGENT_EVENT_BYTES - 1);

      if (eventBytes === null) {
        finish(
          "disconnected",
          new Error("sandbox-agent NDJSON event exceeded its bounded record size"),
        );

        return "overflow";
      }
      const chunk = encoder.encode(ndjsonLine(event));
      const available = controller.desiredSize;

      if (
        chunk.byteLength !== eventBytes + 1 ||
        available === null ||
        available < chunk.byteLength
      ) {
        finish(
          "disconnected",
          new Error("sandbox-agent NDJSON response exceeded its bounded live queue"),
        );

        return "overflow";
      }
      controller.enqueue(chunk);

      return "written";
    },
    close: () => finish("closed"),
    disconnect,
  };
};
