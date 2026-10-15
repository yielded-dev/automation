import { Schema } from "effect";

import type {
  AgentEvent,
  AgentTimings,
  ContainerSize,
  HarnessName,
  ReasoningEffort,
  RunLifecycle,
  WorkspaceMaterialization,
} from "./Contract";

/** Archive retention bounds. Live transport has its own, smaller queue bound. */
export const MAX_RECORDED_EVENT_BYTES = 4 * 1024 * 1024;
export const MAX_RECORDED_EVENT_COUNT = 4_000;
export const MAX_AGENT_EVENT_BYTES = 256 * 1024;

/** One observed server-side phase span, relative to request acceptance. */
export const SpanRecord = Schema.Struct({
  name: Schema.String,
  startAt: Schema.Finite,
  endAt: Schema.NullOr(Schema.Finite),
  outcome: Schema.Literals(["ok", "failed", "unknown"]),
  attempts: Schema.optionalKey(Schema.Int),
});
export type SpanRecord = typeof SpanRecord.Type;

/** One streamed protocol event with its server-relative receive offset. */
export interface RecordedAgentEvent {
  /** Milliseconds since the message request was accepted. */
  readonly at: number;
  readonly event: AgentEvent;
}

/** What the bounded recorder retained from the full live event sequence. */
export interface RunEventCapture {
  readonly retainedCount: number;
  readonly droppedCount: number;
  /** UTF-8 bytes of the retained event records before enclosing-document JSON. */
  readonly retainedBytes: number;
  readonly limitBytes: number;
}

export interface BoundedRunRecorder {
  /** Records one event. Lifecycle and terminal records displace older dialect
   * records when the recorder reaches either bound. */
  readonly append: (record: RecordedAgentEvent) => void;
  /** Takes an immutable snapshot for the run sink. */
  readonly snapshot: () => {
    readonly events: ReadonlyArray<RecordedAgentEvent>;
    readonly eventCapture: RunEventCapture;
  };
}

const MAX_JSON_DEPTH = 64;

const jsonStringByteLengthWithin = (value: string, limit: number): number | null => {
  let bytes = 2;

  if (bytes > limit) return null;
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    let nextBytes: number;

    if (
      code === 0x22 ||
      code === 0x5c ||
      code === 0x08 ||
      code === 0x09 ||
      code === 0x0a ||
      code === 0x0c ||
      code === 0x0d
    ) {
      nextBytes = 2;
    } else if (code <= 0x1f) {
      nextBytes = 6;
    } else if (code >= 0xd800 && code <= 0xdbff) {
      const trailing = value.charCodeAt(index + 1);

      if (trailing >= 0xdc00 && trailing <= 0xdfff) {
        nextBytes = 4;
        index += 1;
      } else {
        nextBytes = 6;
      }
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      nextBytes = 6;
    } else if (code <= 0x7f) {
      nextBytes = 1;
    } else if (code <= 0x7ff) {
      nextBytes = 2;
    } else {
      nextBytes = 3;
    }
    bytes += nextBytes;
    if (bytes > limit) return null;
  }

  return bytes;
};

/**
 * Measures the exact UTF-8 size `JSON.stringify` will produce for ordinary
 * plain JSON data, stopping before the caller's byte limit. Exotic objects,
 * accessors, cycles, and pathologically deep values fail closed instead of
 * reaching the allocating serializer.
 */
export const jsonByteLengthWithin = (value: unknown, limit: number): number | null => {
  const ancestors = new Set<object>();
  let visited = 0;
  const measure = (
    current: unknown,
    remaining: number,
    depth: number,
  ): number | null | undefined => {
    if (remaining < 0 || depth > MAX_JSON_DEPTH || visited > limit) return null;
    if (current === null) return remaining >= 4 ? 4 : null;

    switch (typeof current) {
      case "string":
        return jsonStringByteLengthWithin(current, remaining);
      case "boolean": {
        const bytes = current ? 4 : 5;

        return bytes <= remaining ? bytes : null;
      }
      case "number": {
        const encoded = Number.isFinite(current) ? String(current) : "null";

        return encoded.length <= remaining ? encoded.length : null;
      }
      case "undefined":
      case "function":
      case "symbol":
        return undefined;
      case "bigint":
        return null;
      case "object":
        break;
    }

    if (ancestors.has(current)) return null;
    const prototype = Object.getPrototypeOf(current);

    if (
      Array.isArray(current)
        ? prototype !== Array.prototype
        : prototype !== Object.prototype && prototype !== null
    ) {
      return null;
    }
    let currentPrototype = prototype;

    while (currentPrototype !== null) {
      const inheritedToJson = Object.getOwnPropertyDescriptor(currentPrototype, "toJSON");

      if (
        inheritedToJson?.get !== undefined ||
        inheritedToJson?.set !== undefined ||
        typeof inheritedToJson?.value === "function"
      ) {
        return null;
      }
      currentPrototype = Object.getPrototypeOf(currentPrototype);
    }
    const toJsonDescriptor = Object.getOwnPropertyDescriptor(current, "toJSON");

    if (
      toJsonDescriptor !== undefined &&
      (toJsonDescriptor.get !== undefined ||
        toJsonDescriptor.set !== undefined ||
        typeof toJsonDescriptor.value === "function")
    ) {
      return null;
    }
    ancestors.add(current);
    visited += 1;
    let bytes = 1;

    if (Array.isArray(current)) {
      for (let index = 0; index < current.length; index += 1) {
        if (index > 0) bytes += 1;
        if (bytes > remaining) return null;
        const descriptor = Object.getOwnPropertyDescriptor(current, String(index));

        if (descriptor?.get !== undefined || descriptor?.set !== undefined) return null;
        const measured = measure(descriptor?.value, remaining - bytes, depth + 1);
        const itemBytes = measured === undefined ? 4 : measured;

        if (itemBytes === null) return null;
        bytes += itemBytes;
      }
    } else {
      let propertyCount = 0;

      for (const key in current) {
        if (!Object.hasOwn(current, key)) continue;
        propertyCount += 1;
        if (propertyCount > limit) return null;
        const descriptor = Object.getOwnPropertyDescriptor(current, key);

        if (
          descriptor === undefined ||
          descriptor.get !== undefined ||
          descriptor.set !== undefined
        ) {
          return null;
        }
        const measured = measure(descriptor.value, remaining - bytes, depth + 1);

        if (measured === undefined) continue;
        if (measured === null) return null;
        const keyBytes = jsonStringByteLengthWithin(key, remaining - bytes);

        if (keyBytes === null) return null;
        if (bytes > 1) bytes += 1;
        bytes += keyBytes + 1 + measured;
        if (bytes > remaining) return null;
      }
    }
    ancestors.delete(current);
    bytes += 1;

    return bytes <= remaining ? bytes : null;
  };
  const measured = measure(value, limit, 0);

  return measured === undefined ? null : measured;
};

type RetentionPriority = "ordinary" | "essential" | "terminal";

const priorityRank: Record<RetentionPriority, number> = {
  ordinary: 0,
  essential: 1,
  terminal: 2,
};

const eventPriority = (event: AgentEvent): RetentionPriority => {
  if (event.type === "agent.completed" || event.type === "agent.failed") return "terminal";
  if (
    event.type === "codex.event" &&
    event.event.type !== "turn.completed" &&
    event.event.type !== "turn.failed" &&
    event.event.type !== "error"
  ) {
    return "ordinary";
  }

  return "essential";
};

/**
 * Keeps archive memory bounded without changing the live protocol. Core
 * lifecycle records and the harness terminal record win space over ordinary
 * dialect records, so a truncated archive still explains how the run ended.
 */
export const makeBoundedRunRecorder = (): BoundedRunRecorder => {
  const retained: Array<{
    readonly record: RecordedAgentEvent;
    readonly bytes: number;
    readonly priority: RetentionPriority;
  }> = [];
  let retainedBytes = 0;
  let droppedCount = 0;

  const removeOldestLowerPriority = (priority: RetentionPriority): boolean => {
    let index = -1;

    for (let rank = 0; rank < priorityRank[priority]; rank += 1) {
      index = retained.findIndex(({ priority: candidate }) => priorityRank[candidate] === rank);
      if (index >= 0) break;
    }

    if (index === -1) return false;
    const removed = retained[index];

    if (removed === undefined) return false;
    retained.splice(index, 1);
    retainedBytes -= removed.bytes;
    droppedCount += 1;

    return true;
  };

  const append = (record: RecordedAgentEvent): void => {
    const bytes = jsonByteLengthWithin(record, MAX_AGENT_EVENT_BYTES);
    const priority = eventPriority(record.event);

    if (bytes === null) {
      droppedCount += 1;

      return;
    }
    if (priority !== "ordinary") {
      while (
        (retained.length >= MAX_RECORDED_EVENT_COUNT ||
          retainedBytes + bytes > MAX_RECORDED_EVENT_BYTES) &&
        removeOldestLowerPriority(priority)
      ) {
        // Keep making room until this record fits or no lower-priority records
        // remain. The final agent disposition can displace older essentials.
      }
    }
    if (
      retained.length >= MAX_RECORDED_EVENT_COUNT ||
      retainedBytes + bytes > MAX_RECORDED_EVENT_BYTES
    ) {
      droppedCount += 1;

      return;
    }
    retained.push({ record, bytes, priority });
    retainedBytes += bytes;
  };

  return {
    append,
    snapshot: () => ({
      events: retained.map(({ record }) => record),
      eventCapture: {
        retainedCount: retained.length,
        droppedCount,
        retainedBytes,
        limitBytes: MAX_RECORDED_EVENT_BYTES,
      },
    }),
  };
};

/**
 * Complete observation of one message turn. The kernel produces this value;
 * an optional host-owned sink decides whether and where to retain it.
 */
export interface SessionRunRecord {
  /** Host-independent identity assigned before setup begins. */
  readonly runId: string;
  /** Deterministic sink key suggested by the kernel. */
  readonly archiveKey: string;
  readonly sessionId: string;
  readonly startedAt: string;
  readonly request: {
    readonly colo: string | null;
    readonly rayId: string | null;
  };
  readonly prompt: string;
  readonly promptProfile: { readonly name: string; readonly version: number } | null;
  readonly lifecycle: RunLifecycle | null;
  readonly spans: ReadonlyArray<SpanRecord>;
  readonly outcome: "completed" | "failed";
  readonly failureReason: string | null;
  readonly timings: AgentTimings | null;
  readonly harness: HarnessName | null;
  readonly harnessVersion: string | null;
  readonly model: string | null;
  readonly reasoningEffort: ReasoningEffort | null;
  readonly fast: boolean | null;
  readonly containerSize: ContainerSize | null;
  readonly materialization: WorkspaceMaterialization | null;
  readonly head: string | null;
  readonly changedPaths: ReadonlyArray<string>;
  /** Harness-owned usage payload, retained without normalization. */
  readonly usage: unknown;
  readonly resumeToken: string | null;
  readonly events: ReadonlyArray<RecordedAgentEvent>;
  /** Present for bounded recorders. Optional for in-process hosts that build
   * legacy records directly. */
  readonly eventCapture?: RunEventCapture;
}

/** Measurements a retaining sink can report without making them recursive. */
export interface RunArchiveReceipt {
  readonly key: string;
  readonly token: string | null;
  readonly serializeMs: number;
  readonly putMs: number;
  readonly bytes: number;
}
