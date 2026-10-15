import { Schema } from "effect";
//#region src/RunRecord.ts
/** Archive retention bounds. Live transport has its own, smaller queue bound. */
const MAX_RECORDED_EVENT_BYTES = 4194304;
const MAX_RECORDED_EVENT_COUNT = 4e3;
const MAX_AGENT_EVENT_BYTES = 262144;
/** One observed server-side phase span, relative to request acceptance. */
const SpanRecord = Schema.Struct({
	name: Schema.String,
	startAt: Schema.Finite,
	endAt: Schema.NullOr(Schema.Finite),
	outcome: Schema.Literals([
		"ok",
		"failed",
		"unknown"
	]),
	attempts: Schema.optionalKey(Schema.Int)
});
const MAX_JSON_DEPTH = 64;
const jsonStringByteLengthWithin = (value, limit) => {
	let bytes = 2;
	if (bytes > limit) return null;
	for (let index = 0; index < value.length; index += 1) {
		const code = value.charCodeAt(index);
		let nextBytes;
		if (code === 34 || code === 92 || code === 8 || code === 9 || code === 10 || code === 12 || code === 13) nextBytes = 2;
		else if (code <= 31) nextBytes = 6;
		else if (code >= 55296 && code <= 56319) {
			const trailing = value.charCodeAt(index + 1);
			if (trailing >= 56320 && trailing <= 57343) {
				nextBytes = 4;
				index += 1;
			} else nextBytes = 6;
		} else if (code >= 56320 && code <= 57343) nextBytes = 6;
		else if (code <= 127) nextBytes = 1;
		else if (code <= 2047) nextBytes = 2;
		else nextBytes = 3;
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
const jsonByteLengthWithin = (value, limit) => {
	const ancestors = /* @__PURE__ */ new Set();
	let visited = 0;
	const measure = (current, remaining, depth) => {
		if (remaining < 0 || depth > MAX_JSON_DEPTH || visited > limit) return null;
		if (current === null) return remaining >= 4 ? 4 : null;
		switch (typeof current) {
			case "string": return jsonStringByteLengthWithin(current, remaining);
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
			case "symbol": return;
			case "bigint": return null;
		}
		if (ancestors.has(current)) return null;
		const prototype = Object.getPrototypeOf(current);
		if (Array.isArray(current) ? prototype !== Array.prototype : prototype !== Object.prototype && prototype !== null) return null;
		let currentPrototype = prototype;
		while (currentPrototype !== null) {
			const inheritedToJson = Object.getOwnPropertyDescriptor(currentPrototype, "toJSON");
			if (inheritedToJson?.get !== void 0 || inheritedToJson?.set !== void 0 || typeof inheritedToJson?.value === "function") return null;
			currentPrototype = Object.getPrototypeOf(currentPrototype);
		}
		const toJsonDescriptor = Object.getOwnPropertyDescriptor(current, "toJSON");
		if (toJsonDescriptor !== void 0 && (toJsonDescriptor.get !== void 0 || toJsonDescriptor.set !== void 0 || typeof toJsonDescriptor.value === "function")) return null;
		ancestors.add(current);
		visited += 1;
		let bytes = 1;
		if (Array.isArray(current)) for (let index = 0; index < current.length; index += 1) {
			if (index > 0) bytes += 1;
			if (bytes > remaining) return null;
			const descriptor = Object.getOwnPropertyDescriptor(current, String(index));
			if (descriptor?.get !== void 0 || descriptor?.set !== void 0) return null;
			const measured = measure(descriptor?.value, remaining - bytes, depth + 1);
			const itemBytes = measured === void 0 ? 4 : measured;
			if (itemBytes === null) return null;
			bytes += itemBytes;
		}
		else {
			let propertyCount = 0;
			for (const key in current) {
				if (!Object.hasOwn(current, key)) continue;
				propertyCount += 1;
				if (propertyCount > limit) return null;
				const descriptor = Object.getOwnPropertyDescriptor(current, key);
				if (descriptor === void 0 || descriptor.get !== void 0 || descriptor.set !== void 0) return null;
				const measured = measure(descriptor.value, remaining - bytes, depth + 1);
				if (measured === void 0) continue;
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
	return measured === void 0 ? null : measured;
};
const priorityRank = {
	ordinary: 0,
	essential: 1,
	terminal: 2
};
const eventPriority = (event) => {
	if (event.type === "agent.completed" || event.type === "agent.failed") return "terminal";
	if (event.type === "codex.event" && event.event.type !== "turn.completed" && event.event.type !== "turn.failed" && event.event.type !== "error") return "ordinary";
	return "essential";
};
/**
* Keeps archive memory bounded without changing the live protocol. Core
* lifecycle records and the harness terminal record win space over ordinary
* dialect records, so a truncated archive still explains how the run ended.
*/
const makeBoundedRunRecorder = () => {
	const retained = [];
	let retainedBytes = 0;
	let droppedCount = 0;
	const removeOldestLowerPriority = (priority) => {
		let index = -1;
		for (let rank = 0; rank < priorityRank[priority]; rank += 1) {
			index = retained.findIndex(({ priority: candidate }) => priorityRank[candidate] === rank);
			if (index >= 0) break;
		}
		if (index === -1) return false;
		const removed = retained[index];
		if (removed === void 0) return false;
		retained.splice(index, 1);
		retainedBytes -= removed.bytes;
		droppedCount += 1;
		return true;
	};
	const append = (record) => {
		const bytes = jsonByteLengthWithin(record, MAX_AGENT_EVENT_BYTES);
		const priority = eventPriority(record.event);
		if (bytes === null) {
			droppedCount += 1;
			return;
		}
		if (priority !== "ordinary") while ((retained.length >= 4e3 || retainedBytes + bytes > 4194304) && removeOldestLowerPriority(priority));
		if (retained.length >= 4e3 || retainedBytes + bytes > 4194304) {
			droppedCount += 1;
			return;
		}
		retained.push({
			record,
			bytes,
			priority
		});
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
				limitBytes: MAX_RECORDED_EVENT_BYTES
			}
		})
	};
};
//#endregion
export { MAX_AGENT_EVENT_BYTES, MAX_RECORDED_EVENT_BYTES, MAX_RECORDED_EVENT_COUNT, SpanRecord, jsonByteLengthWithin, makeBoundedRunRecorder };
