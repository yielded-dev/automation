import { CodexErrorCode, HarnessTerminalStatus, withFailureDiagnostic } from "../observability/FailureDiagnostic.mjs";
import { HarnessTurnError } from "./Harness.mjs";
import { markPoint } from "../tracing/RunSpanCapture.mjs";
import { SessionStateStore } from "../cloudflare/SessionStateStore.mjs";
import { boundCodexEvent } from "./CodexEventBounds.mjs";
import { Clock, Deferred, Effect, Match, Redacted, Schema, Stream } from "effect";
//#region src/harness/CodexAppServer.ts
const RpcMessage = Schema.Struct({
	id: Schema.optionalKey(Schema.Union([Schema.String, Schema.Int])),
	method: Schema.optionalKey(Schema.String),
	params: Schema.optionalKey(Schema.Unknown),
	result: Schema.optionalKey(Schema.Unknown),
	error: Schema.optionalKey(Schema.Struct({ message: Schema.String }))
});
const Item = Schema.StructWithRest(Schema.Struct({
	id: Schema.String,
	type: Schema.String,
	text: Schema.optionalKey(Schema.String),
	summary: Schema.optionalKey(Schema.Array(Schema.String)),
	aggregatedOutput: Schema.optionalKey(Schema.NullOr(Schema.String)),
	exitCode: Schema.optionalKey(Schema.NullOr(Schema.Int))
}), [Schema.Record(Schema.String, Schema.Unknown)]);
const ItemNotification = Schema.Struct({
	threadId: Schema.String,
	turnId: Schema.String,
	item: Item
});
const TextDelta = Schema.Struct({
	threadId: Schema.String,
	turnId: Schema.String,
	itemId: Schema.String,
	delta: Schema.String,
	summaryIndex: Schema.optionalKey(Schema.Natural)
});
const SummaryPart = Schema.Struct({
	threadId: Schema.String,
	turnId: Schema.String,
	itemId: Schema.String,
	summaryIndex: Schema.Natural
});
const UsageTotal = Schema.Struct({
	inputTokens: Schema.Natural,
	cachedInputTokens: Schema.Natural,
	outputTokens: Schema.Natural,
	reasoningOutputTokens: Schema.optionalKey(Schema.Natural),
	cacheWriteInputTokens: Schema.optionalKey(Schema.Natural)
});
const NativePlan = Schema.Array(Schema.Struct({
	step: Schema.String,
	status: Schema.String
}));
const NativeTurn = Schema.Struct({
	id: Schema.String,
	status: Schema.Literals([
		"inProgress",
		"completed",
		...HarnessTerminalStatus.literals
	]),
	items: Schema.Array(Item),
	error: Schema.NullOr(Schema.Struct({
		message: Schema.String,
		codexErrorInfo: Schema.optionalKey(Schema.Unknown)
	})),
	replay: Schema.optionalKey(Schema.Struct({
		tokenUsage: Schema.optionalKey(UsageTotal),
		plan: Schema.optionalKey(NativePlan)
	}))
});
const TurnNotification = Schema.Struct({
	threadId: Schema.String,
	turn: NativeTurn
});
const TokenUsage = Schema.Struct({
	threadId: Schema.String,
	turnId: Schema.String,
	tokenUsage: Schema.Struct({ total: UsageTotal })
});
const PlanNotification = Schema.Struct({
	threadId: Schema.String,
	turnId: Schema.String,
	plan: NativePlan
});
const ThreadResult = Schema.Struct({ thread: Schema.Struct({
	id: Schema.String,
	turns: Schema.Array(NativeTurn)
}) });
const SteeringResult = Schema.Struct({ turnId: Schema.String });
const codexErrorCode = (info) => Match.value(info).pipe(Match.when(Schema.is(CodexErrorCode), (code) => code), Match.when({ httpConnectionFailed: Match.any }, () => "httpConnectionFailed"), Match.when({ responseStreamConnectionFailed: Match.any }, () => "responseStreamConnectionFailed"), Match.when({ responseStreamDisconnected: Match.any }, () => "responseStreamDisconnected"), Match.when({ responseTooManyFailedAttempts: Match.any }, () => "responseTooManyFailedAttempts"), Match.when({ activeTurnNotSteerable: Match.any }, () => "activeTurnNotSteerable"), Match.orElse(() => void 0));
const decode = (schema, value) => Schema.decodeUnknownEffect(schema)(value).pipe(Effect.mapError(() => HarnessTurnError.make({
	reason: "malformedEvent",
	message: "Invalid Codex app-server message"
})));
const itemTypes = /* @__PURE__ */ new Map([
	["agentMessage", "agent_message"],
	["commandExecution", "command_execution"],
	["fileChange", "file_change"],
	["mcpToolCall", "mcp_tool_call"],
	["collabAgentToolCall", "collab_tool_call"],
	["webSearch", "web_search"],
	["plan", "agent_message"]
]);
/** Keep the persisted task dialect readable by existing clients and histories. */
const taskItem = (item, turnId) => {
	const id = `${turnId}:${item.id}`;
	if (item.type === "reasoning") return {
		id,
		type: "reasoning",
		text: (item.summary ?? []).join("\n")
	};
	const { aggregatedOutput, exitCode, ...rest } = item;
	return {
		...rest,
		id,
		type: itemTypes.get(item.type) ?? item.type,
		...item.type === "plan" ? { phase: "commentary" } : {},
		...aggregatedOutput === void 0 ? {} : { aggregated_output: aggregatedOutput ?? "" },
		...exitCode === void 0 ? {} : { exit_code: exitCode }
	};
};
const TEXT_UPDATE_INTERVAL_MILLIS = 100;
const MAX_ACTIVE_ITEMS = 64;
const MAX_STEERING_INPUTS = 64;
const encoder = new TextEncoder();
/** Drive one turn over an already-open, scoped app-server connection. */
const runCodexAppServerTurn = Effect.fn("CodexAppServer.runTurn")(function* (request, connection) {
	const store = yield* SessionStateStore;
	const stored = yield* store.codexTurn.get;
	if (stored === null || stored.id !== request.turnId) return yield* HarnessTurnError.make({
		reason: "turnFailed",
		message: "The native turn has no matching durable owner"
	});
	let checkpoint = stored;
	let threadId = checkpoint.threadId;
	let turnId = checkpoint.turnId;
	let lastAgentMessage = null;
	let usage = null;
	let terminalEventType = null;
	let firstEvent = true;
	let acceptingSteering = false;
	const started = yield* Deferred.make();
	const steeringReceipts = /* @__PURE__ */ new Map();
	const active = /* @__PURE__ */ new Map();
	const restoredItems = /* @__PURE__ */ new Map();
	let planItem;
	const steer = Effect.fn("CodexAppServer.steer")(function* (input) {
		const id = `steer:${input.id}`;
		const existing = steeringReceipts.get(id);
		if (existing !== void 0) return existing.text === input.text ? yield* Deferred.await(existing.result) : {
			_tag: "Rejected",
			reason: "The steering identity already has different input"
		};
		if (!acceptingSteering || threadId === null || turnId === null) return terminalEventType === "turn.completed" ? { _tag: "Inactive" } : {
			_tag: "Rejected",
			reason: "The native turn is no longer available for steering"
		};
		if (steeringReceipts.size >= MAX_STEERING_INPUTS || encoder.encode(input.text).byteLength > 196608) return {
			_tag: "Rejected",
			reason: "The native turn's steering input limit was reached"
		};
		const result = Deferred.makeUnsafe();
		steeringReceipts.set(id, {
			inputId: input.id,
			text: input.text,
			turnId,
			result
		});
		yield* Effect.annotateCurrentSpan({
			"codex.thread_id": threadId,
			"codex.turn_id": turnId,
			"sandbox.steering.input_id": input.id
		});
		yield* markPoint("codex-steering-sent");
		yield* connection.send({
			id,
			method: "turn/steer",
			params: {
				threadId,
				expectedTurnId: turnId,
				input: [{
					type: "text",
					text: input.text
				}]
			}
		}).pipe(Effect.catch((error) => Deferred.succeed(result, {
			_tag: "Unknown",
			reason: error.message
		})));
		return yield* Deferred.await(result).pipe(Effect.timeoutOrElse({
			duration: request.deadlinePolicy.harnessSetupMillis,
			orElse: () => {
				const outcome = {
					_tag: "Unknown",
					reason: "The native turn did not acknowledge steering before the deadline"
				};
				return Deferred.succeed(result, outcome).pipe(Effect.as(outcome));
			}
		}));
	});
	const emit = Effect.fn("CodexAppServer.emit")(function* (event) {
		const bounded = yield* boundCodexEvent(event);
		yield* request.onEvent({
			type: "codex.event",
			event: bounded
		});
	});
	const emitItem = (type, item, itemTurnId) => emit({
		type,
		item: taskItem(item, itemTurnId)
	});
	const observeUsage = (total) => Effect.sync(() => {
		usage = {
			input_tokens: total.inputTokens,
			cached_input_tokens: total.cachedInputTokens,
			output_tokens: total.outputTokens,
			reasoning_output_tokens: total.reasoningOutputTokens ?? 0,
			cache_write_input_tokens: total.cacheWriteInputTokens ?? 0
		};
	});
	const restoredPlan = (nativeTurnId, plan) => ({
		id: `${nativeTurnId}:plan`,
		type: "todo_list",
		items: plan.map((step) => ({
			text: step.step,
			completed: step.status === "completed"
		}))
	});
	const observeTurn = Effect.fn("CodexAppServer.observeTurn")(function* (turn, restoring) {
		if (turnId !== null && turn.id !== turnId) return false;
		turnId = turn.id;
		checkpoint = {
			...checkpoint,
			turnId
		};
		yield* store.codexTurn.put(checkpoint);
		yield* Deferred.succeed(started, void 0);
		acceptingSteering = turn.status === "inProgress";
		if (turn.replay?.tokenUsage !== void 0) yield* observeUsage(turn.replay.tokenUsage);
		if (turn.replay?.plan !== void 0) {
			planItem = restoredPlan(turn.id, turn.replay.plan);
			yield* emit({
				type: "item.updated",
				item: planItem
			});
		}
		if (acceptingSteering) {
			if (request.onSteeringReady !== void 0) yield* request.onSteeringReady(steer);
			yield* emit({ type: "turn.started" });
		}
		if (restoring || !acceptingSteering) for (const item of turn.items) {
			if (item.type === "userMessage" || item.type === "hookPrompt") continue;
			if (item.type === "agentMessage" || item.type === "plan") lastAgentMessage = item.text ?? null;
			const completed = !acceptingSteering || typeof item.status === "string" && item.status !== "inProgress";
			if (!completed) restoredItems.set(item.id, {
				item,
				emittedAt: void 0
			});
			else {
				active.delete(item.id);
				restoredItems.delete(item.id);
			}
			yield* emitItem(completed ? "item.completed" : "item.updated", item, turn.id);
		}
		if (turn.status === "inProgress") return false;
		const succeeded = turn.status === "completed";
		terminalEventType = succeeded ? "turn.completed" : "turn.failed";
		for (const { item } of new Map([...restoredItems, ...active]).values()) yield* emitItem("item.completed", item, turn.id);
		active.clear();
		restoredItems.clear();
		if (planItem !== void 0) yield* emit({
			type: "item.completed",
			item: planItem
		});
		const failureMessage = turn.error?.message ?? `Codex turn ${turn.status}`;
		yield* emit({
			type: terminalEventType,
			...succeeded ? { usage } : { error: { message: failureMessage } }
		});
		yield* markPoint("codex-terminal-event");
		if (turn.status !== "completed") {
			const code = codexErrorCode(turn.error?.codexErrorInfo);
			return yield* withFailureDiagnostic(Effect.fail(HarnessTurnError.make({
				reason: "turnFailed",
				message: failureMessage,
				terminalStatus: turn.status,
				...code === void 0 ? {} : { code }
			})), { operation: "CodexAppServer.observeTurn" });
		}
		return true;
	});
	const receive = Effect.fn("CodexAppServer.receive")(function* (text) {
		const message = yield* decode(Schema.fromJsonString(RpcMessage), text);
		if (firstEvent) {
			firstEvent = false;
			yield* markPoint("codex-first-json-event");
		}
		const steering = typeof message.id === "string" && message.method === void 0 ? steeringReceipts.get(message.id) : void 0;
		if (steering !== void 0) {
			const outcome = message.error !== void 0 ? message.error.message === "no active turn to steer" ? { _tag: "Inactive" } : {
				_tag: "Rejected",
				reason: message.error.message
			} : yield* decode(SteeringResult, message.result).pipe(Effect.match({
				onFailure: (error) => ({
					_tag: "Unknown",
					reason: error.message
				}),
				onSuccess: (result) => result.turnId === steering.turnId ? {
					_tag: "Accepted",
					turnId: result.turnId
				} : {
					_tag: "Unknown",
					reason: "The steering acknowledgement changed turns"
				}
			}));
			yield* Deferred.succeed(steering.result, outcome);
			yield* Effect.annotateCurrentSpan({
				"sandbox.steering.input_id": steering.inputId,
				"codex.turn_id": steering.turnId,
				"sandbox.steering.outcome": outcome._tag
			});
			if (outcome._tag === "Accepted") yield* markPoint("codex-steering-accepted");
			return false;
		}
		if (message.error !== void 0) return yield* HarnessTurnError.make({
			reason: message.id === 1 && checkpoint.resumeToken !== null ? "freshSessionRequired" : "turnFailed",
			message: message.error.message
		});
		if (message.id !== void 0 && message.method !== void 0) {
			yield* connection.send({
				id: message.id,
				error: {
					code: -32601,
					message: "Interactive requests are unavailable in SandboxTasks"
				}
			});
			return yield* HarnessTurnError.make({
				reason: "turnFailed",
				message: `Codex requested ${message.method}`
			});
		}
		if (message.id === 0) {
			yield* connection.send({
				method: "initialized",
				params: {}
			});
			yield* connection.send({
				id: "authenticate",
				method: "account/login/start",
				params: {
					type: "apiKey",
					apiKey: Redacted.value(connection.apiKey)
				}
			});
		} else if (message.id === "authenticate") yield* connection.send({
			id: 1,
			method: (threadId ?? checkpoint.resumeToken) === null ? "thread/start" : "thread/resume",
			params: {
				...(threadId ?? checkpoint.resumeToken) === null ? {} : { threadId: threadId ?? checkpoint.resumeToken },
				...checkpoint.threadParams
			}
		});
		else if (message.id === 1) {
			const result = yield* decode(ThreadResult, message.result);
			if (checkpoint.threadId !== null && result.thread.id !== checkpoint.threadId) return yield* HarnessTurnError.make({
				reason: "turnFailed",
				message: "Codex reattachment returned a different thread"
			});
			threadId = result.thread.id;
			yield* markPoint("codex-thread-started");
			yield* emit({
				type: "thread.started",
				thread_id: threadId
			});
			if (checkpoint.startDispatched) {
				const candidates = checkpoint.turnId === null ? result.thread.turns.filter((turn, index, turns) => checkpoint.previousTurnId === null || index > turns.findIndex((candidate) => candidate.id === checkpoint.previousTurnId)) : result.thread.turns.filter((turn) => turn.id === checkpoint.turnId);
				const candidate = candidates[0];
				if (candidates.length > 1) return yield* HarnessTurnError.make({
					reason: "turnFailed",
					message: "The dispatched Codex turn could not be identified; its input was not replayed"
				});
				if (candidate !== void 0) return yield* observeTurn(candidate, true);
			}
			checkpoint = {
				...checkpoint,
				threadId,
				previousTurnId: result.thread.turns.at(-1)?.id ?? null,
				startDispatched: true
			};
			yield* store.codexTurn.put(checkpoint);
			yield* connection.send({
				id: 2,
				method: "turn/start",
				params: {
					threadId,
					...checkpoint.turnParams
				}
			});
		} else if (message.method === "turn/started" || message.method === "turn/completed") {
			const notification = yield* decode(TurnNotification, message.params);
			if (notification.threadId !== threadId) return false;
			return yield* observeTurn(notification.turn, false);
		} else if (message.method === "item/started" || message.method === "item/completed") {
			const notification = yield* decode(ItemNotification, message.params);
			if (notification.threadId !== threadId || notification.turnId !== turnId) return false;
			const item = notification.item;
			if (item.type === "userMessage" || item.type === "hookPrompt") return false;
			const completed = message.method === "item/completed";
			if (completed) {
				active.delete(item.id);
				restoredItems.delete(item.id);
				if (item.type === "agentMessage" || item.type === "plan") lastAgentMessage = item.text ?? null;
			} else {
				if (active.size >= MAX_ACTIVE_ITEMS && !active.has(item.id)) return yield* HarnessTurnError.make({
					reason: "oversizedEvent",
					message: "Too many active Codex items"
				});
				active.set(item.id, {
					item,
					emittedAt: void 0
				});
			}
			yield* emitItem(completed ? "item.completed" : "item.started", item, notification.turnId);
		} else if (message.method === "item/reasoning/summaryPartAdded") {
			const part = yield* decode(SummaryPart, message.params);
			if (part.threadId !== threadId || part.turnId !== turnId) return false;
			const current = active.get(part.itemId) ?? restoredItems.get(part.itemId);
			if (current === void 0 || current.item.type !== "reasoning") return yield* HarnessTurnError.make({
				reason: "malformedEvent",
				message: "Codex summary section arrived without a reasoning item"
			});
			const summary = [...current.item.summary ?? []];
			if (part.summaryIndex > summary.length) return yield* HarnessTurnError.make({
				reason: "malformedEvent",
				message: "Codex reasoning summary skipped a section"
			});
			summary[part.summaryIndex] ??= "";
			(active.has(part.itemId) ? active : restoredItems).set(part.itemId, {
				...current,
				item: {
					...current.item,
					summary
				}
			});
		} else if (message.method === "item/agentMessage/delta" || message.method === "item/plan/delta" || message.method === "item/reasoning/summaryTextDelta") {
			const delta = yield* decode(TextDelta, message.params);
			if (delta.threadId !== threadId || delta.turnId !== turnId) return false;
			const current = active.get(delta.itemId) ?? restoredItems.get(delta.itemId);
			if (current === void 0) return yield* HarnessTurnError.make({
				reason: "malformedEvent",
				message: "Codex text delta arrived without an item"
			});
			let item;
			if (message.method === "item/reasoning/summaryTextDelta") {
				const summary = [...current.item.summary ?? []];
				const index = delta.summaryIndex ?? 0;
				if (index > summary.length) return yield* HarnessTurnError.make({
					reason: "malformedEvent",
					message: "Codex reasoning summary skipped a section"
				});
				summary[index] = `${summary[index] ?? ""}${delta.delta}`;
				item = {
					...current.item,
					summary
				};
			} else item = {
				...current.item,
				text: `${current.item.text ?? ""}${delta.delta}`
			};
			if ((item.type === "reasoning" ? (item.summary ?? []).reduce((length, part) => length + part.length, 0) : item.text?.length ?? 0) > 196608) return yield* HarnessTurnError.make({
				reason: "oversizedEvent",
				message: "Codex streaming text exceeded the item limit"
			});
			const now = yield* Clock.currentTimeMillis;
			const shouldEmit = current.emittedAt === void 0 || now - current.emittedAt >= TEXT_UPDATE_INTERVAL_MILLIS;
			(active.has(item.id) ? active : restoredItems).set(item.id, {
				item,
				emittedAt: shouldEmit ? now : current.emittedAt
			});
			if (shouldEmit) yield* emitItem("item.updated", item, delta.turnId);
		} else if (message.method === "thread/tokenUsage/updated") {
			const notification = yield* decode(TokenUsage, message.params);
			if (notification.threadId !== threadId || notification.turnId !== turnId) return false;
			const total = notification.tokenUsage.total;
			yield* observeUsage(total);
		} else if (message.method === "turn/plan/updated") {
			const notification = yield* decode(PlanNotification, message.params);
			if (notification.threadId !== threadId || notification.turnId !== turnId) return false;
			const type = planItem === void 0 ? "item.started" : "item.updated";
			planItem = restoredPlan(notification.turnId, notification.plan);
			yield* emit({
				type,
				item: planItem
			});
		}
		return false;
	});
	yield* connection.send({
		id: 0,
		method: "initialize",
		params: { clientInfo: {
			name: "repairs",
			version: "0.0.0"
		} }
	});
	yield* connection.messages.pipe(Stream.timeout(request.deadlinePolicy.harnessSetupMillis), Stream.mapEffect(receive), Stream.takeUntil((terminal) => terminal), Stream.runDrain, Effect.raceFirst(Deferred.await(started).pipe(Effect.timeoutOrElse({
		duration: request.deadlinePolicy.harnessSetupMillis,
		orElse: () => HarnessTurnError.make({
			reason: checkpoint.startDispatched ? "connectionLost" : "deadlineExceeded",
			message: "Codex app-server did not attach before the connection deadline"
		})
	}), Effect.andThen(Effect.never))), Effect.ensuring(Effect.sync(() => {
		acceptingSteering = false;
		for (const receipt of steeringReceipts.values()) Deferred.doneUnsafe(receipt.result, Effect.succeed({
			_tag: "Unknown",
			reason: "The native connection ended before steering was acknowledged"
		}));
	})));
	if (terminalEventType === null) return yield* HarnessTurnError.make({
		reason: "missingTerminalEvent",
		message: "Codex connection closed before the turn finished"
	});
	if (threadId === null) return yield* HarnessTurnError.make({
		reason: "missingResumeToken",
		message: "Codex did not report a thread id"
	});
	return {
		resumeToken: threadId,
		terminalEventType,
		lastAgentMessage,
		usage,
		stderrTail: ""
	};
});
//#endregion
export { runCodexAppServerTurn };
