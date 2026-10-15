import { SandboxTaskId, TurnId } from "./ids.mjs";
import { Schema } from "effect";
//#region src/server/sandbox-contracts/sandbox-task-event.ts
const SandboxTaskEventKind = Schema.Literals(["runtime", "codex"]);
var SandboxTaskEvent = class extends Schema.Class("SandboxTaskEvent")({
	sequence: Schema.Natural,
	taskId: SandboxTaskId,
	turnId: Schema.NullOr(TurnId),
	kind: SandboxTaskEventKind,
	method: Schema.String,
	payload: Schema.Unknown,
	createdAt: Schema.String
}) {};
const SandboxTaskEventList = Schema.Array(SandboxTaskEvent);
//#endregion
export { SandboxTaskEvent, SandboxTaskEventKind, SandboxTaskEventList };
