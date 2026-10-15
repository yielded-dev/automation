import { Schema } from "effect";

import { TurnId, SandboxTaskId } from "./ids";

export const SandboxTaskEventKind = Schema.Literals(["runtime", "codex"]);
export type SandboxTaskEventKind = typeof SandboxTaskEventKind.Type;

export class SandboxTaskEvent extends Schema.Class<SandboxTaskEvent>("SandboxTaskEvent")({
  sequence: Schema.Natural,
  taskId: SandboxTaskId,
  turnId: Schema.NullOr(TurnId),
  kind: SandboxTaskEventKind,
  method: Schema.String,
  payload: Schema.Unknown,
  createdAt: Schema.String,
}) {}

export const SandboxTaskEventList = Schema.Array(SandboxTaskEvent);
