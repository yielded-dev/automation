import { Schema } from "effect";

const uuid = Schema.String.check(Schema.isUUID());

export const CommandId = uuid
  .pipe(Schema.brand("@kommunikasie/CommandId"))
  .annotate({ identifier: "CommandId" });
export type CommandId = typeof CommandId.Type;

export const SandboxTaskId = Schema.String.pipe(Schema.brand("kommunikasie/SandboxTaskId"));
export type SandboxTaskId = typeof SandboxTaskId.Type;

export const TurnId = Schema.String.pipe(Schema.brand("kommunikasie/TurnId"));
export type TurnId = typeof TurnId.Type;

export const WorkspaceId = uuid.pipe(Schema.brand("@kommunikasie/WorkspaceId"));
export type WorkspaceId = typeof WorkspaceId.Type;

export const TaskId = uuid.pipe(Schema.brand("@kommunikasie/TaskId"));
export type TaskId = typeof TaskId.Type;

export const ConversationId = uuid.pipe(Schema.brand("@kommunikasie/ConversationId"));
export type ConversationId = typeof ConversationId.Type;

export const PersonaId = uuid.pipe(Schema.brand("@kommunikasie/PersonaId"));
export type PersonaId = typeof PersonaId.Type;

export const ArtifactId = uuid
  .pipe(Schema.brand("@kommunikasie/ArtifactId"))
  .annotate({ identifier: "ArtifactId" });
export type ArtifactId = typeof ArtifactId.Type;
