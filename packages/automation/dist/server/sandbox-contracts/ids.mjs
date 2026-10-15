import { Schema } from "effect";
//#region src/server/sandbox-contracts/ids.ts
const uuid = Schema.String.check(Schema.isUUID());
const CommandId = uuid.pipe(Schema.brand("@kommunikasie/CommandId")).annotate({ identifier: "CommandId" });
const SandboxTaskId = Schema.String.pipe(Schema.brand("kommunikasie/SandboxTaskId"));
const TurnId = Schema.String.pipe(Schema.brand("kommunikasie/TurnId"));
const WorkspaceId = uuid.pipe(Schema.brand("@kommunikasie/WorkspaceId"));
const TaskId = uuid.pipe(Schema.brand("@kommunikasie/TaskId"));
const ConversationId = uuid.pipe(Schema.brand("@kommunikasie/ConversationId"));
const PersonaId = uuid.pipe(Schema.brand("@kommunikasie/PersonaId"));
const ArtifactId = uuid.pipe(Schema.brand("@kommunikasie/ArtifactId")).annotate({ identifier: "ArtifactId" });
//#endregion
export { ArtifactId, CommandId, ConversationId, PersonaId, SandboxTaskId, TaskId, TurnId, WorkspaceId };
