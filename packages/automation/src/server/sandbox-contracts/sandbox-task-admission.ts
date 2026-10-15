import { Schema } from "effect";
import { GitObjectId } from "./git";
import {
  ArtifactId,
  CommandId,
  ConversationId,
  PersonaId,
  SandboxTaskId,
  TaskId,
  TurnId,
  WorkspaceId,
} from "./ids";

export const GithubRepositoryBranch = Schema.NonEmptyString.check(
  Schema.isMaxLength(255),
  // JSON Schema consumers cannot rely on JavaScript Unicode property escapes.
  // oxlint-disable-next-line no-control-regex -- Reject the full Unicode Cc range with portable syntax.
  Schema.isPattern(/^[^\u0000-\u001f\u007f-\u009f]+$/u),
  Schema.isPattern(
    /^(?!-)(?!.*(?:\.\.|@\{|\/\/|\.lock(?:\/|$)))(?!.*[ ~^:?*[\\])(?!.*\.$)(?!.*\/$)(?!\/).+$/u,
  ).annotate({
    // OpenAI rejects lookaround; the decoding codec still enforces the full Git rules.
    expected: "a valid Git branch name",
    toJsonSchema: () => ({}),
  }),
);
export const ArtifactsRepositoryRef = GithubRepositoryBranch.check(
  Schema.isPattern(/^refs\/heads\/.+$/u),
);

export const SandboxTaskAttachedRepository = Schema.TaggedStruct("AttachedRepository", {
  repositoryName: Schema.NonEmptyString,
  artifactsRepositoryId: Schema.NonEmptyString,
  commitSha: GitObjectId,
});
/** Read-only input. The kernel owns a separate repository for the task's output. */
export const SandboxTaskSeedRepository = Schema.TaggedStruct("SeedRepository", {
  repositoryName: Schema.NonEmptyString,
  artifactsRepositoryId: Schema.NonEmptyString,
  commitSha: GitObjectId,
  artifactsRef: Schema.optionalKey(ArtifactsRepositoryRef),
});
export const SandboxTaskLaunchAssociation = Schema.Union([
  Schema.TaggedStruct("Task", { taskId: TaskId }),
  Schema.TaggedStruct("Persona", {
    ownerConversationId: ConversationId,
    personaId: PersonaId,
    threadId: Schema.NonEmptyString,
    submissionId: Schema.NonEmptyString,
  }),
  Schema.TaggedStruct("Site", { siteId: ArtifactId }),
  Schema.TaggedStruct("Repair", {
    repositoryId: Schema.NonEmptyString,
    repairId: Schema.NonEmptyString,
    attemptId: Schema.NonEmptyString,
    evidenceHash: Schema.NonEmptyString,
    proofPolicyHash: Schema.NonEmptyString,
  }),
]);
export type SandboxTaskLaunchAssociation = typeof SandboxTaskLaunchAssociation.Type;

export class SandboxTaskLaunchReceipt extends Schema.TaggedClass<SandboxTaskLaunchReceipt>()(
  "SandboxTaskLaunchReceipt",
  {
    commandId: CommandId,
    requestFingerprint: Schema.NonEmptyString,
    result: Schema.Struct({
      id: SandboxTaskId,
      workspaceId: Schema.optionalKey(WorkspaceId),
      turnId: Schema.optionalKey(TurnId),
      outcome: Schema.Literals(["launched", "cancelled"]),
    }),
  },
) {}
