import { GitObjectId } from "./git.mjs";
import { ArtifactId, CommandId, ConversationId, PersonaId, SandboxTaskId, TaskId, TurnId, WorkspaceId } from "./ids.mjs";
import { Schema } from "effect";
//#region src/server/sandbox-contracts/sandbox-task-admission.ts
const GithubRepositoryBranch = Schema.NonEmptyString.check(Schema.isMaxLength(255), Schema.isPattern(/^[^\u0000-\u001f\u007f-\u009f]+$/u), Schema.isPattern(/^(?!-)(?!.*(?:\.\.|@\{|\/\/|\.lock(?:\/|$)))(?!.*[ ~^:?*[\\])(?!.*\.$)(?!.*\/$)(?!\/).+$/u).annotate({
	expected: "a valid Git branch name",
	toJsonSchema: () => ({})
}));
const ArtifactsRepositoryRef = GithubRepositoryBranch.check(Schema.isPattern(/^refs\/heads\/.+$/u));
const SandboxTaskAttachedRepository = Schema.TaggedStruct("AttachedRepository", {
	repositoryName: Schema.NonEmptyString,
	artifactsRepositoryId: Schema.NonEmptyString,
	commitSha: GitObjectId
});
/** Read-only input. The kernel owns a separate repository for the task's output. */
const SandboxTaskSeedRepository = Schema.TaggedStruct("SeedRepository", {
	repositoryName: Schema.NonEmptyString,
	artifactsRepositoryId: Schema.NonEmptyString,
	commitSha: GitObjectId,
	artifactsRef: Schema.optionalKey(ArtifactsRepositoryRef)
});
const SandboxTaskLaunchAssociation = Schema.Union([
	Schema.TaggedStruct("Task", { taskId: TaskId }),
	Schema.TaggedStruct("Persona", {
		ownerConversationId: ConversationId,
		personaId: PersonaId,
		threadId: Schema.NonEmptyString,
		submissionId: Schema.NonEmptyString
	}),
	Schema.TaggedStruct("Site", { siteId: ArtifactId }),
	Schema.TaggedStruct("Repair", {
		repositoryId: Schema.NonEmptyString,
		repairId: Schema.NonEmptyString,
		attemptId: Schema.NonEmptyString,
		evidenceHash: Schema.NonEmptyString,
		proofPolicyHash: Schema.NonEmptyString
	})
]);
var SandboxTaskLaunchReceipt = class extends Schema.TaggedClass()("SandboxTaskLaunchReceipt", {
	commandId: CommandId,
	requestFingerprint: Schema.NonEmptyString,
	result: Schema.Struct({
		id: SandboxTaskId,
		workspaceId: Schema.optionalKey(WorkspaceId),
		turnId: Schema.optionalKey(TurnId),
		outcome: Schema.Literals(["launched", "cancelled"])
	})
}) {};
//#endregion
export { ArtifactsRepositoryRef, GithubRepositoryBranch, SandboxTaskAttachedRepository, SandboxTaskLaunchAssociation, SandboxTaskLaunchReceipt, SandboxTaskSeedRepository };
