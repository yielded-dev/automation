import { Schema } from "effect";
//#region src/server/sandbox-contracts/sandbox-task-native.ts
const Text = Schema.String;
const Identity = Schema.Struct({
	path: Text,
	id: Text
});
const Start = Schema.Struct({
	expectedInstanceId: Schema.optionalKey(Text),
	size: Schema.optionalKey(Schema.Literals(["standard", "big"])),
	recovery: Schema.optionalKey(Schema.Literals(["freshAllowed", "exactWorkspaceRequired"]))
});
const NativeProcessResource = Schema.Struct({
	key: Schema.String.check(Schema.isPattern(/^[a-z][a-z0-9-]{0,63}$/u)),
	generation: Schema.Natural,
	materializationId: Schema.NonEmptyString
});
const NativeFileType = Schema.Literals([
	"file",
	"directory",
	"symlink",
	"blockDevice",
	"characterDevice",
	"fifo",
	"socket"
]);
const NativeFileStat = Schema.Struct({
	type: NativeFileType,
	size: Schema.String,
	mode: Schema.Int,
	uid: Schema.Int,
	gid: Schema.Int,
	accessedAt: Schema.Finite,
	modifiedAt: Schema.Finite,
	changedAt: Schema.Finite
});
const NativeDirectoryEntries = Schema.Array(Schema.Struct({
	name: Schema.String,
	type: NativeFileType
}));
const Command = Schema.NonEmptyArray(Text);
const SandboxTaskNativeAction = Schema.Union([
	Schema.TaggedStruct("Describe", {}),
	Schema.TaggedStruct("JournalGet", { key: Schema.NonEmptyString.check(Schema.isMaxLength(100)) }),
	Schema.TaggedStruct("JournalPut", {
		key: Schema.NonEmptyString.check(Schema.isMaxLength(100)),
		value: Schema.Json,
		expectedValue: Schema.optionalKey(Schema.Json)
	}),
	Schema.TaggedStruct("PrepareRepository", {
		head: Text,
		scope: Text,
		resetReceipt: Schema.Boolean
	}),
	Schema.TaggedStruct("EnsureCommit", { head: Text }),
	Schema.TaggedStruct("AllocateProcess", { resource: Schema.optionalKey(NativeProcessResource) }),
	Schema.TaggedStruct("Lstat", { path: Text }),
	Schema.TaggedStruct("ReadDirectory", { path: Text }),
	Schema.TaggedStruct("Lease", {
		id: Text,
		active: Schema.Boolean,
		renewal: Schema.Boolean
	}),
	Schema.TaggedStruct("Park", {}),
	Schema.TaggedStruct("Ensure", { start: Start }),
	Schema.TaggedStruct("Exec", {
		argv: Command,
		options: Schema.Struct({
			processId: Schema.optionalKey(Text),
			nativeListener: Schema.optionalKey(Schema.Literal(true)),
			resource: Schema.optionalKey(NativeProcessResource),
			cwd: Schema.optionalKey(Text),
			env: Schema.optionalKey(Schema.Record(Text, Text)),
			deadlineAt: Schema.Finite,
			workspaceIdentity: Schema.optionalKey(Identity)
		})
	}),
	Schema.TaggedStruct("WaitPort", {
		processId: Text,
		port: Schema.Int,
		timeout: Schema.Natural
	}),
	Schema.TaggedStruct("WaitHttp", {
		processId: Text,
		port: Schema.Int,
		timeout: Schema.Natural,
		path: Schema.optionalKey(Text),
		status: Schema.optionalKey(Schema.Struct({
			min: Schema.Int,
			max: Schema.Int
		}))
	}),
	Schema.TaggedStruct("Process", {
		processId: Text,
		maxBytes: Schema.Natural
	}),
	Schema.TaggedStruct("WaitProcess", {
		processId: Text,
		maxBytes: Schema.Natural,
		waitMillis: Schema.Natural
	}),
	Schema.TaggedStruct("AcknowledgeProcess", { processId: Text }),
	Schema.TaggedStruct("SignalProcess", {
		processId: Text,
		signal: Schema.Int
	}),
	Schema.TaggedStruct("Processes", {}),
	Schema.TaggedStruct("ReadFileString", { path: Text }),
	Schema.TaggedStruct("Exists", { path: Text }),
	Schema.TaggedStruct("WriteFile", {
		path: Text,
		content: Text
	}),
	Schema.TaggedStruct("Mkdir", {
		path: Text,
		recursive: Schema.Boolean
	}),
	Schema.TaggedStruct("Remove", {
		path: Text,
		recursive: Schema.Boolean,
		force: Schema.Boolean
	}),
	Schema.TaggedStruct("RepositoryAuthorization", {
		remote: Text,
		credential: Text,
		expiresAt: Schema.Finite,
		grantId: Text
	}),
	Schema.TaggedStruct("ClearRepositoryAuthorization", {
		remote: Text,
		instanceId: Text,
		grantId: Text
	}),
	Schema.TaggedStruct("Destroy", {}),
	Schema.TaggedStruct("PrepareFile", {
		path: Text,
		access: Schema.Literals(["read", "write"]),
		nativeToken: Text,
		expectedInstanceId: Text
	}),
	Schema.TaggedStruct("PrepareForward", {
		port: Schema.Literal(4502),
		nativeOperationId: Text,
		nativeToken: Text,
		expectedInstanceId: Text,
		materializationId: Text
	})
]);
const SandboxTaskNativeRequest = Schema.Struct({
	operationId: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
	expectedInstanceId: Schema.optionalKey(Schema.NonEmptyString),
	purpose: Schema.Literals([
		"turn",
		"validation",
		"export",
		"source"
	]),
	action: SandboxTaskNativeAction
});
/** Inspection exposes results and repository pointers, never private recovery capabilities. */
const SandboxTaskExecutionSnapshot = Schema.Struct({
	repositoryName: Schema.NullOr(Schema.String),
	verifiedHead: Schema.NullOr(Schema.String),
	materializationId: Schema.NullOr(Schema.String),
	containerSize: Schema.NullOr(Schema.Literals(["standard", "big"])),
	pipeline: Schema.NullOr(Schema.Struct({
		id: Schema.String,
		instanceId: Schema.optionalKey(Schema.String),
		materializationId: Schema.String,
		head: Schema.NullOr(Schema.String),
		baseHead: Schema.NullOr(Schema.String),
		persisted: Schema.NullOr(Schema.Struct({
			head: Schema.NullOr(Schema.String),
			changedPaths: Schema.Array(Schema.String)
		}))
	})),
	codexTurn: Schema.NullOr(Schema.Struct({
		id: Schema.String,
		result: Schema.NullOr(Schema.Struct({
			terminalEventType: Schema.String,
			lastAgentMessage: Schema.NullOr(Schema.String)
		}))
	}))
});
const NativeContainerIdentity = Schema.Struct({
	instanceId: Schema.String,
	imageDigest: Schema.String,
	restoredSnapshotId: Schema.NullOr(Schema.String),
	timings: Schema.Struct({
		nativeStartRequestedAt: Schema.Finite,
		nativeStartReturnedAt: Schema.NullOr(Schema.Finite),
		ownerReadyAt: Schema.NullOr(Schema.Finite)
	})
});
//#endregion
export { NativeContainerIdentity, NativeDirectoryEntries, NativeFileStat, NativeFileType, NativeProcessResource, SandboxTaskExecutionSnapshot, SandboxTaskNativeAction, SandboxTaskNativeRequest };
