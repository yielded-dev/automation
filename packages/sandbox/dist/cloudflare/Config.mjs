import { Context, Effect, Layer, Schema } from "effect";
//#region src/cloudflare/Config.ts
const DeadlineMillis = Schema.Int.check(Schema.isGreaterThan(0));
const SandboxAgentDeadlineOverrides = Schema.Struct({
	artifactsOperationMillis: Schema.optionalKey(DeadlineMillis),
	forkReadyMillis: Schema.optionalKey(DeadlineMillis),
	gitCommandMillis: Schema.optionalKey(DeadlineMillis),
	harnessSetupMillis: Schema.optionalKey(DeadlineMillis),
	harnessTurnMillis: Schema.optionalKey(DeadlineMillis)
});
const SandboxAgentDeadlinePolicySchema = Schema.Struct({
	artifactsOperationMillis: DeadlineMillis,
	forkReadyMillis: DeadlineMillis,
	gitCommandMillis: DeadlineMillis,
	harnessSetupMillis: DeadlineMillis,
	harnessTurnMillis: DeadlineMillis
});
const SandboxMcpConfiguration = Schema.Struct({
	url: Schema.String.check(Schema.isPattern(/^https:\/\/[^\s]+$/u)),
	enabledTools: Schema.Array(Schema.NonEmptyString)
});
const RepositorySetupPath = Schema.NonEmptyString.check(Schema.isMaxLength(512), Schema.makeFilter((path) => !path.startsWith("/") && path.split("/").every((part) => part !== ".." && part !== "." && part.length > 0), { expected: "a repository-relative setup recipe path" }));
const SandboxAgentConfigInput = Schema.Struct({
	repositorySetupPath: RepositorySetupPath,
	commitCoauthorTrailer: Schema.optionalKey(Schema.NonEmptyString),
	sentryMcp: Schema.optionalKey(SandboxMcpConfiguration),
	defaultModel: Schema.String,
	seedRepository: Schema.optionalKey(Schema.NonEmptyString),
	artifactsNamespace: Schema.String,
	artifactsHostname: Schema.String,
	sandboxNamespace: Schema.optionalKey(Schema.NonEmptyString),
	deadlines: Schema.optionalKey(SandboxAgentDeadlineOverrides),
	repositorySetupSecrets: Schema.optionalKey(Schema.Record(Schema.String, Schema.Redacted(Schema.Record(Schema.String, Schema.String))))
});
const defaultSandboxAgentDeadlinePolicy = {
	artifactsOperationMillis: 15e3,
	forkReadyMillis: 3e5,
	gitCommandMillis: 12e4,
	harnessSetupMillis: 3e4,
	harnessTurnMillis: 36e5
};
/** Resolves one complete policy so consumers cannot apply divergent defaults. */
const sandboxAgentDeadlinePolicy = (config) => config.deadlines;
/** Runtime policy supplied by the host; the package reads no environment. */
var SandboxAgentConfig = class SandboxAgentConfig extends Context.Service()("sandbox-agent/Config") {
	/** Validates host policy and resolves all deadline defaults. */
	static make = Effect.fn("SandboxAgentConfig.make")(function* (input) {
		const config = yield* Schema.decodeEffect(SandboxAgentConfigInput)(input);
		const deadlines = yield* Schema.decodeEffect(SandboxAgentDeadlinePolicySchema)({
			...defaultSandboxAgentDeadlinePolicy,
			...config.deadlines
		});
		return SandboxAgentConfig.of({
			repositorySetupPath: config.repositorySetupPath,
			...config.commitCoauthorTrailer === void 0 ? {} : { commitCoauthorTrailer: config.commitCoauthorTrailer },
			...config.sentryMcp === void 0 ? {} : { sentryMcp: config.sentryMcp },
			seedCloneRepositoryName: input.seedCloneRepositoryName ?? ((name) => name),
			defaultModel: config.defaultModel,
			...config.seedRepository === void 0 ? {} : { seedRepository: config.seedRepository },
			artifactsNamespace: config.artifactsNamespace,
			artifactsHostname: config.artifactsHostname,
			...config.sandboxNamespace === void 0 ? {} : { sandboxNamespace: config.sandboxNamespace },
			deadlines,
			repositorySetupSecrets: config.repositorySetupSecrets ?? {}
		});
	});
	/** Provides validated host configuration to the kernel. */
	static layer = (config) => Layer.effect(SandboxAgentConfig)(SandboxAgentConfig.make(config));
};
/** A product launch must carry its accepted source; fixture hosts may configure a default. */
var RepositorySourceRequiredError = class extends Schema.TaggedError()("RepositorySourceRequiredError", {}) {};
const requireRepositorySeed = (seed) => seed === void 0 ? Effect.fail(RepositorySourceRequiredError.make({})) : Effect.succeed(seed);
//#endregion
export { RepositorySourceRequiredError, SandboxAgentConfig, SandboxMcpConfiguration, defaultSandboxAgentDeadlinePolicy, requireRepositorySeed, sandboxAgentDeadlinePolicy };
