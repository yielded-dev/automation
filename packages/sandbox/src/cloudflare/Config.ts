import { Context, Effect, Layer, type Redacted, Schema } from "effect";

const DeadlineMillis = Schema.Int.check(Schema.isGreaterThan(0));

const SandboxAgentDeadlineOverrides = Schema.Struct({
  artifactsOperationMillis: Schema.optionalKey(DeadlineMillis),
  forkReadyMillis: Schema.optionalKey(DeadlineMillis),
  gitCommandMillis: Schema.optionalKey(DeadlineMillis),
  harnessSetupMillis: Schema.optionalKey(DeadlineMillis),
  harnessTurnMillis: Schema.optionalKey(DeadlineMillis),
});

const SandboxAgentDeadlinePolicySchema = Schema.Struct({
  artifactsOperationMillis: DeadlineMillis,
  forkReadyMillis: DeadlineMillis,
  gitCommandMillis: DeadlineMillis,
  harnessSetupMillis: DeadlineMillis,
  harnessTurnMillis: DeadlineMillis,
});

export const SandboxMcpConfiguration = Schema.Struct({
  url: Schema.String.check(Schema.isPattern(/^https:\/\/[^\s]+$/u)),
  enabledTools: Schema.Array(Schema.NonEmptyString),
});
export type SandboxMcpConfiguration = typeof SandboxMcpConfiguration.Type;

const RepositorySetupPath = Schema.NonEmptyString.check(
  Schema.isMaxLength(512),
  Schema.makeFilter(
    (path) =>
      !path.startsWith("/") &&
      path.split("/").every((part) => part !== ".." && part !== "." && part.length > 0),
    { expected: "a repository-relative setup recipe path" },
  ),
);

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
  repositorySetupSecrets: Schema.optionalKey(
    Schema.Record(Schema.String, Schema.Redacted(Schema.Record(Schema.String, Schema.String))),
  ),
});

/** Bounded external-operation policy applied by the reusable kernel. */
export interface SandboxAgentDeadlinePolicy {
  /** One Artifacts binding operation, including repository resolution. */
  readonly artifactsOperationMillis: number;
  /** Total time allowed for an asynchronous Artifacts fork to become readable. */
  readonly forkReadyMillis: number;
  /** One Git subprocess, including output collection. */
  readonly gitCommandMillis: number;
  /** One harness setup subprocess such as `codex --version`. */
  readonly harnessSetupMillis: number;
  /** One reasoning-harness turn subprocess. */
  readonly harnessTurnMillis: number;
}

export const defaultSandboxAgentDeadlinePolicy: SandboxAgentDeadlinePolicy = {
  artifactsOperationMillis: 15_000,
  forkReadyMillis: 300_000,
  gitCommandMillis: 120_000,
  harnessSetupMillis: 30_000,
  harnessTurnMillis: 60 * 60_000,
};

/** Host policy required by the kernel after environment decoding. */
export interface SandboxAgentConfigShape {
  /** Committed setup recipe selected by the host. */
  readonly repositorySetupPath: string;
  readonly commitCoauthorTrailer?: string;
  readonly sentryMcp?: SandboxMcpConfiguration;
  /** Host mapping for provider-specific source clone workarounds. */
  readonly seedCloneRepositoryName?: (repositoryName: string) => string;
  /** Default Codex model; a message may override it. */
  readonly defaultModel: string;
  /** Optional fixture-host default. Product launches select their source per request. */
  readonly seedRepository?: string;
  /** Namespace segment of the Artifacts Git remote URL. */
  readonly artifactsNamespace: string;
  /** Artifacts Git authority, for example `<account>.artifacts.cloudflare.net`. */
  readonly artifactsHostname: string;
  /** Host isolation scope when several environments share container capacity. */
  readonly sandboxNamespace?: string;
  /** Optional host tuning; omitted fields retain kernel defaults. */
  readonly deadlines?: Partial<SandboxAgentDeadlinePolicy>;
  readonly repositorySetupSecrets?: Readonly<
    Record<string, Redacted.Redacted<Readonly<Record<string, string>>>>
  >;
}

interface ResolvedSandboxAgentConfigShape extends Omit<
  SandboxAgentConfigShape,
  "deadlines" | "seedCloneRepositoryName"
> {
  readonly seedCloneRepositoryName: (repositoryName: string) => string;
  readonly deadlines: SandboxAgentDeadlinePolicy;
}

/** Resolves one complete policy so consumers cannot apply divergent defaults. */
export const sandboxAgentDeadlinePolicy = (
  config: ResolvedSandboxAgentConfigShape,
): SandboxAgentDeadlinePolicy => config.deadlines;

/** Runtime policy supplied by the host; the package reads no environment. */
export class SandboxAgentConfig extends Context.Service<
  SandboxAgentConfig,
  ResolvedSandboxAgentConfigShape
>()("sandbox-agent/Config") {
  /** Validates host policy and resolves all deadline defaults. */
  static readonly make = Effect.fn("SandboxAgentConfig.make")(function* (
    input: SandboxAgentConfigShape,
  ) {
    const config = yield* Schema.decodeEffect(SandboxAgentConfigInput)(input);
    const deadlines = yield* Schema.decodeEffect(SandboxAgentDeadlinePolicySchema)({
      ...defaultSandboxAgentDeadlinePolicy,
      ...config.deadlines,
    });

    return SandboxAgentConfig.of({
      repositorySetupPath: config.repositorySetupPath,
      ...(config.commitCoauthorTrailer === undefined
        ? {}
        : { commitCoauthorTrailer: config.commitCoauthorTrailer }),
      ...(config.sentryMcp === undefined ? {} : { sentryMcp: config.sentryMcp }),
      seedCloneRepositoryName: input.seedCloneRepositoryName ?? ((name) => name),
      defaultModel: config.defaultModel,
      ...(config.seedRepository === undefined ? {} : { seedRepository: config.seedRepository }),
      artifactsNamespace: config.artifactsNamespace,
      artifactsHostname: config.artifactsHostname,
      ...(config.sandboxNamespace === undefined
        ? {}
        : { sandboxNamespace: config.sandboxNamespace }),
      deadlines,
      repositorySetupSecrets: config.repositorySetupSecrets ?? {},
    });
  });

  /** Provides validated host configuration to the kernel. */
  static readonly layer = (config: SandboxAgentConfigShape) =>
    Layer.effect(SandboxAgentConfig)(SandboxAgentConfig.make(config));
}

/** A product launch must carry its accepted source; fixture hosts may configure a default. */
export class RepositorySourceRequiredError extends Schema.TaggedError<RepositorySourceRequiredError>()(
  "RepositorySourceRequiredError",
  {},
) {}
export const requireRepositorySeed = (seed: string | undefined) =>
  seed === undefined ? Effect.fail(RepositorySourceRequiredError.make({})) : Effect.succeed(seed);
