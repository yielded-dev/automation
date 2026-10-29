import * as SecureRandom from "effect/crypto/SecureRandom";
import { Cause, Clock, Context, Effect, Layer, Redacted, Schedule, Schema } from "effect";
import type { Artifacts } from "effect-cf";
import { FetchHttpClient } from "effect/http";

import type { RepositorySetupError } from "../model/RepositorySetup";
import { reportSanitizedCause } from "../observability/RuntimeDiagnostics";
import { sanitizeCause, withFailureDiagnostic } from "../observability/FailureDiagnostic";

import { agentPhases } from "../Contract";
import { HarnessTurnError } from "../harness/Harness";
import { toJson } from "../Http";
import { transferRepositoryGit } from "../repository/RepositoryGitTransfer";
import {
  RepositoryMutationFence,
  RepositoryMutationFenceFailure,
} from "../repository/RepositoryMutationFence";
import { SessionSandbox } from "../SessionSandbox";
import { annotateSpanAttempts, inMeasure, inPhase, markPoint } from "../tracing/RunSpanCapture";
import { REPO_DIR, WORKSPACE_DIR } from "../workspace-paths";
import {
  artifactsForkPolicyFromDeadlines,
  artifactsMainHead,
  type ArtifactsForkDisposition,
  type ArtifactsForkPolicy,
  ArtifactsOperationTimeoutError,
  awaitArtifactsForkReady,
  ForkNotReadyError,
  isForkProvisioning,
  requestArtifactsFork,
} from "./ArtifactsFork";
import { SandboxAgentArtifacts } from "./Bindings";
import {
  requireRepositorySeed,
  type RepositorySourceRequiredError,
  SandboxAgentConfig,
  sandboxAgentDeadlinePolicy,
} from "./Config";
import type * as Sandbox from "./NativeSandbox";
import {
  RepositoryPersistenceCheckpoint,
  type RepositoryPersistenceCheckpointError,
} from "./RepositoryPersistenceCheckpoint";
import { assertRepositorySetupSecretsNotPersisted } from "./RepositorySetup";
import { resourceHash } from "./resource-names";
import { runSandboxCommand } from "./SandboxCommand";
import {
  SessionStateStore,
  type SessionStateStoreError,
  type StoredRepositoryFork,
} from "./SessionStateStore";
import {
  acknowledgeWorkspacePreparation,
  runWorkspacePreparation,
  type WorkspacePreparationRequest,
} from "./WorkspacePreparation";

/**
 * Durable source state through Cloudflare Artifacts: fork the immutable seed,
 * materialize a disposable sandbox workspace, persist committed work
 * back, and verify every push through the binding before success is claimed.
 */

const MINIMUM_TOKEN_TTL_SECONDS = 300;
const AGENT_COMMIT_AUTHOR = "Sandbox Agent";
const AGENT_COMMIT_EMAIL = "sandbox-agent@localhost";
const GIT_OUTPUT_LIMIT_BYTES = 512 * 1_024;
const CHANGED_PATHS_LIMIT_BYTES = 128 * 1_024;
const GIT_READ_RETRY_COUNT = 2;
const GIT_TRANSPORT_DIR = `${WORKSPACE_DIR}/.kommunikasie-git-transport`;
const GIT_TRANSPORT_REPOSITORY = `${GIT_TRANSPORT_DIR}/repository.git`;
const isolatedGitTransportEnv = {
  GIT_CEILING_DIRECTORIES: GIT_TRANSPORT_DIR,
  GIT_CONFIG_GLOBAL: "/dev/null",
  GIT_CONFIG_NOSYSTEM: "1",
  GIT_CONFIG_SYSTEM: "/dev/null",
} satisfies Record<string, string>;

/** A sandboxed Git command exited unsuccessfully; arguments are never retained. */
export class GitCommandError extends Schema.TaggedError<GitCommandError>()("GitCommandError", {
  command: Schema.String,
  exitCode: Schema.Int,
  stderr: Schema.String,
  status: Schema.optionalKey(Schema.Int.check(Schema.isBetween({ minimum: 400, maximum: 599 }))),
}) {}

/** Persisting every changed path would exceed the lifecycle event's safe bound. */
export class PersistedPathsLimitError extends Schema.TaggedError<PersistedPathsLimitError>()(
  "PersistedPathsLimitError",
  {
    pathCount: Schema.Int,
    observedBytes: Schema.Int,
    maxBytes: Schema.Int,
  },
) {}

/** Artifacts read-back did not match the commit that Git reported as pushed. */
export class PushVerificationError extends Schema.TaggedError<PushVerificationError>()(
  "PushVerificationError",
  {
    forkName: Schema.String,
    localHead: Schema.String,
    remoteHead: Schema.String,
  },
) {}

/** The sandbox tried to replace main instead of advancing its accepted head. */
export class NonFastForwardPushError extends Schema.TaggedError<NonFastForwardPushError>()(
  "NonFastForwardPushError",
  {
    forkName: Schema.String,
    acceptedHead: Schema.String,
    localHead: Schema.String,
  },
) {}

/** A staged generation handoff can no longer find its exact commit in the preserved workspace. */
export class PreparedCommitUnavailableError extends Schema.TaggedError<PreparedCommitUnavailableError>()(
  "PreparedCommitUnavailableError",
  {
    repositoryName: Schema.String,
    preparedHead: Schema.String,
  },
) {}

/** A mutable repository name no longer resolves to its authorized provider generation. */
export class ArtifactsRepositoryGenerationMismatchError extends Schema.TaggedError<ArtifactsRepositoryGenerationMismatchError>()(
  "ArtifactsRepositoryGenerationMismatchError",
  {
    repositoryName: Schema.String,
    expectedRepositoryId: Schema.NonEmptyString,
  },
) {}

export interface ArtifactsRepositoryGeneration {
  readonly repositoryName: string;
  readonly repositoryId: string;
}

/** A verified, immutable ref in an authorized source generation. */
export interface ArtifactsRepositorySource extends ArtifactsRepositoryGeneration {
  readonly commitSha: string;
  readonly artifactsRef?: string;
}

export class RepositoryForkIntentError extends Schema.TaggedError<RepositoryForkIntentError>()(
  "RepositoryForkIntentError",
  { reason: Schema.Literals(["source-pin", "intent-mismatch", "target-mismatch"]) },
) {}

/** Release/fence ports cannot fail with a typed error; retain every original reason. */
const failuresAsDefects = <E>(cause: Cause.Cause<E>): Cause.Cause<never> =>
  Cause.fromReasons(
    cause.reasons.map((reason) =>
      Cause.isFailReason(reason) ? Cause.makeDieReason(reason.error) : reason,
    ),
  );

/**
 * True when an Artifacts error means "the fork is still provisioning", the
 * protocol fact both the kernel's readiness loop and host-side pre-fork
 * loops must agree on. Owned here so a new provider code cannot silently
 * diverge the two.
 */
export { ForkNotReadyError, isForkProvisioning } from "./ArtifactsFork";

/** Result of finding or provisioning the session's durable writable fork. */
export interface EnsureForkResult {
  /** Verified head of the fork's main branch. */
  readonly head: string;
  /** What the provider proved about this caller's fork request. */
  readonly disposition: ArtifactsForkDisposition;
  /** Readiness probes issued while the fork was provisioning; 0 when the
   * fork already existed. */
  readonly pollAttempts: number;
}

/** Commit and changed paths durably verified after a mutating turn. */
export interface PersistResult {
  readonly head: string;
  readonly changedPaths: ReadonlyArray<string>;
}

/** Exact local commit prepared for a generation-bound push. */
export type BeforeGenerationPush<E, R> = (prepared: PersistResult) => Effect.Effect<void, E, R>;

/**
 * Credentials remain in the native owner's outbound authority. Commands use
 * an isolated transport with HTTPS, redirects and hooks explicitly bounded.
 */
const gitTransportEnv = (remote: string): Record<string, string> => ({
  ...isolatedGitTransportEnv,
  ALL_PROXY: "",
  GIT_ALLOW_PROTOCOL: "https",
  GIT_CONFIG_COUNT: "13",
  GIT_CONFIG_KEY_0: `http.${remote}.extraHeader`,
  GIT_CONFIG_VALUE_0: "",
  GIT_CONFIG_KEY_1: "core.hooksPath",
  GIT_CONFIG_VALUE_1: "/dev/null",
  GIT_CONFIG_KEY_2: "credential.helper",
  GIT_CONFIG_VALUE_2: "",
  GIT_CONFIG_KEY_3: `http.${remote}.proxy`,
  GIT_CONFIG_VALUE_3: "",
  GIT_CONFIG_KEY_4: `http.${remote}.sslVerify`,
  GIT_CONFIG_VALUE_4: "true",
  GIT_CONFIG_KEY_5: `http.${remote}.followRedirects`,
  GIT_CONFIG_VALUE_5: "false",
  GIT_CONFIG_KEY_6: `http.${remote}.curloptResolve`,
  GIT_CONFIG_VALUE_6: "",
  GIT_CONFIG_KEY_7: `url.${remote}.insteadOf`,
  GIT_CONFIG_VALUE_7: remote,
  GIT_CONFIG_KEY_8: "protocol.allow",
  GIT_CONFIG_VALUE_8: "never",
  GIT_CONFIG_KEY_9: "protocol.https.allow",
  GIT_CONFIG_VALUE_9: "always",
  // Artifacts native forks omit delta-encoded objects. Disable both delta
  // compression and bitmap pack reuse, including deltas in existing packs.
  GIT_CONFIG_KEY_10: "pack.window",
  GIT_CONFIG_VALUE_10: "0",
  GIT_CONFIG_KEY_11: "pack.depth",
  GIT_CONFIG_VALUE_11: "0",
  GIT_CONFIG_KEY_12: "pack.allowPackReuse",
  GIT_CONFIG_VALUE_12: "false",
  GIT_SSL_NO_VERIFY: "0",
  GIT_TERMINAL_PROMPT: "0",
  HTTPS_PROXY: "",
  HTTP_PROXY: "",
  NO_PROXY: "*",
  all_proxy: "",
  https_proxy: "",
  http_proxy: "",
  no_proxy: "*",
});

const sanitizeGitDiagnostic = (value: string): string =>
  value
    .replace(/Authorization:\s*Bearer\s+\S+/giu, "Authorization: Bearer [redacted]")
    .replace(/https:\/\/[^\s/@]+@/giu, "https://[redacted]@");

const gitCommandError = (
  command: string,
  output: {
    readonly exitCode: number | null;
    readonly stdout: string;
    readonly stderr: string;
    readonly timedOut: boolean;
    readonly truncated: boolean;
  },
) => {
  // Argv is deliberately not echoed: a token-bearing invocation must
  // never reach an event stream or log through its failure message.
  const text = output.stderr.trim() === "" ? output.stdout : output.stderr;
  const flags = [
    output.timedOut ? "process timed out" : null,
    output.truncated ? "output truncated" : null,
  ].filter((value): value is string => value !== null);
  const diagnostic = `${sanitizeGitDiagnostic(text).trim()}${
    flags.length === 0 ? "" : `${text.trim() === "" ? "" : "\n"}${flags.join(", ")}`
  }`;
  const httpFailure = /The requested URL returned error: ([45][0-9]{2})(?:\s|$)/u.exec(text);

  return GitCommandError.make({
    command,
    exitCode: output.exitCode ?? -1,
    stderr: diagnostic.slice(-2_048),
    ...(httpFailure === null ? {} : { status: Number(httpFailure[1]) }),
  });
};

const statusAndHead = (
  output: string,
): { readonly dirty: boolean; readonly head: string } | null => {
  let head: string | null = null;
  let dirty = false;

  for (const line of output.split("\n")) {
    if (line.startsWith("# branch.oid ")) {
      const value = line.slice("# branch.oid ".length).trim();

      if (value !== "(initial)" && value !== "") head = value;
    } else if (line.trim() !== "" && !line.startsWith("# ")) {
      dirty = true;
    }
  }

  return head === null ? null : { dirty, head };
};

/** Host-owned checkout root; never supplied by model-generated file paths. */
export const ArtifactsCheckoutDirectory = Context.Reference<string>(
  "sandbox-agent/ArtifactsCheckoutDirectory",
  { defaultValue: () => REPO_DIR },
);

const reportCleanupFailure = <E>(cause: Cause.Cause<E>, operation: string): Effect.Effect<void> => {
  if (cause.reasons.every(Cause.isInterruptReason)) return Effect.void;

  return reportSanitizedCause(cause, { operation });
};

const clearRepositoryAuthorization = Effect.fn("ArtifactsCheckout.clearRepositoryAuthorization")(
  function* (grant: Sandbox.RepositoryAuthorizationGrant) {
    const sandbox = yield* SessionSandbox;

    yield* sandbox.clearRepositoryAuthorization(grant).pipe(
      Effect.tapCause((cause) =>
        reportCleanupFailure(cause, "ArtifactsCheckout.clearRepositoryAuthorization"),
      ),
      Effect.ignore,
    );
  },
);

const make = Effect.gen(function* () {
  const random = yield* SecureRandom.SecureRandom;
  const repositoryDirectory = yield* ArtifactsCheckoutDirectory;
  const artifacts = yield* SandboxAgentArtifacts;
  const config = yield* SandboxAgentConfig;
  const deadlines = sandboxAgentDeadlinePolicy(config);
  const forkPolicy = artifactsForkPolicyFromDeadlines(deadlines);
  const tokenTtlSeconds = Math.max(
    MINIMUM_TOKEN_TTL_SECONDS,
    Math.ceil((deadlines.gitCommandMillis + 2 * deadlines.artifactsOperationMillis) / 1_000),
  );

  const remoteFor = (forkName: string): string =>
    `https://${config.artifactsHostname}/git/${config.artifactsNamespace}/${forkName}.git`;

  const boundedArtifacts =
    (operation: string) =>
    <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      effect.pipe(
        Effect.timeoutOrElse({
          duration: deadlines.artifactsOperationMillis,
          orElse: () =>
            ArtifactsOperationTimeoutError.make({
              operation,
              timeoutMillis: deadlines.artifactsOperationMillis,
            }),
        }),
        (bounded) =>
          withFailureDiagnostic(bounded, {
            operation: `ArtifactsCheckout.${operation.replaceAll(":", ".")}`,
          }),
      );

  const verifyRepositoryMetadata = Effect.fnUntraced(function* (
    metadata: Artifacts.ArtifactsRepoInfo,
    generation: ArtifactsRepositoryGeneration,
  ) {
    if (
      metadata.id !== generation.repositoryId ||
      metadata.name !== generation.repositoryName ||
      metadata.remote !== remoteFor(generation.repositoryName)
    ) {
      return yield* ArtifactsRepositoryGenerationMismatchError.make({
        repositoryName: generation.repositoryName,
        expectedRepositoryId: generation.repositoryId,
      });
    }
  });

  const verifyRepositoryGeneration = Effect.fn("ArtifactsCheckout.verifyRepositoryGeneration")(
    function* (
      repository: Artifacts.ArtifactsRepoClient,
      generation: ArtifactsRepositoryGeneration,
    ) {
      yield* verifyRepositoryMetadata(
        yield* repository.info.pipe(boundedArtifacts("verifyRepositoryGeneration")),
        generation,
      );
    },
  );

  const readPinnedRepository = Effect.fnUntraced(function* (
    generation: ArtifactsRepositoryGeneration,
    query:
      | { readonly _tag: "Commit"; readonly commit: string }
      | { readonly _tag: "Heads"; readonly branch: string; readonly includeMain: boolean },
    retryDeadlineAtMillis?: number,
  ) {
    const operation = query._tag === "Commit" ? "readPinnedCommit" : "readSourcePin";
    const repository = yield* artifacts
      .get(generation.repositoryName)
      .pipe(boundedArtifacts(query._tag === "Commit" ? "getCommitRepository" : "getSourcePin"));
    yield* verifyRepositoryMetadata(repository, generation);
    const read = Effect.gen(function* () {
      if (query._tag === "Commit") {
        const commit = yield* repository.readCommit(query.commit).pipe(boundedArtifacts(operation));
        return { pin: commit.hash, main: null };
      }
      const [commits, main] = yield* Effect.all(
        [
          repository.log({ ref: query.branch, limit: 1 }).pipe(boundedArtifacts(operation)),
          query.includeMain && query.branch !== "main"
            ? repository
                .log({ ref: "main", limit: 1 })
                .pipe(boundedArtifacts("readGenerationMainHead"))
            : Effect.succeed([]),
        ],
        { concurrency: "unbounded" },
      );
      yield* verifyRepositoryGeneration(repository, generation);
      const pin = commits[0]?.hash ?? null;
      return { pin, main: query.branch === "main" ? pin : (main[0]?.hash ?? null) };
    });

    return yield* read.pipe(
      Effect.catchCause((original) =>
        Effect.gen(function* () {
          if (
            retryDeadlineAtMillis === undefined ||
            original.reasons.length === 0 ||
            !original.reasons.every(
              (reason) =>
                Cause.isFailReason(reason) &&
                reason.error._tag === "ArtifactsOperationTimeoutError" &&
                (reason.error.operation === operation ||
                  (query._tag === "Heads" && reason.error.operation === "readGenerationMainHead")),
            )
          )
            return yield* Effect.failCause(original);
          const remaining = Math.min(
            deadlines.artifactsOperationMillis,
            retryDeadlineAtMillis - (yield* Clock.currentTimeMillis),
          );
          if (!(remaining > 0)) return yield* Effect.failCause(original);

          // One read-only retry, including generation checks, spends the original budget.
          return yield* Effect.gen(function* () {
            yield* verifyRepositoryGeneration(repository, generation);
            const result = yield* read;
            if (query._tag === "Commit") yield* verifyRepositoryGeneration(repository, generation);
            return result;
          }).pipe(
            Effect.timeoutOrElse({
              duration: remaining,
              orElse: () =>
                ArtifactsOperationTimeoutError.make({ operation, timeoutMillis: remaining }),
            }),
            (recovery) =>
              withFailureDiagnostic(recovery, { operation: `ArtifactsCheckout.${operation}` }),
            Effect.catchCause((recovery) =>
              Effect.failCause(
                Cause.fromReasons<
                  | Artifacts.ArtifactsOperationError
                  | ArtifactsOperationTimeoutError
                  | ArtifactsRepositoryGenerationMismatchError
                >([...original.reasons, ...recovery.reasons]),
              ),
            ),
          );
        }),
      ),
    );
  });

  const runGit = Effect.fn("ArtifactsCheckout.runGit")(function* (
    args: ReadonlyArray<string>,
    options?: {
      readonly cwd?: string;
      readonly env?: Record<string, string>;
      readonly requireCompleteOutput?: boolean;
      readonly deadlineAt?: number;
      readonly inspect?: boolean;
    },
  ) {
    const timeout =
      options?.deadlineAt === undefined
        ? deadlines.gitCommandMillis
        : Math.max(1, options.deadlineAt - (yield* Clock.currentTimeMillis));
    const output = yield* options?.inspect
      ? (yield* SessionSandbox).inspect(["git", ...args], {
          cwd: options.cwd ?? repositoryDirectory,
          timeout,
          deadlineAt: options.deadlineAt,
          maxBytes: GIT_OUTPUT_LIMIT_BYTES,
        })
      : runSandboxCommand(
          ["git", ...args],
          {
            cwd: options?.cwd ?? repositoryDirectory,
            env: options?.env,
            timeout,
            deadlineAt: options?.deadlineAt,
          },
          {
            timeout,
            maxBytes: GIT_OUTPUT_LIMIT_BYTES,
            operation: "ArtifactsCheckout.runGit",
          },
        );

    if (
      output.exitCode !== 0 ||
      output.timedOut ||
      (options?.requireCompleteOutput === true && output.truncated)
    ) {
      return yield* gitCommandError(args[0] ?? "git", output);
    }

    return output.stdout;
  });

  const resetGitTransportDirectory = Effect.fn("ArtifactsCheckout.resetGitTransportDirectory")(
    function* () {
      const sandbox = yield* SessionSandbox;
      const cleanup = yield* sandbox.exec(["rm", "-rf", GIT_TRANSPORT_DIR], {
        cwd: WORKSPACE_DIR,
        timeout: deadlines.gitCommandMillis,
      });
      const output = yield* cleanup.outputText({
        timeout: deadlines.gitCommandMillis,
        maxBytes: GIT_OUTPUT_LIMIT_BYTES,
      });

      if (output.exitCode !== 0 || output.timedOut) {
        const diagnostic = sanitizeGitDiagnostic(
          output.stderr.trim() === "" ? output.stdout : output.stderr,
        );

        return yield* GitCommandError.make({
          command: "git-transport-reset",
          exitCode: output.exitCode,
          stderr: diagnostic.slice(-2_048),
        });
      }

      yield* sandbox.mkdir(GIT_TRANSPORT_DIR);
    },
  );

  const withRepoToken = Effect.fn("ArtifactsCheckout.withRepoToken")(function* <A, E, R>(
    forkName: string,
    scope: "read" | "write",
    use: (deadlineAt: number) => Effect.Effect<A, E, R>,
    expectedRepositoryId?: string,
  ) {
    const repo = yield* artifacts.get(forkName).pipe(boundedArtifacts("getTokenRepository"));
    const sandbox = yield* SessionSandbox;
    const remote = remoteFor(forkName);
    const generation =
      expectedRepositoryId === undefined
        ? undefined
        : { repositoryName: forkName, repositoryId: expectedRepositoryId };

    if (generation !== undefined) yield* verifyRepositoryGeneration(repo, generation);

    return yield* Effect.acquireUseRelease(
      repo
        .createToken(scope, tokenTtlSeconds)
        .pipe(boundedArtifacts(`createToken:${scope}`), inMeasure(`git-token-mint-${scope}`)),
      (minted) =>
        Effect.gen(function* () {
          if (generation !== undefined) yield* verifyRepositoryGeneration(repo, generation);
          const deadlineAt = (yield* Clock.currentTimeMillis) + deadlines.gitCommandMillis;

          return yield* Effect.acquireUseRelease(
            sandbox.setRepositoryAuthorization(
              remote,
              `Bearer ${minted.plaintext}`,
              deadlineAt,
              minted.id,
            ),
            () =>
              Effect.gen(function* () {
                const result = yield* use(deadlineAt);

                if (generation !== undefined) yield* verifyRepositoryGeneration(repo, generation);

                return result;
              }),
            clearRepositoryAuthorization,
          );
        }),
      (minted) =>
        repo.revokeToken(minted.id).pipe(
          boundedArtifacts(`revokeToken:${scope}`),
          Effect.tapCause((cause) => reportCleanupFailure(cause, "ArtifactsCheckout.revokeToken")),
          Effect.ignore,
          inMeasure(`git-token-revoke-${scope}`),
        ),
    );
  });

  const boundGenerationMainHead = Effect.fn("ArtifactsCheckout.boundGenerationMainHead")(function* (
    repository: Artifacts.ArtifactsRepoClient,
    generation: ArtifactsRepositoryGeneration,
  ) {
    // Callers validate fresh get metadata or refresh a previously held client.
    const commits = yield* repository
      .log({ ref: "main", limit: 1 })
      .pipe(boundedArtifacts("readGenerationMainHead"));

    yield* verifyRepositoryGeneration(repository, generation);

    return commits[0]?.hash ?? null;
  });

  const generationMainHead = Effect.fn("ArtifactsCheckout.generationMainHead")(function* (
    generation: ArtifactsRepositoryGeneration,
  ) {
    const repository = yield* artifacts
      .get(generation.repositoryName)
      .pipe(boundedArtifacts("getHeadRepository"));

    yield* verifyRepositoryMetadata(repository, generation);
    return yield* boundGenerationMainHead(repository, generation);
  });

  const sourceGeneration = Effect.fn("ArtifactsCheckout.sourceGeneration")(function* (
    repositoryName: string,
  ) {
    const repository = yield* artifacts
      .get(repositoryName)
      .pipe(boundedArtifacts("getSourceGeneration"));
    const info = yield* repository.info.pipe(boundedArtifacts("readSourceGeneration"));
    const generation = { repositoryName, repositoryId: info.id };

    yield* verifyRepositoryGeneration(repository, generation);

    return generation;
  });

  const readSourceHeads = Effect.fnUntraced(function* (
    source: ArtifactsRepositorySource,
    includeMain: boolean,
    retryDeadlineAtMillis?: number,
  ) {
    const ref = source.artifactsRef ?? "refs/heads/main";

    if (ref.startsWith("refs/") && !ref.startsWith("refs/heads/"))
      return yield* RepositoryForkIntentError.make({ reason: "source-pin" });
    // Artifacts' REST log API takes a branch name; Git advertisements use full refs.
    const heads = yield* readPinnedRepository(
      source,
      { _tag: "Heads", branch: ref.replace(/^refs\/heads\//u, ""), includeMain },
      retryDeadlineAtMillis,
    );
    if (heads.pin !== source.commitSha)
      return yield* RepositoryForkIntentError.make({ reason: "source-pin" });
    return { pin: source.commitSha, main: heads.main };
  });

  const readSourcePin = Effect.fn("ArtifactsCheckout.readSourcePin")(function* (
    source: ArtifactsRepositorySource,
    retryDeadlineAtMillis?: number,
  ) {
    return (yield* readSourceHeads(source, false, retryDeadlineAtMillis)).pin;
  });

  const retainFork = Effect.fn("ArtifactsCheckout.retainFork")(function* (
    forkName: string,
    source: ArtifactsRepositorySource,
    taskDeadlineAtMillis?: number,
  ) {
    const store = yield* SessionStateStore;
    const ref = source.artifactsRef ?? "refs/heads/main";
    const artifactsRef = ref.startsWith("refs/") ? ref : `refs/heads/${ref}`;
    const retained = yield* store.repositoryFork.get;

    if (retained !== null) {
      if (
        retained.forkName !== forkName ||
        retained.source.repositoryName !== source.repositoryName ||
        retained.source.repositoryId !== source.repositoryId ||
        retained.source.commitSha !== source.commitSha ||
        retained.source.artifactsRef !== artifactsRef
      )
        return yield* RepositoryForkIntentError.make({ reason: "intent-mismatch" });

      return retained;
    }
    // Host preparation may have consumed time before this fork budget starts.
    const requestedAtMillis = yield* Clock.currentTimeMillis;
    const deadlineAtMillis = Math.min(
      taskDeadlineAtMillis ?? Infinity,
      requestedAtMillis + deadlines.forkReadyMillis,
    );
    const { main: originalMainHead } = yield* readSourceHeads(source, true, deadlineAtMillis);
    const fingerprint = yield* resourceHash(
      yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Array(Schema.String)))([
        forkName,
        source.repositoryName,
        source.repositoryId,
        source.commitSha,
        artifactsRef,
      ]),
    );
    const intent: StoredRepositoryFork = {
      forkName,
      source: { ...source, artifactsRef },
      originalMainHead,
      description: `sandbox-task-fork:${fingerprint}`,
      requestedAtMillis,
      deadlineAtMillis,
      repositoryId: null,
      initialized: false,
    };

    // The provider may complete a request whose response is lost. This receipt
    // owns its exact name and creation shape before the first external mutation.
    yield* store.repositoryFork.put(intent);

    return intent;
  });

  const sameForkIntent = (left: StoredRepositoryFork, right: StoredRepositoryFork) =>
    left.forkName === right.forkName &&
    left.description === right.description &&
    left.source.repositoryName === right.source.repositoryName &&
    left.source.repositoryId === right.source.repositoryId &&
    left.source.commitSha === right.source.commitSha &&
    left.source.artifactsRef === right.source.artifactsRef &&
    left.originalMainHead === right.originalMainHead &&
    left.requestedAtMillis === right.requestedAtMillis &&
    left.deadlineAtMillis === right.deadlineAtMillis;

  const plannedForkGeneration = Effect.fn("ArtifactsCheckout.plannedForkGeneration")(function* (
    intent: StoredRepositoryFork,
  ) {
    const info = yield* artifacts.get(intent.forkName).pipe(boundedArtifacts("getPlannedFork"));

    if (
      info.name !== intent.forkName ||
      info.remote !== remoteFor(intent.forkName) ||
      info.description !== intent.description ||
      info.defaultBranch !== "main" ||
      info.source !== `artifacts:${config.artifactsNamespace}/${intent.source.repositoryName}` ||
      info.readOnly ||
      (intent.repositoryId !== null && info.id !== intent.repositoryId)
    )
      return yield* RepositoryForkIntentError.make({ reason: "target-mismatch" });

    return { repositoryName: intent.forkName, repositoryId: info.id };
  });

  /** Remote work only: this can run beside workspace preparation without touching its Git state. */
  const ensurePlannedFork = Effect.fn("ArtifactsCheckout.ensurePlannedFork")(function* (
    intent: StoredRepositoryFork,
  ) {
    const store = yield* SessionStateStore;
    const retained = yield* store.repositoryFork.get;

    if (retained === null || !sameForkIntent(retained, intent))
      return yield* RepositoryForkIntentError.make({ reason: "intent-mismatch" });
    if (retained.initialized) {
      if (retained.repositoryId === null)
        return yield* RepositoryForkIntentError.make({ reason: "target-mismatch" });

      return yield* plannedForkGeneration(retained);
    }
    const expired = () =>
      ForkNotReadyError.make({
        forkName: retained.forkName,
        message: "The retained fork initialization deadline expired",
      });

    yield* readSourcePin(retained.source, retained.deadlineAtMillis);
    const existing = yield* plannedForkGeneration(retained).pipe(
      Effect.catchCauseIf(
        (cause) =>
          cause.reasons.length > 0 &&
          cause.reasons.every(
            (reason) =>
              Cause.isFailReason(reason) &&
              reason.error._tag === "ArtifactsOperationError" &&
              isForkProvisioning(reason.error),
          ),
        () => Effect.succeed(null),
      ),
      inMeasure("fork-lookup"),
    );

    if (existing !== null && (yield* generationMainHead(existing)) === retained.source.commitSha) {
      // A fork or CAS can finish after its response/deadline was lost. Reads
      // may reconcile that exact result; expiry never authorizes another write.
      yield* readSourcePin(
        {
          ...existing,
          commitSha: retained.source.commitSha,
          artifactsRef: retained.source.artifactsRef,
        },
        retained.deadlineAtMillis,
      );
      const bound = { ...retained, repositoryId: existing.repositoryId };

      yield* plannedForkGeneration(bound);
      yield* store.repositoryFork.put({ ...bound, initialized: true });

      return existing;
    }
    const remainingBeforeRequest = retained.deadlineAtMillis - (yield* Clock.currentTimeMillis);

    if (remainingBeforeRequest <= 0) return yield* expired();

    if (existing === null) {
      if (retained.repositoryId !== null)
        return yield* RepositoryForkIntentError.make({ reason: "target-mismatch" });
      yield* requestArtifactsFork({
        artifacts,
        seedRepository: retained.source.repositoryName,
        forkName: retained.forkName,
        policy: {
          ...forkPolicy,
          operationTimeoutMillis: Math.min(
            forkPolicy.operationTimeoutMillis,
            remainingBeforeRequest,
          ),
        },
        description: retained.description,
        defaultBranchOnly: false,
      }).pipe(inMeasure("fork-create-request"));
    }
    let pollAttempts = 0;

    // This loop owns its deadline so a competing timer cannot discard the provider cause.
    yield* awaitArtifactsForkReady({
      artifacts,
      forkName: retained.forkName,
      policy: forkPolicy,
      readyRef: retained.source.artifactsRef.replace(/^refs\/heads\//u, ""),
      deadlineAtMillis: retained.deadlineAtMillis,
      onPollAttempt: () => {
        pollAttempts += 1;
      },
    }).pipe(
      Effect.onExit(() => annotateSpanAttempts(pollAttempts)),
      inMeasure("fork-poll"),
    );
    const remaining = retained.deadlineAtMillis - (yield* Clock.currentTimeMillis);

    if (remaining <= 0) return yield* expired();

    return yield* Effect.gen(function* () {
      yield* readSourcePin(retained.source, retained.deadlineAtMillis);
      const generation = yield* plannedForkGeneration(retained);
      const bound = { ...retained, repositoryId: generation.repositoryId };

      yield* store.repositoryFork.put(bound);
      yield* readSourcePin(
        {
          ...generation,
          commitSha: retained.source.commitSha,
          artifactsRef: retained.source.artifactsRef,
        },
        retained.deadlineAtMillis,
      );
      const repository = yield* artifacts
        .get(generation.repositoryName)
        .pipe(boundedArtifacts("getForkInitialization"));
      const head = yield* generationMainHead(generation);

      if (head !== retained.source.commitSha) {
        if (head !== retained.originalMainHead)
          return yield* RepositoryForkIntentError.make({ reason: "target-mismatch" });
        const fence = Effect.gen(function* () {
          const current = yield* store.repositoryFork.get;

          if (
            current === null ||
            !sameForkIntent(current, retained) ||
            current.initialized ||
            current.repositoryId !== generation.repositoryId ||
            (yield* Clock.currentTimeMillis) >= retained.deadlineAtMillis
          )
            return yield* RepositoryMutationFenceFailure.make({
              reason: "stopped",
              message: "The retained fork no longer authorizes initialization",
            });
          yield* plannedForkGeneration(bound).pipe(Effect.asVoid);
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.failCause(
              Cause.map(cause, (error) =>
                error._tag === "RepositoryMutationFenceFailure"
                  ? error
                  : RepositoryMutationFenceFailure.make({
                      reason:
                        error._tag === "RepositoryForkIntentError" ? "stopped" : "unavailable",
                      message: "The private fork initialization fence could not be verified",
                      cause: error,
                    }),
              ),
            ),
          ),
        );

        yield* Effect.acquireUseRelease(
          repository
            .createToken("write", tokenTtlSeconds)
            .pipe(boundedArtifacts("createToken:initialize")),
          (minted) =>
            transferRepositoryGit({
              source: {
                url: remoteFor(retained.forkName),
                authorization: Redacted.make(`Bearer ${minted.plaintext}`),
              },
              target: {
                url: remoteFor(retained.forkName),
                authorization: Redacted.make(`Bearer ${minted.plaintext}`),
              },
              commitSha: retained.source.commitSha,
              branch: "main",
              // The immutable pin ref exists even when main is absent. Omitted
              // expectedHead still gives main an absent-ref CAS, not an empty repo check.
              requireEmptyTarget: false,
              ...(retained.originalMainHead === null
                ? {}
                : { expectedHead: retained.originalMainHead }),
            }).pipe(
              Effect.provideService(RepositoryMutationFence, { check: fence }),
              Effect.provide(FetchHttpClient.layer),
            ),
          (minted) =>
            repository.revokeToken(minted.id).pipe(
              boundedArtifacts("revokeToken:initialize"),
              Effect.catchCause((cause) => Effect.failCause(failuresAsDefects(cause))),
            ),
        ).pipe(inMeasure("fork-main-initialize"));
      }
      yield* verifyExpectedHead(
        retained.forkName,
        retained.source.commitSha,
        generation.repositoryId,
      );
      // Never initialize main again after this receipt. Subsequent heads belong to task outputs.
      yield* store.repositoryFork.put({ ...bound, initialized: true });

      return generation;
    }).pipe(Effect.timeoutOrElse({ duration: remaining, orElse: expired }));
  });

  /** Read-through used after push to verify the durable branch head. */
  const mainHead = Effect.fn("ArtifactsCheckout.mainHead")(function* (
    forkName: string,
    expectedRepositoryId?: string,
  ) {
    const head =
      expectedRepositoryId === undefined
        ? yield* artifactsMainHead(artifacts, forkName, deadlines.artifactsOperationMillis)
        : yield* generationMainHead({
            repositoryName: forkName,
            repositoryId: expectedRepositoryId,
          });

    if (head === null) {
      return yield* ForkNotReadyError.make({
        forkName,
        message: "Fork main branch has no commits",
      });
    }

    return head;
  });

  const verifyExpectedHead = Effect.fn("ArtifactsCheckout.verifyExpectedHead")(function* (
    forkName: string,
    expectedHead: string,
    expectedRepositoryId?: string,
  ) {
    return yield* mainHead(forkName, expectedRepositoryId).pipe(
      Effect.filterOrElse(
        (remoteHead) => remoteHead === expectedHead,
        (remoteHead) =>
          PushVerificationError.make({
            forkName,
            localHead: expectedHead,
            remoteHead,
          }),
      ),
      Effect.retry({
        times: 2,
        while: (error) =>
          error._tag === "PushVerificationError" ||
          error._tag === "ForkNotReadyError" ||
          error._tag === "ArtifactsOperationTimeoutError" ||
          error._tag === "ArtifactsOperationError",
        schedule: Schedule.exponential("200 millis").pipe(Schedule.jittered),
      }),
    );
  });

  const verifyExpectedBoundGenerationHead = Effect.fn(
    "ArtifactsCheckout.verifyExpectedBoundGenerationHead",
  )(function* (
    repository: Artifacts.ArtifactsRepoClient,
    generation: ArtifactsRepositoryGeneration,
    expectedHead: string,
  ) {
    return yield* verifyRepositoryGeneration(repository, generation).pipe(
      Effect.andThen(boundGenerationMainHead(repository, generation)),
      Effect.flatMap((remoteHead) =>
        Effect.gen(function* () {
          if (remoteHead === null) {
            return yield* ForkNotReadyError.make({
              forkName: generation.repositoryName,
              message: "Fork main branch has no commits",
            });
          }
          if (remoteHead !== expectedHead) {
            return yield* PushVerificationError.make({
              forkName: generation.repositoryName,
              localHead: expectedHead,
              remoteHead,
            });
          }

          return remoteHead;
        }),
      ),
      Effect.retry({
        times: 2,
        while: (error) =>
          error._tag === "PushVerificationError" ||
          error._tag === "ForkNotReadyError" ||
          error._tag === "ArtifactsOperationTimeoutError" ||
          error._tag === "ArtifactsOperationError",
        schedule: Schedule.exponential("200 millis").pipe(Schedule.jittered),
      }),
    );
  });

  const ensureFork = Effect.fn("ArtifactsCheckout.ensureFork")(function* (
    forkName: string,
    seed = config.seedRepository,
    policy: ArtifactsForkPolicy = forkPolicy,
  ) {
    const seedRepository = yield* requireRepositorySeed(seed);
    const existingHead = yield* artifactsMainHead(
      artifacts,
      forkName,
      policy.operationTimeoutMillis,
    ).pipe(
      Effect.catchTags({
        ArtifactsOperationError: (error) =>
          error.code === "NOT_FOUND" || error.code === "FORK_IN_PROGRESS"
            ? Effect.succeed(null)
            : Effect.fail(error),
        ArtifactsOperationTimeoutError: () => Effect.succeed(null),
      }),
      inMeasure("fork-lookup"),
    );

    if (existingHead !== null) {
      return {
        head: existingHead,
        disposition: "existing",
        pollAttempts: 0,
      } satisfies EnsureForkResult;
    }
    const requested = yield* requestArtifactsFork({
      artifacts,
      seedRepository,
      forkName,
      policy,
    }).pipe(inMeasure("fork-create-request"));
    let pollAttempts = 0;
    const head = yield* awaitArtifactsForkReady({
      artifacts,
      forkName,
      policy,
      onPollAttempt: () => {
        pollAttempts += 1;
      },
    }).pipe(
      Effect.onExit(() => annotateSpanAttempts(pollAttempts)),
      inMeasure("fork-poll"),
    );

    return {
      head,
      disposition: requested.disposition,
      pollAttempts,
    } satisfies EnsureForkResult;
  });

  const discardCloneReceipt = (scope: string) =>
    Effect.flatMap(
      SessionStateStore,
      (store) => store.workspacePreparation(`clone:${scope}`).delete,
    ).pipe(Effect.asVoid);

  const cloneRepositoryIntoSandbox = Effect.fn("ArtifactsCheckout.cloneRepositoryIntoSandbox")(
    function* (
      repositoryName: string,
      expectedRepositoryId?: string,
      scope = "workspace",
      ref = "main",
    ) {
      const remote = remoteFor(repositoryName);
      const generation =
        expectedRepositoryId === undefined
          ? undefined
          : { repositoryName, repositoryId: expectedRepositoryId };
      const repository = yield* artifacts
        .get(repositoryName)
        .pipe(boundedArtifacts("getCloneRepository"));
      const sandbox = yield* SessionSandbox;

      if (generation !== undefined) yield* verifyRepositoryGeneration(repository, generation);
      const cloneAttempt = Effect.gen(function* () {
        const retained = yield* (yield* SessionStateStore).workspacePreparation(`clone:${scope}`)
          .get;

        if (
          retained !== null &&
          (retained.repositoryToken?.repositoryName !== repositoryName ||
            retained.repositoryToken.repositoryId !== expectedRepositoryId)
        )
          return yield* HarnessTurnError.make({
            reason: "turnFailed",
            message: "A different repository preparation still owns this workspace",
          });
        let repositoryToken = retained?.repositoryToken;
        let request: WorkspacePreparationRequest | undefined;

        if (retained === null) {
          if (generation !== undefined) yield* verifyRepositoryGeneration(repository, generation);
          const minted = yield* repository
            .createToken("read", tokenTtlSeconds)
            .pipe(boundedArtifacts("createToken:read"));

          if (generation !== undefined) yield* verifyRepositoryGeneration(repository, generation);
          const deadlineAt = (yield* Clock.currentTimeMillis) + deadlines.gitCommandMillis;

          const authorization = yield* sandbox.setRepositoryAuthorization(
            remote,
            `Bearer ${minted.plaintext}`,
            deadlineAt,
            minted.id,
          );

          repositoryToken = {
            repositoryName,
            ...(expectedRepositoryId === undefined ? {} : { repositoryId: expectedRepositoryId }),
            tokenId: minted.id,
            authorization,
          };
          request = {
            deadlineAt,
            command: [
              "sh",
              "-c",
              // Accepted clone receipts own their exact command bytes. Keep
              // main's existing command unchanged when no new ref was admitted.
              ref === "main"
                ? 'set -eu; rm -rf -- "$1" "$2"; mkdir -p -- "$2"; cd "$2"; git clone --depth 1 --single-branch --branch main --no-tags "$3" "$1"; git -C "$1" config --local user.name "$4"; git -C "$1" config --local user.email "$5"; git -C "$1" rev-parse HEAD'
                : 'set -eu; rm -rf -- "$1" "$2"; mkdir -p -- "$2"; cd "$2"; git clone --depth 1 --single-branch --branch "$6" --no-tags "$3" "$1"; git -C "$1" config --local user.name "$4"; git -C "$1" config --local user.email "$5"; git -C "$1" rev-parse HEAD',
              "workspace-clone",
              repositoryDirectory,
              GIT_TRANSPORT_DIR,
              remote,
              AGENT_COMMIT_AUTHOR,
              AGENT_COMMIT_EMAIL,
              ...(ref === "main" ? [] : [ref]),
            ],
            cwd: WORKSPACE_DIR,
            env: gitTransportEnv(remote),
            maxOutputBytes: GIT_OUTPUT_LIMIT_BYTES,
            repositoryToken,
          };
        }
        const output = yield* runWorkspacePreparation(`clone:${scope}`, request);

        if (repositoryToken !== undefined) {
          yield* clearRepositoryAuthorization(repositoryToken.authorization);
          const { repositoryName, tokenId } = repositoryToken;

          yield* Effect.gen(function* () {
            const repo = yield* artifacts.get(repositoryName);

            yield* repo.revokeToken(tokenId);
          }).pipe(
            boundedArtifacts("revokeToken:read"),
            Effect.tapCause((cause) =>
              reportCleanupFailure(cause, "ArtifactsCheckout.revokeToken"),
            ),
            Effect.ignore,
          );
        }
        yield* acknowledgeWorkspacePreparation(`clone:${scope}`);

        return output;
      }).pipe(
        Effect.flatMap((output) => {
          if (output.exitCode !== 0 || output.timedOut || output.truncated)
            return gitCommandError("clone", output);
          const head = output.stdout.trim();

          return /^[0-9a-f]{40}$/u.test(head)
            ? Effect.succeed({ head })
            : GitCommandError.make({
                command: "clone",
                exitCode: 0,
                stderr: "Clone returned an invalid HEAD",
              });
        }),
      );
      const cloned = yield* cloneAttempt.pipe(
        Effect.retry({
          times: GIT_READ_RETRY_COUNT,
          while: (error) => error._tag === "GitCommandError",
          schedule: Schedule.exponential("250 millis").pipe(Schedule.jittered),
        }),
      );

      if (generation !== undefined) yield* verifyRepositoryGeneration(repository, generation);

      return cloned;
    },
  );

  const cloneIntoSandbox = Effect.fn("ArtifactsCheckout.cloneIntoSandbox")(function* (
    forkName: string,
    scope = "workspace",
    bootstrap?: {
      readonly source: ArtifactsRepositoryGeneration;
      readonly forkHead: string;
    },
  ) {
    if (bootstrap === undefined)
      return yield* cloneRepositoryIntoSandbox(forkName, undefined, scope);
    const retained = yield* (yield* SessionStateStore).workspacePreparation(`clone:${scope}`).get;
    const seedCloneName = config.seedCloneRepositoryName(bootstrap.source.repositoryName);
    let cloneSource = bootstrap.source;

    if (retained !== null) {
      // The preparation receipt keeps the exact source and generation chosen at admission.
      const token = retained.repositoryToken;

      if (token?.repositoryName === forkName && token.repositoryId === undefined)
        return yield* cloneRepositoryIntoSandbox(forkName, undefined, scope);
      if (
        token?.repositoryId === undefined ||
        (token.repositoryName !== bootstrap.source.repositoryName &&
          token.repositoryName !== seedCloneName) ||
        (token.repositoryName === bootstrap.source.repositoryName &&
          token.repositoryId !== bootstrap.source.repositoryId)
      )
        return yield* HarnessTurnError.make({
          reason: "turnFailed",
          message: "A different repository preparation still owns this workspace",
        });
      cloneSource = { repositoryName: token.repositoryName, repositoryId: token.repositoryId };
    } else {
      if ((yield* generationMainHead(bootstrap.source)) !== bootstrap.forkHead)
        return yield* cloneRepositoryIntoSandbox(forkName, undefined, scope);
      // The sync repository serves Git while the immutable source remains the head authority.
      if (seedCloneName !== bootstrap.source.repositoryName)
        cloneSource = yield* sourceGeneration(seedCloneName);
    }
    const cloned = yield* cloneRepositoryIntoSandbox(
      cloneSource.repositoryName,
      cloneSource.repositoryId,
      scope,
    );

    if (
      cloned.head !== bootstrap.forkHead ||
      (yield* generationMainHead(bootstrap.source)) !== bootstrap.forkHead
    )
      return yield* GitCommandError.make({
        command: "clone",
        exitCode: 0,
        stderr: "Bootstrap clone does not match the admitted fork head",
      });
    yield* retargetOrigin(forkName);

    return cloned;
  });

  const cloneGenerationIntoSandbox = Effect.fn("ArtifactsCheckout.cloneGenerationIntoSandbox")(
    function* (generation: ArtifactsRepositoryGeneration) {
      return yield* cloneRepositoryIntoSandbox(generation.repositoryName, generation.repositoryId);
    },
  );

  const cloneSourceIntoSandbox = Effect.fn("ArtifactsCheckout.cloneSourceIntoSandbox")(function* (
    source: ArtifactsRepositorySource,
    forkName: string,
    retryDeadlineAtMillis?: number,
  ) {
    yield* readSourcePin(source, retryDeadlineAtMillis);
    const cloned = yield* cloneRepositoryIntoSandbox(
      source.repositoryName,
      source.repositoryId,
      "workspace",
      (source.artifactsRef ?? "main").replace(/^refs\/heads\//u, ""),
    );

    if (cloned.head !== source.commitSha)
      return yield* RepositoryForkIntentError.make({ reason: "source-pin" });
    yield* readSourcePin(source, retryDeadlineAtMillis);
    yield* retargetOrigin(forkName);

    return cloned;
  });

  /** Point a seed clone at its writable session fork without persisting a
   * credential. The caller first proves both repositories have the same head. */
  const retargetOrigin = Effect.fn("ArtifactsCheckout.retargetOrigin")(function* (
    forkName: string,
  ) {
    yield* runGit(["remote", "set-url", "origin", remoteFor(forkName)]).pipe(
      inMeasure("git-remote-retarget"),
    );
  });

  const checkoutCommit = Effect.fn("ArtifactsCheckout.checkoutCommit")(function* (commit: string) {
    yield* runGit(["checkout", "--detach", commit]);
    const head = (yield* runGit(["rev-parse", "HEAD"], { requireCompleteOutput: true })).trim();

    if (head !== commit)
      return yield* GitCommandError.make({
        command: "checkout",
        exitCode: 0,
        stderr: "the checkout does not match the pinned commit",
      });
  });

  const restoreSnapshotCheckout = Effect.fn("ArtifactsCheckout.restoreSnapshotCheckout")(function* (
    repositoryName: string,
    head: string,
    checkpointHead: string,
  ) {
    const restoredHead = (yield* runGit(["rev-parse", "HEAD"], {
      requireCompleteOutput: true,
      inspect: true,
    })).trim();

    if (restoredHead !== checkpointHead)
      return yield* GitCommandError.make({
        command: "snapshot-head",
        exitCode: 0,
        stderr: "Snapshot source does not match its receipt",
      });
    yield* retargetOrigin(repositoryName);
    // Snapshot validation already loaded the target commit and inspected its setup inputs.
    // An unchanged pristine snapshot retains the installation's filesystem outputs.
    yield* Effect.annotateCurrentSpan(
      "repository.git.checkout",
      head === checkpointHead ? "unchanged" : "updated",
    );
    if (head !== checkpointHead) {
      yield* runGit(["reset", "--hard", head]);
      yield* checkoutCommit(head);
    }
  });

  const restorePreparedRepository = Effect.fn("ArtifactsCheckout.restorePreparedRepository")(
    function* (candidate: PersistResult, transportDirectory: string) {
      const sandbox = yield* SessionSandbox;
      const cleanup = yield* sandbox.exec(["rm", "-rf", "--", repositoryDirectory], {
        cwd: WORKSPACE_DIR,
        timeout: deadlines.gitCommandMillis,
      });
      const output = yield* cleanup.outputText({
        timeout: deadlines.gitCommandMillis,
        maxBytes: GIT_OUTPUT_LIMIT_BYTES,
      });

      if (output.exitCode !== 0 || output.timedOut)
        return yield* gitCommandError("restore-repository-reset", output);

      const env = { ...isolatedGitTransportEnv, GIT_ALLOW_PROTOCOL: "file" };

      yield* runGit(["init", repositoryDirectory], {
        cwd: WORKSPACE_DIR,
        env,
        requireCompleteOutput: true,
      });
      for (const [key, value] of [
        ["core.hooksPath", "/dev/null"],
        ["user.name", AGENT_COMMIT_AUTHOR],
        ["user.email", AGENT_COMMIT_EMAIL],
      ] as const) {
        yield* runGit(["config", "--local", key, value], { env, requireCompleteOutput: true });
      }
      // FETCH_HEAD is not a branch: a normal clone could omit the saved commit.
      // Fetch its exact object from the restored, host-owned bare repository.
      yield* runGit(
        ["fetch", "--no-tags", "--update-shallow", transportDirectory, candidate.head],
        { env, requireCompleteOutput: true },
      );
      yield* runGit(["checkout", "--detach", candidate.head], {
        env,
        requireCompleteOutput: true,
      });
      const head = (yield* runGit(["rev-parse", "HEAD"], {
        env,
        requireCompleteOutput: true,
      })).trim();

      if (head !== candidate.head)
        return yield* GitCommandError.make({
          command: "checkout",
          exitCode: 0,
          stderr: "the restored checkout does not match the prepared commit",
        });
    },
  );

  const describePreparedCommit = Effect.fn("ArtifactsCheckout.describePreparedCommit")(function* (
    forkName: string,
    verifiedHead: string,
    head: string,
  ) {
    const resolved = (yield* runGit(["rev-parse", `${head}^{commit}`], {
      requireCompleteOutput: true,
    })).trim();

    if (resolved !== head) {
      return yield* GitCommandError.make({
        command: "rev-parse",
        exitCode: 0,
        stderr: "the prepared commit no longer resolves to its exact object id",
      });
    }

    yield* runGit(["merge-base", "--is-ancestor", verifiedHead, head], {
      requireCompleteOutput: true,
    }).pipe(
      Effect.catchTag(
        "GitCommandError",
        (error): Effect.Effect<never, GitCommandError | NonFastForwardPushError> =>
          error.exitCode === 1
            ? Effect.fail(
                NonFastForwardPushError.make({
                  forkName,
                  acceptedHead: verifiedHead,
                  localHead: head,
                }),
              )
            : Effect.fail(error),
      ),
    );
    const changedPaths = (yield* runGit(["diff", "--name-only", verifiedHead, head], {
      requireCompleteOutput: true,
    }))
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line.length > 0);
    const encodedBytes = new TextEncoder().encode(toJson(changedPaths)).byteLength;

    if (encodedBytes > CHANGED_PATHS_LIMIT_BYTES) {
      return yield* PersistedPathsLimitError.make({
        pathCount: changedPaths.length,
        observedBytes: encodedBytes,
        maxBytes: CHANGED_PATHS_LIMIT_BYTES,
      });
    }

    return { head, changedPaths } satisfies PersistResult;
  });

  const prepareGitTransport = Effect.fn("ArtifactsCheckout.prepareGitTransport")(function* (
    local: PersistResult,
  ) {
    yield* resetGitTransportDirectory();
    yield* runGit(["init", "--bare", GIT_TRANSPORT_REPOSITORY], {
      cwd: GIT_TRANSPORT_DIR,
      env: isolatedGitTransportEnv,
      requireCompleteOutput: true,
    });
    yield* runGit(
      ["fetch", "--no-tags", "--force", "--update-shallow", repositoryDirectory, local.head],
      {
        cwd: GIT_TRANSPORT_DIR,
        env: {
          ...isolatedGitTransportEnv,
          GIT_DIR: GIT_TRANSPORT_REPOSITORY,
        },
        requireCompleteOutput: true,
      },
    );
  });

  const pushPreparedRepository = Effect.fn("ArtifactsCheckout.pushPreparedRepository")(function* <
    E,
    R,
  >(
    forkName: string,
    local: PersistResult,
    generation?: ArtifactsRepositoryGeneration,
    beforeGenerationPush?: BeforeGenerationPush<E, R>,
  ) {
    const persisted = yield* Effect.scoped(
      Effect.gen(function* () {
        const repo = yield* artifacts
          .get(forkName)
          .pipe(boundedArtifacts("getWriteTokenRepository"));

        if (generation !== undefined) {
          yield* verifyRepositoryGeneration(repo, generation);
          if (beforeGenerationPush !== undefined) {
            // Commit the exact prepared source before granting its remote write.
            yield* beforeGenerationPush(local);
          }
        }
        const credential = yield* Effect.acquireRelease(
          Effect.gen(function* () {
            const minted = yield* repo
              .createToken("write", tokenTtlSeconds)
              .pipe(boundedArtifacts("createToken:write"), inMeasure("git-token-mint-write"));

            return { repo, minted } as const;
          }),
          ({ repo, minted }) =>
            repo.revokeToken(minted.id).pipe(
              boundedArtifacts("revokeToken:write"),
              Effect.tapCause((cause) =>
                reportCleanupFailure(cause, "ArtifactsCheckout.revokeToken"),
              ),
              Effect.ignore,
              inMeasure("git-token-revoke-write"),
            ),
        );
        const remote = remoteFor(forkName);
        const sandbox = yield* SessionSandbox;

        if (generation !== undefined) {
          yield* verifyRepositoryGeneration(credential.repo, generation);
        }
        const deadlineAt = (yield* Clock.currentTimeMillis) + deadlines.gitCommandMillis;

        yield* Effect.acquireRelease(
          sandbox.setRepositoryAuthorization(
            remote,
            `Bearer ${credential.minted.plaintext}`,
            deadlineAt,
            credential.minted.id,
          ),
          clearRepositoryAuthorization,
        );

        const verifyPushedHead = Effect.gen(function* () {
          if (generation === undefined) {
            return yield* verifyExpectedHead(forkName, local.head);
          }

          return yield* verifyExpectedBoundGenerationHead(credential.repo, generation, local.head);
        });

        yield* runGit(["push", "--no-thin", remote, "FETCH_HEAD:refs/heads/main"], {
          cwd: GIT_TRANSPORT_DIR,
          deadlineAt,
          env: {
            ...gitTransportEnv(remote),
            GIT_DIR: GIT_TRANSPORT_REPOSITORY,
          },
        }).pipe(
          inPhase(agentPhases.gitPush),
          // A failed push is indeterminate: the remote may have accepted the
          // commit before the failure surfaced. The provider head decides.
          Effect.catch((error) =>
            verifyPushedHead.pipe(
              Effect.asVoid,
              Effect.catchTag("PushVerificationError", () => Effect.fail(error)),
            ),
          ),
        );
        yield* verifyPushedHead;

        return local;
      }),
    );

    yield* verifyExpectedHead(forkName, persisted.head, generation?.repositoryId).pipe(
      inPhase(agentPhases.artifactsVerify),
    );

    return persisted;
  });

  const prepareRepository = Effect.fn("ArtifactsCheckout.prepareRepository")(function* (
    forkName: string,
    verifiedHead: string,
    generation?: ArtifactsRepositoryGeneration,
  ) {
    const statusOutput = yield* runGit(
      ["status", "--porcelain=v2", "--branch", "--untracked-files=normal"],
      { requireCompleteOutput: true },
    ).pipe(inPhase(agentPhases.gitStatus));
    const status = statusAndHead(statusOutput);

    if (status === null) {
      return yield* GitCommandError.make({
        command: "status",
        exitCode: 0,
        stderr: "git status did not report branch.oid",
      });
    }

    // ensureFork verified the remote at the start of this serialized turn.
    // When neither the working tree nor HEAD moved, minting a write token,
    // pushing, and reading the same remote head again cannot add durability.
    if (!status.dirty && status.head === verifiedHead) {
      if (generation !== undefined) {
        yield* verifyExpectedHead(forkName, verifiedHead, generation.repositoryId);
      }
      yield* markPoint("git-persist-skipped");

      return {
        changed: false,
        local: { head: status.head, changedPaths: [] } satisfies PersistResult,
      };
    }
    if (status.dirty) {
      // Harness-authored commits are preserved as-is; only residual working
      // tree changes get the kernel-owned commit.
      yield* runGit(["add", "-A"], { requireCompleteOutput: true }).pipe(
        Effect.flatMap(() =>
          runGit(
            [
              "-c",
              `user.name=${AGENT_COMMIT_AUTHOR}`,
              "-c",
              `user.email=${AGENT_COMMIT_EMAIL}`,
              "commit",
              "-m",
              ["sandbox-agent: persist harness turn", config.commitCoauthorTrailer]
                .filter(Boolean)
                .join("\n\n"),
            ],
            { requireCompleteOutput: true },
          ),
        ),
        inPhase(agentPhases.gitCommit),
      );
    }
    const head = status.dirty
      ? (yield* runGit(["rev-parse", "HEAD"], { requireCompleteOutput: true })).trim()
      : status.head;
    const local = yield* describePreparedCommit(forkName, verifiedHead, head);

    return { changed: true, local };
  });

  const persist = Effect.fn("ArtifactsCheckout.persist")(function* (
    forkName: string,
    verifiedHead: string,
    sourceKey: string,
    repositoryId?: string,
  ) {
    const generation =
      repositoryId === undefined ? undefined : { repositoryName: forkName, repositoryId };
    const prepared = yield* prepareRepository(forkName, verifiedHead, generation);

    if (!prepared.changed) {
      yield* (yield* RepositoryPersistenceCheckpoint).prepared(prepared.local, null);

      return prepared.local;
    }
    yield* assertRepositorySetupSecretsNotPersisted(
      sourceKey,
      verifiedHead,
      prepared.local.head,
    ).pipe(Effect.provideService(SandboxAgentConfig, config));
    yield* prepareGitTransport(prepared.local);
    // This immutable host-owned copy is complete before checkpointing, and no
    // remote write token or push exists until the host retains its recovery fact.
    yield* (yield* RepositoryPersistenceCheckpoint).prepared(
      prepared.local,
      GIT_TRANSPORT_REPOSITORY,
    );

    return yield* pushPreparedRepository<never, never>(forkName, prepared.local, generation);
  });

  const persistGeneration = Effect.fn("ArtifactsCheckout.persistGeneration")(function* <E, R>(
    generation: ArtifactsRepositoryGeneration,
    verifiedHead: string,
    beforePush?: BeforeGenerationPush<E, R>,
  ) {
    const prepared = yield* prepareRepository(generation.repositoryName, verifiedHead, generation);

    if (!prepared.changed) return prepared.local;

    yield* prepareGitTransport(prepared.local);

    return yield* pushPreparedRepository(
      generation.repositoryName,
      prepared.local,
      generation,
      beforePush,
    );
  });

  const pushPreparedGeneration = Effect.fn("ArtifactsCheckout.pushPreparedGeneration")(function* (
    generation: ArtifactsRepositoryGeneration,
    verifiedHead: string,
    preparedHead: string,
  ) {
    yield* runGit(["cat-file", "-e", `${preparedHead}^{commit}`], {
      requireCompleteOutput: true,
    }).pipe(
      Effect.catchTag(
        "GitCommandError",
        (error): Effect.Effect<never, GitCommandError | PreparedCommitUnavailableError> =>
          error.exitCode === 128
            ? Effect.fail(
                PreparedCommitUnavailableError.make({
                  repositoryName: generation.repositoryName,
                  preparedHead,
                }),
              )
            : Effect.fail(error),
      ),
    );
    const local = yield* describePreparedCommit(
      generation.repositoryName,
      verifiedHead,
      preparedHead,
    );

    yield* prepareGitTransport(local);

    return yield* pushPreparedRepository<never, never>(
      generation.repositoryName,
      local,
      generation,
    );
  });

  const ensureCommit = Effect.fn("ArtifactsCheckout.ensureCommit")(function* (
    forkName: string,
    commit: string,
    options?: {
      readonly includeHistory?: boolean;
      readonly repositoryId?: string;
      readonly retryDeadlineAtMillis?: number;
    },
  ) {
    if (options?.repositoryId !== undefined) {
      yield* readPinnedRepository(
        { repositoryName: forkName, repositoryId: options.repositoryId },
        { _tag: "Commit", commit },
        options.retryDeadlineAtMillis,
      );
    }
    const { present, shallow } = yield* runGit(
      ["rev-parse", "--is-shallow-repository", "--verify", `${commit}^{commit}`],
      { requireCompleteOutput: true, inspect: true },
    ).pipe(
      Effect.map((output) => ({ present: true, shallow: output.split("\n")[0] === "true" })),
      Effect.catchTag("GitCommandError", (error) =>
        error.exitCode === 128
          ? runGit(["rev-parse", "--is-shallow-repository"], {
              requireCompleteOutput: true,
              inspect: true,
            }).pipe(Effect.map((output) => ({ present: false, shallow: output.trim() === "true" })))
          : Effect.fail(error),
      ),
    );

    // A depth-1 fetch would make a full repository shallow again.
    const includeHistory = !shallow || options?.includeHistory === true;
    // Local hits can follow an attempt interrupted after importing the objects.
    const cleanup = (yield* SessionSandbox).remove(GIT_TRANSPORT_DIR, {
      recursive: true,
      force: true,
    });

    if (present && (!includeHistory || !shallow)) return yield* cleanup;

    const head = (yield* runGit(["rev-parse", "HEAD"], { requireCompleteOutput: true })).trim();
    const unshallow = includeHistory && shallow;
    const revisions = unshallow ? [head, commit] : [commit];

    // Repairs need ancestry to diagnose changes such as reused migration ids.
    // Include HEAD when unshallowing, including when the requested revision diverged.
    // Credentials remain outside the worker-controlled Git configuration.
    yield* resetGitTransportDirectory();
    yield* runGit(["init", "--bare", GIT_TRANSPORT_REPOSITORY], {
      cwd: GIT_TRANSPORT_DIR,
      env: isolatedGitTransportEnv,
      requireCompleteOutput: true,
    });
    if (!unshallow) {
      const sandbox = yield* SessionSandbox;

      // Borrow immutable objects, never the checkout's config or credentials.
      // A shallow receiver must also lend its boundary so negotiation cannot
      // advertise ancestry that it does not have.
      yield* sandbox.writeFile(
        `${GIT_TRANSPORT_REPOSITORY}/objects/info/alternates`,
        `${repositoryDirectory}/.git/objects\n`,
      );
      if (shallow)
        yield* sandbox.writeFile(
          `${GIT_TRANSPORT_REPOSITORY}/shallow`,
          yield* sandbox.readFileString(`${repositoryDirectory}/.git/shallow`),
        );
    }
    yield* Effect.annotateCurrentSpan({
      "repository.git.fetch": unshallow ? "full-history" : "incremental",
      "repository.git.shallow": shallow,
    });
    const remote = remoteFor(forkName);

    yield* withRepoToken(
      forkName,
      "read",
      (deadlineAt) =>
        runGit(
          [
            "fetch",
            ...(includeHistory ? [] : ["--depth", "1"]),
            ...(!unshallow ? ["--negotiation-tip", head] : []),
            "--no-tags",
            remote,
            ...revisions,
          ],
          {
            cwd: GIT_TRANSPORT_DIR,
            env: { ...gitTransportEnv(remote), GIT_DIR: GIT_TRANSPORT_REPOSITORY },
            requireCompleteOutput: true,
            deadlineAt,
          },
        ),
      options?.repositoryId,
    ).pipe(
      // Only the isolated read is retried; each attempt owns a fresh scoped token.
      Effect.retry({
        times: GIT_READ_RETRY_COUNT,
        while: (error) =>
          error._tag === "GitCommandError" &&
          error.status !== undefined &&
          [408, 429, 500, 502, 503, 504].includes(error.status),
        schedule: Schedule.exponential("250 millis").pipe(
          Schedule.jittered,
          Schedule.tap(({ input }) =>
            Effect.logWarning(
              "Artifacts history read attempt failed",
              sanitizeCause(Cause.fail(input), { operation: "ArtifactsCheckout.ensureCommit" }),
            ),
          ),
        ),
      }),
    );
    yield* runGit(
      [
        "fetch",
        "--no-tags",
        ...(unshallow ? ["--unshallow"] : ["--update-shallow"]),
        GIT_TRANSPORT_REPOSITORY,
        ...revisions,
      ],
      {
        env: { ...isolatedGitTransportEnv, GIT_ALLOW_PROTOCOL: "file" },
        requireCompleteOutput: true,
      },
    );
    yield* runGit(["cat-file", "-e", `${commit}^{commit}`], { requireCompleteOutput: true });
    // The receiving repository owns the imported objects. Do not snapshot a
    // second Git object store or leave alternates attached to a disposable fetch.
    yield* cleanup;
  });

  const deleteFork = Effect.fn("ArtifactsCheckout.deleteFork")(function* (forkName: string) {
    return yield* artifacts.delete(forkName).pipe(
      boundedArtifacts("deleteFork"),
      Effect.catchTag("ArtifactsOperationError", (error) =>
        error.code === "NOT_FOUND" ? Effect.succeed(false) : Effect.fail(error),
      ),
    );
  });

  const deletePlannedFork = Effect.fn("ArtifactsCheckout.deletePlannedFork")(function* (
    intent: StoredRepositoryFork,
  ) {
    return yield* Effect.gen(function* () {
      // The provider exposes deletion by name. Fence it with retained generation
      // (or the exact creation fingerprint when its acknowledgement was lost).
      yield* plannedForkGeneration(intent);

      return yield* artifacts.delete(intent.forkName).pipe(boundedArtifacts("deletePlannedFork"));
    }).pipe(
      Effect.catchCauseIf(
        (cause) =>
          cause.reasons.length > 0 &&
          cause.reasons.every(
            (reason) =>
              Cause.isFailReason(reason) &&
              reason.error._tag === "ArtifactsOperationError" &&
              reason.error.code === "NOT_FOUND",
          ),
        () => Effect.succeed(false),
      ),
    );
  });

  return {
    ensureFork,
    retainFork,
    ensurePlannedFork,
    readSourcePin,
    cloneSourceIntoSandbox,
    deletePlannedFork,
    sourceGeneration,
    cloneIntoSandbox: (...args: Parameters<typeof cloneIntoSandbox>) =>
      cloneIntoSandbox(...args).pipe(Effect.provideService(SecureRandom.SecureRandom, random)),
    discardCloneReceipt,
    cloneGenerationIntoSandbox: (...args: Parameters<typeof cloneGenerationIntoSandbox>) =>
      cloneGenerationIntoSandbox(...args).pipe(
        Effect.provideService(SecureRandom.SecureRandom, random),
      ),
    readGenerationMainHead: (generation: ArtifactsRepositoryGeneration) =>
      mainHead(generation.repositoryName, generation.repositoryId),
    retargetOrigin,
    checkoutCommit,
    restoreSnapshotCheckout,
    restorePreparedRepository,
    persist,
    persistGeneration,
    pushPreparedGeneration,
    ensureCommit,
    deleteFork,
    remoteFor,
  };
});

/** Explicit public contract of {@link ArtifactsCheckout}. */
export interface ArtifactsCheckoutService {
  readonly retainFork: Effect.Success<typeof make>["retainFork"];
  readonly ensurePlannedFork: Effect.Success<typeof make>["ensurePlannedFork"];
  /** An optional absolute deadline permits one read-timeout retry against the same generation. */
  readonly readSourcePin: Effect.Success<typeof make>["readSourcePin"];
  readonly cloneSourceIntoSandbox: Effect.Success<typeof make>["cloneSourceIntoSandbox"];
  readonly deletePlannedFork: Effect.Success<typeof make>["deletePlannedFork"];
  readonly sourceGeneration: (
    repositoryName: string,
  ) => Effect.Effect<
    ArtifactsRepositoryGeneration,
    | Artifacts.ArtifactsOperationError
    | ArtifactsOperationTimeoutError
    | ArtifactsRepositoryGenerationMismatchError
  >;
  readonly restoreSnapshotCheckout: (
    repositoryName: string,
    head: string,
    checkpointHead: string,
  ) => Effect.Effect<void, GitCommandError | Sandbox.SandboxOperationError, SessionSandbox>;
  /** Makes a saved commit available for review without changing the checkout's head or files. */
  readonly ensureCommit: (
    forkName: string,
    commit: string,
    options?: {
      readonly includeHistory?: boolean;
      readonly repositoryId?: string;
      /** Absolute host budget for one pinned-read retry; omission keeps reads single-attempt. */
      readonly retryDeadlineAtMillis?: number;
    },
  ) => Effect.Effect<
    void,
    | Artifacts.ArtifactsOperationError
    | ArtifactsOperationTimeoutError
    | ArtifactsRepositoryGenerationMismatchError
    | GitCommandError
    | Sandbox.SandboxOperationError,
    SessionSandbox
  >;
  /** Finds or provisions the session's writable fork and verifies its head. */
  readonly ensureFork: (
    forkName: string,
    seedRepository?: string,
    policy?: ArtifactsForkPolicy,
  ) => Effect.Effect<
    EnsureForkResult,
    | RepositorySourceRequiredError
    | Artifacts.ArtifactsOperationError
    | ArtifactsOperationTimeoutError
    | ForkNotReadyError
  >;
  /** Forget preparation only after the caller has destroyed its dedicated sandbox. */
  readonly discardCloneReceipt: (
    scope: string,
  ) => Effect.Effect<void, SessionStateStoreError, SessionStateStore>;
  /** Clones the fork, or verified seed content for a pristine workspace. */
  readonly cloneIntoSandbox: (
    forkName: string,
    scope?: string,
    bootstrap?: {
      readonly source: ArtifactsRepositoryGeneration;
      readonly forkHead: string;
    },
  ) => Effect.Effect<
    { readonly head: string },
    | Artifacts.ArtifactsOperationError
    | ArtifactsOperationTimeoutError
    | ArtifactsRepositoryGenerationMismatchError
    | GitCommandError
    | Sandbox.SandboxOperationError
    | SessionStateStoreError
    | HarnessTurnError,
    SessionSandbox | SessionStateStore
  >;
  /** Clones only while the name resolves to the authorized provider generation. */
  readonly cloneGenerationIntoSandbox: (
    generation: ArtifactsRepositoryGeneration,
  ) => Effect.Effect<
    { readonly head: string },
    | Artifacts.ArtifactsOperationError
    | ArtifactsOperationTimeoutError
    | ArtifactsRepositoryGenerationMismatchError
    | GitCommandError
    | Sandbox.SandboxOperationError
    | SessionStateStoreError
    | HarnessTurnError,
    SessionSandbox | SessionStateStore
  >;
  /** Reads main only while the repository name still resolves to this generation. */
  readonly readGenerationMainHead: (
    generation: ArtifactsRepositoryGeneration,
  ) => Effect.Effect<
    string,
    | Artifacts.ArtifactsOperationError
    | ArtifactsOperationTimeoutError
    | ArtifactsRepositoryGenerationMismatchError
    | ForkNotReadyError
  >;
  /** Point a seed clone at its writable session fork without persisting a
   * credential. The caller first proves both repositories share a head. */
  readonly retargetOrigin: (
    forkName: string,
  ) => Effect.Effect<void, GitCommandError | Sandbox.SandboxOperationError, SessionSandbox>;
  /** Starts a host-pinned turn at an available commit without changing the durable fork head. */
  readonly checkoutCommit: (
    commit: string,
  ) => Effect.Effect<void, GitCommandError | Sandbox.SandboxOperationError, SessionSandbox>;
  /** Restores the exact candidate from a host-owned bare transport archive, without credentials. */
  readonly restorePreparedRepository: (
    candidate: PersistResult,
    transportDirectory: string,
  ) => Effect.Effect<void, GitCommandError | Sandbox.SandboxOperationError, SessionSandbox>;
  /** Commits residual changes, pushes, and verifies the pushed head. */
  readonly persist: (
    forkName: string,
    verifiedHead: string,
    sourceKey: string,
    repositoryId?: string,
  ) => Effect.Effect<
    PersistResult,
    | Artifacts.ArtifactsOperationError
    | ArtifactsOperationTimeoutError
    | ArtifactsRepositoryGenerationMismatchError
    | ForkNotReadyError
    | GitCommandError
    | NonFastForwardPushError
    | PersistedPathsLimitError
    | PushVerificationError
    | Sandbox.SandboxOperationError
    | RepositorySetupError
    | RepositoryPersistenceCheckpointError,
    SessionSandbox
  >;
  /** Persists only through a token minted by the authorized provider generation. */
  readonly persistGeneration: <E, R>(
    generation: ArtifactsRepositoryGeneration,
    verifiedHead: string,
    beforePush?: BeforeGenerationPush<E, R>,
  ) => Effect.Effect<
    PersistResult,
    | Artifacts.ArtifactsOperationError
    | ArtifactsOperationTimeoutError
    | ArtifactsRepositoryGenerationMismatchError
    | ForkNotReadyError
    | GitCommandError
    | NonFastForwardPushError
    | PersistedPathsLimitError
    | PushVerificationError
    | Sandbox.SandboxOperationError
    | E,
    SessionSandbox | R
  >;
  /** Resumes one already-staged exact generation push without touching the working tree. */
  readonly pushPreparedGeneration: (
    generation: ArtifactsRepositoryGeneration,
    verifiedHead: string,
    preparedHead: string,
  ) => Effect.Effect<
    PersistResult,
    | Artifacts.ArtifactsOperationError
    | ArtifactsOperationTimeoutError
    | ArtifactsRepositoryGenerationMismatchError
    | ForkNotReadyError
    | NonFastForwardPushError
    | PersistedPathsLimitError
    | PreparedCommitUnavailableError
    | PushVerificationError
    | GitCommandError
    | Sandbox.SandboxOperationError,
    SessionSandbox
  >;
  /** Deletes the session fork; `false` when it never existed. */
  readonly deleteFork: (
    forkName: string,
  ) => Effect.Effect<boolean, Artifacts.ArtifactsOperationError | ArtifactsOperationTimeoutError>;
  /** Artifacts Git remote URL for a repository name. */
  readonly remoteFor: (forkName: string) => string;
}

/**
 * Internal service that owns the seed/fork Git lifecycle. Its methods mint
 * short-lived repository tokens only around the Git process that consumes
 * them and verify every claimed durable write through the Artifacts binding.
 */
export class ArtifactsCheckout extends Context.Service<
  ArtifactsCheckout,
  ArtifactsCheckoutService
>()("sandbox-agent/ArtifactsCheckout") {
  static readonly layer = Layer.effect(ArtifactsCheckout)(
    Effect.map(make, (service) => ArtifactsCheckout.of(service)),
  );
}
