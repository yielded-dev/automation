import { Cause, DateTime, Effect, Option, Redacted, Schedule, Schema } from "effect";
import { Base64 } from "effect/encoding";
import { SqlClient } from "effect/sql";

import { SandboxTaskSeedRepository } from "./sandbox-contracts/sandbox-task-admission";
import { SandboxTaskMachineCreate } from "./sandbox-contracts/sandbox-task-machine";
import { RunConfig } from "./RunConfig";
import { ConflictError } from "../domain/errors";
import { RunState, type RunLaunch } from "../domain/RepairExecution";
import { RepairSandboxTaskClient } from "./RepairSandboxTaskClient";
import { SandboxAgentArtifacts } from "@yielded/sandbox/cloudflare/Bindings";
import { transferRepositoryGit } from "@yielded/sandbox/repository/RepositoryGitTransfer";
import {
  RepositoryMutationFence,
  RepositoryMutationFenceFailure,
} from "@yielded/sandbox/repository/RepositoryMutationFence";
import { GithubAppAuth } from "./GithubAppAuth";

interface RepairSourcePreparation {
  readonly source: typeof SandboxTaskSeedRepository.Type;
  readonly missing: ReadonlyArray<string>;
}

/** Resolve read authority before acquiring a preparation sandbox or changing source refs. */
export const readRepairSource = Effect.fn("RepairSource.read")(function* (input: RunLaunch) {
  const artifacts = yield* SandboxAgentArtifacts;
  const continuation = input.launch.continuation;
  const head = continuation?.candidateSha ?? input.launch.rebase?.headSha ?? input.launch.targetSha;
  let repositoryName = input.launch.seedRepository;
  let expectedGeneration: string | undefined;

  if (continuation !== undefined) {
    const sql = (yield* SqlClient.SqlClient).withoutTransforms();
    const previous = (yield* sql<{
      intent: string;
      state: string;
      admission_fingerprint: string | null;
    }>`SELECT receipt.intent, execution.state, execution.admission_fingerprint
      FROM repair_execution_receipts receipt JOIN repair_executions execution ON execution.run_id = receipt.run_id
      WHERE receipt.run_id = ${continuation.runId} AND receipt.kind = 'sandbox-admission' AND receipt.key = 'launch'`)[0];

    if (previous === undefined)
      return yield* ConflictError.make({
        message: "The continuation has no retained repository authority",
      });
    const admitted = yield* Schema.decodeEffect(Schema.fromJsonString(SandboxTaskMachineCreate))(
      previous.intent,
    );
    const state = yield* Schema.decodeEffect(Schema.fromJsonString(RunState))(previous.state);

    if (
      admitted.id !== continuation.runId ||
      state.runId !== continuation.runId ||
      state.outcome !== "completed" ||
      state.origin.repository !== input.origin.repository ||
      state.launch.githubRepositoryId !== input.launch.githubRepositoryId ||
      state.origin.incidentId !== input.origin.incidentId ||
      admitted.repair?.repositoryId !== state.origin.repository ||
      admitted.repair.repairId !== state.origin.incidentId ||
      admitted.repair.attemptId !== state.origin.attemptId ||
      admitted.repair.evidenceHash !== state.origin.evidenceHash ||
      (admitted.source._tag !== "AttachedRepository" && admitted.source._tag !== "SeedRepository")
    )
      return yield* ConflictError.make({ message: "The continuation lineage does not match" });
    const detail = yield* (yield* RepairSandboxTaskClient).inspect(continuation.runId);
    const saved = detail?.savedCheckpoint;
    const association = detail?.launchAssociation;
    const requestFingerprint = detail?.requestFingerprint ?? previous.admission_fingerprint;

    if (
      detail === null ||
      detail.id !== continuation.runId ||
      requestFingerprint === null ||
      (previous.admission_fingerprint !== null &&
        requestFingerprint !== previous.admission_fingerprint) ||
      detail.activeTurnId !== null ||
      association?._tag !== "Repair" ||
      association.repositoryId !== admitted.repair.repositoryId ||
      association.repairId !== admitted.repair.repairId ||
      association.attemptId !== admitted.repair.attemptId ||
      association.evidenceHash !== admitted.repair.evidenceHash ||
      association.proofPolicyHash !== admitted.repair.proofPolicyHash ||
      saved === undefined ||
      saved.commitSha !== head ||
      saved.baseHead !== admitted.source.commitSha ||
      (admitted.source._tag === "AttachedRepository"
        ? saved.repositoryName !== admitted.source.repositoryName ||
          saved.artifactsRepositoryId !== admitted.source.artifactsRepositoryId
        : saved.repositoryName === admitted.source.repositoryName)
    )
      return yield* ConflictError.make({
        message: "The continuation private checkpoint does not match",
      });
    // A lost create reply can leave a completed native run unconfirmed in PG.
    // A legacy inspection cannot confirm a lost reply without its fingerprint.
    // Bind authenticated inspection without replaying the retired launch.
    if (previous.admission_fingerprint === null) {
      const confirmed = yield* sql`UPDATE repair_executions
        SET admission_fingerprint = ${requestFingerprint}
        WHERE run_id = ${continuation.runId}
          AND (admission_fingerprint IS NULL OR admission_fingerprint = ${requestFingerprint})
        RETURNING run_id`;

      if (confirmed.length !== 1)
        return yield* ConflictError.make({ message: "The continuation admission conflicts" });
    }
    repositoryName = saved.repositoryName;
    expectedGeneration = saved.artifactsRepositoryId;
  }

  const repository = yield* artifacts.get(repositoryName).pipe(
    Effect.catchTag("ArtifactsOperationError", (error) =>
      continuation === undefined && error.code === "NOT_FOUND"
        ? artifacts.create(repositoryName).pipe(
            Effect.catchTag("ArtifactsOperationError", (creation) =>
              creation.code === "ALREADY_EXISTS" ? Effect.void : Effect.fail(creation),
            ),
            Effect.andThen(artifacts.get(repositoryName)),
          )
        : Effect.fail(error),
    ),
  );
  const generation = repository.id;

  if (expectedGeneration !== undefined && generation !== expectedGeneration)
    return yield* ConflictError.make({ message: "The continuation repository generation changed" });
  if (
    continuation !== undefined &&
    (yield* repository.log({ ref: "main", limit: 1 }))[0]?.hash !== head
  )
    return yield* ConflictError.make({ message: "The continuation repository head changed" });
  const revisions = [
    ...new Set([
      input.origin.offendingSha,
      input.launch.targetSha,
      ...(input.launch.rebase ? [input.launch.rebase.headSha] : []),
      ...(continuation ? [continuation.candidateSha] : []),
    ]),
  ];
  const missing = yield* Effect.filter(revisions, (sha) =>
    Effect.gen(function* () {
      const ref = continuation !== undefined && sha === head ? "main" : `repair-source/${sha}`;
      const retained = (yield* repository.log({ ref, limit: 1 }))[0];

      if (retained === undefined) return true;
      if (retained.hash !== sha)
        return yield* ConflictError.make({ message: "The retained repair source pin changed" });
      yield* repository.readCommit(sha);

      return false;
    }).pipe(
      Effect.catchCause((cause) =>
        cause.reasons.length > 0 &&
        cause.reasons.every(
          (reason) =>
            Cause.isFailReason(reason) &&
            reason.error._tag === "ArtifactsOperationError" &&
            reason.error.code === "NOT_FOUND",
        )
          ? Effect.succeed(true)
          : Effect.failCause(cause),
      ),
    ),
  );

  if ((yield* repository.info()).id !== generation)
    return yield* ConflictError.make({ message: "The repair source generation changed" });
  const source = yield* Schema.decodeEffect(SandboxTaskSeedRepository)({
    _tag: "SeedRepository",
    repositoryName,
    artifactsRepositoryId: generation,
    commitSha: head,
    artifactsRef:
      continuation === undefined ? `refs/heads/repair-source/${head}` : "refs/heads/main",
  });

  return { source, missing } satisfies RepairSourcePreparation;
});

/** Append missing immutable pins; the kernel alone creates the new run's private repository. */
export const prepareRepairSource = Effect.fn("RepairSource.prepare")(function* (
  input: RunLaunch,
  preparation: RepairSourcePreparation,
) {
  const config = yield* RunConfig;
  const outerFence = yield* RepositoryMutationFence;
  const { source, missing } = preparation;
  const repository = yield* (yield* SandboxAgentArtifacts).get(source.repositoryName);
  const verifyGeneration = Effect.gen(function* () {
    if ((yield* repository.info()).id !== source.artifactsRepositoryId)
      return yield* ConflictError.make({ message: "The repair source generation changed" });
    if (
      source.artifactsRef === "refs/heads/main" &&
      (yield* repository.log({ ref: "main", limit: 1 }))[0]?.hash !== source.commitSha
    )
      return yield* ConflictError.make({ message: "The continuation repository head changed" });
  });
  const mutationFence = RepositoryMutationFence.of({
    check: outerFence.check.pipe(
      Effect.andThen(
        verifyGeneration.pipe(
          Effect.catchCause((cause) =>
            Effect.failCause(
              Cause.map(cause, (error) =>
                RepositoryMutationFenceFailure.make({
                  reason: Schema.is(ConflictError)(error) ? "stopped" : "unavailable",
                  message: "The repair source generation is unavailable",
                  cause: error,
                }),
              ),
            ),
          ),
        ),
      ),
    ),
  });
  const tokenTtl = Effect.map(DateTime.now, (now) =>
    Math.min(
      31_536_000,
      Math.max(60, Math.ceil((input.origin.deadlineAtMs - DateTime.toEpochMillis(now)) / 1_000)),
    ),
  );
  const sourceFor = Effect.fn("RepairSource.sourceFor")(function* (commitSha: string) {
    const retained = yield* repository.readCommit(commitSha).pipe(
      Effect.as(true),
      Effect.catchCause((cause) =>
        cause.reasons.length > 0 &&
        cause.reasons.every(
          (reason) => Cause.isFailReason(reason) && reason.error.code === "NOT_FOUND",
        )
          ? Effect.succeed(false)
          : Effect.failCause(cause),
      ),
    );

    if (retained) {
      const token = yield* Effect.acquireRelease(
        repository.createToken("read", yield* tokenTtl),
        (token) => repository.revokeToken(token.id).pipe(Effect.catchCause(Effect.die)),
      );

      return {
        url: `https://${config.accountId}.artifacts.cloudflare.net/git/${config.artifactsNamespace}/${source.repositoryName}.git`,
        authorization: Redacted.make(`Bearer ${token.plaintext}`),
      };
    }
    const app = yield* GithubAppAuth;
    const installation = yield* app.repositoryInstallation(
      `https://github.com/${input.origin.repository}`,
    );

    if (
      Option.isNone(installation) ||
      String(installation.value.id) !== config.githubInstallationId ||
      installation.value.suspended_at !== null
    )
      return yield* ConflictError.make({ message: "Repair source installation does not match" });
    const token = yield* app.repositoryReadToken(
      input.launch.githubRepositoryId,
      "source",
      config.githubInstallationId,
    );

    return {
      url: `https://github.com/${input.origin.repository}.git`,
      authorization: Redacted.make(
        `Basic ${Base64.encode(`x-access-token:${Redacted.value(token)}`)}`,
      ),
    };
  });
  const copyCommit = Effect.fn("RepairSource.copyCommit")(
    function* (commitSha: string) {
      yield* outerFence.check;
      yield* verifyGeneration;
      const from = yield* sourceFor(commitSha);
      const token = yield* Effect.acquireRelease(
        repository.createToken("write", yield* tokenTtl),
        (token) => repository.revokeToken(token.id).pipe(Effect.catchCause(Effect.die)),
      );

      yield* verifyGeneration;
      yield* transferRepositoryGit({
        source: from,
        target: {
          url: `https://${config.accountId}.artifacts.cloudflare.net/git/${config.artifactsNamespace}/${source.repositoryName}.git`,
          authorization: Redacted.make(`Bearer ${token.plaintext}`),
        },
        commitSha,
        branch: `repair-source/${commitSha}`,
        requireEmptyTarget: false,
      }).pipe(
        Effect.provideService(RepositoryMutationFence, mutationFence),
        Effect.withSpan("RepairSource.gitTransfer"),
      );
      yield* verifyGeneration;
      if (
        (yield* repository.log({ ref: `repair-source/${commitSha}`, limit: 1 }))[0]?.hash !==
        commitSha
      )
        return yield* ConflictError.make({
          message: "The retained repair source pin does not match",
        });
      yield* repository.readCommit(commitSha);
    },
    Effect.scoped,
    Effect.sandbox,
    Effect.retry({
      while: (cause) =>
        cause.reasons.length > 0 &&
        cause.reasons.every(
          (reason) =>
            Cause.isFailReason(reason) &&
            reason.error._tag === "RepositoryGitTransferFailure" &&
            (reason.error.reason === "unavailable" || reason.error.reason === "uncertain"),
        ),
      times: 5,
      schedule: Schedule.min([Schedule.exponential("2 seconds"), Schedule.spaced("30 seconds")]),
    }),
    Effect.catch(Effect.failCause),
  );

  for (const commit of missing) yield* copyCommit(commit);
  yield* verifyGeneration;
});
