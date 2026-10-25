import * as Crypto from "effect/Crypto";
import {
  Context,
  DateTime,
  Effect,
  Layer,
  Schema,
  SchemaTransformation,
  Semaphore,
  type Scope,
} from "effect";
import { DurableObject, DurableObjectAlarm, DurableObjectState } from "effect-cf";
import { reportSanitizedCause } from "../observability/RuntimeDiagnostics";
import { Sandboxes } from "./Bindings";
import { SandboxAgentConfig } from "./Config";
import { OutboundAuthority } from "./OutboundAuthority";
import { WorkspaceCheckpoint } from "./NativeSandbox";
import {
  isTerminalWorkspaceBaselinePreparationCause,
  prepareWorkspaceBaseline,
} from "./WorkspaceBaselinePreparation";
import { WorkspaceBaselinePreparationResult } from "./WorkspaceBaselinePreparationState";
import {
  WORKSPACE_PREPARATION_VERSION,
  WorkspaceBaselineRepository,
} from "./WorkspaceBaselineRepository";
import { resourceHash } from "./resource-names";

export const RepositoryPreparationSource = Schema.Struct({
  repositoryName: Schema.NonEmptyString,
  repositoryId: Schema.NonEmptyString,
  commitSha: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/u)),
  artifactsRef: Schema.optionalKey(Schema.NonEmptyString),
});

export class RepositoryPreparationError extends Schema.TaggedError<RepositoryPreparationError>()(
  "RepositoryPreparationError",
  {
    reason: Schema.Literals([
      "unauthorized",
      "owner-mismatch",
      "source-unavailable",
      "unavailable",
    ]),
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/** Hosts authorize repository keys and retain exact source generations; events carry no authority. */
export class RepositoryPreparationSources extends Context.Service<
  RepositoryPreparationSources,
  {
    readonly authorize: (repository: string) => Effect.Effect<void, RepositoryPreparationError>;
    readonly select: (
      repository: string,
    ) => Effect.Effect<
      typeof RepositoryPreparationSource.Type,
      RepositoryPreparationError,
      Scope.Scope
    >;
  }
>()("@reve/repairs/RepositoryPreparationSources") {
  static readonly unconfiguredLayer = Layer.succeed(this, {
    authorize: () => Effect.fail(RepositoryPreparationError.make({ reason: "unauthorized" })),
    select: () => Effect.fail(RepositoryPreparationError.make({ reason: "unauthorized" })),
  });
}

export const RepositoryPreparationRequest = Schema.Struct({
  repository: Schema.NonEmptyString,
  source: RepositoryPreparationSource,
});

const samePreparation = Schema.toEquivalence(RepositoryPreparationRequest);

const Completion = Schema.Struct({
  status: Schema.Literals(["prepared", "reused", "disabled", "expired", "superseded"]),
  repository: Schema.optionalKey(Schema.String),
  checkpoint: Schema.optionalKey(WorkspaceCheckpoint),
  classIdentity: Schema.String,
  imageDigest: Schema.String,
  /** Older receipts have no configuration identity and are revalidated by a new generation. */
  configurationFingerprint: Schema.optionalKey(Schema.String),
});

// The shape and key also decode coordinators deployed before this implementation moved upstream.
export const RepositoryPreparationState = Schema.Struct({
  request: RepositoryPreparationRequest,
  generation: Schema.String.check(Schema.isUUID()),
  completed: Schema.NullOr(Completion),
});

export const RepositoryPreparationExecution = Schema.Struct({
  request: RepositoryPreparationRequest,
  generation: Schema.String.check(Schema.isUUID()),
});

export const RepositoryPreparationReply = Schema.Union([
  WorkspaceBaselinePreparationResult,
  Schema.Struct({ status: Schema.Literal("unavailable") }),
  Schema.Struct({ status: Schema.Literal("superseded") }),
]);

// Original host methods return undefined; the first typed release returns JSON null.
// Read either during rolling upgrades, while retaining the typed release's wire output.
const PreparationWakeReply = Schema.NullOr(Schema.Void).pipe(
  Schema.decodeTo(
    Schema.Void,
    SchemaTransformation.transform<void, void | null>({
      decode: () => undefined,
      encode: () => null,
    }),
  ),
);

/** The RPC wire contract stays compatible with existing host-owned preparation objects. */
export const repositoryPreparationMethods = {
  requestRepositoryPreparation: DurableObject.method({
    args: [Schema.NonEmptyString],
    success: DurableObject.native(PreparationWakeReply),
  }),
  executeRepositoryPreparation: DurableObject.method({
    args: [RepositoryPreparationExecution],
    success: RepositoryPreparationReply,
  }),
};

export class RepositoryPreparationObject extends DurableObject.Tag<RepositoryPreparationObject>()(
  "@reve/repairs/RepositoryPreparationObject",
  repositoryPreparationMethods,
) {}

export class RepositoryPreparationGate extends Context.Service<
  RepositoryPreparationGate,
  Semaphore.Semaphore
>()("@reve/repairs/RepositoryPreparationGate") {
  static readonly layer = Layer.effect(this, Semaphore.make(1));
}

export const repositoryPreparationName = Effect.fnUntraced(function* (repository: string) {
  return `repository-preparation:${yield* resourceHash(JSON.stringify([repository]))}`;
});

export const isRepositoryPreparationOwner = (name: string | undefined) =>
  name?.startsWith("repository-preparation:") === true;

export const isRepositoryPreparationCoordinator = (name: string | undefined) =>
  isRepositoryPreparationOwner(name) && name?.split(":").length === 2;

const preparationOwner = Effect.fnUntraced(function* (repository: string, generation?: string) {
  yield* (yield* RepositoryPreparationSources).authorize(repository);
  const state = yield* DurableObjectState.DurableObjectState;
  const name = yield* repositoryPreparationName(repository);
  if (state.id.name !== (generation === undefined ? name : `${name}:${generation}`))
    return yield* RepositoryPreparationError.make({ reason: "owner-mismatch" });
  return name;
});

/** Coalesce source events in the existing durable alarm lane. */
export const scheduleRepositoryPreparation = Effect.fn("RepositoryPreparation.schedule")(function* (
  repository: string,
) {
  yield* preparationOwner(repository);
  yield* (yield* DurableObjectAlarm.DurableObjectAlarm).scheduleAlarm({
    tag: "repository-preparation",
    id: "main",
    payload: repository,
    runAt: yield* DateTime.now,
  });
});

/** Call from a durable host event consumer so a rolling-upgrade RPC failure can retry. */
export const requestRepositoryPreparation = Effect.fn("RepositoryPreparation.request")(function* (
  repository: string,
) {
  const name = yield* repositoryPreparationName(repository);
  const owner = (yield* RepositoryPreparationObject).byName(name);
  yield* owner.requestRepositoryPreparation(repository);
});

const prepareRepository = Effect.fn("RepositoryPreparation.prepare")(function* (
  repository: string,
) {
  const name = yield* preparationOwner(repository);
  const state = yield* DurableObjectState.DurableObjectState;
  const sources = yield* RepositoryPreparationSources;
  const saved = yield* state.storage.get("repositoryPreparation");
  let execution =
    saved === undefined
      ? undefined
      : yield* Schema.decodeUnknownEffect(RepositoryPreparationState)(saved);
  const resumed = execution?.completed === null;
  const description = yield* (yield* Sandboxes.get("repository-preparation-policy")).describe;
  const config = yield* SandboxAgentConfig;
  const configurationFingerprint = yield* resourceHash(
    JSON.stringify([
      config.repositorySetupPath,
      config.artifactsHostname,
      config.artifactsNamespace,
      config.sandboxNamespace ?? null,
      WORKSPACE_PREPARATION_VERSION,
    ]),
  );

  if (execution === undefined || execution.completed !== null) {
    const source = yield* sources.select(repository);
    const request = { repository, source };
    if (execution !== undefined && execution.completed !== null) {
      const completed = execution.completed;
      if (
        samePreparation(execution.request, request) &&
        completed.classIdentity === description.classIdentity &&
        completed.imageDigest === description.imageDigest &&
        completed.configurationFingerprint === configurationFingerprint
      ) {
        if (completed.status === "disabled") return;
        if (completed.repository !== undefined && completed.checkpoint !== undefined) {
          const baseline = yield* (yield* WorkspaceBaselineRepository).find({
            repository: completed.repository,
            classIdentity: description.classIdentity,
            imageDigest: description.imageDigest,
            head: source.commitSha,
            setupFingerprint: completed.checkpoint.setupFingerprint,
          });
          if (baseline?.checkpoint.head === source.commitSha) return;
        }
      }
    }
    execution = {
      request,
      generation: yield* (yield* Crypto.Crypto).randomUUIDv4().pipe(Effect.orDie),
      completed: null,
    };
    yield* state.storage.put("repositoryPreparation", execution);
  }
  const { request, generation } = execution;
  const owner = (yield* RepositoryPreparationObject).byName(`${name}:${generation}`);
  const result = yield* owner.executeRepositoryPreparation({ request, generation });
  if (result.status === "unavailable")
    return yield* RepositoryPreparationError.make({ reason: "unavailable" });
  const baseline = result.status === "superseded" ? null : result.baseline;
  if (
    (result.status === "prepared" || result.status === "reused") &&
    baseline?.checkpoint.head !== request.source.commitSha
  )
    return yield* RepositoryPreparationError.make({ reason: "unavailable" });
  execution = {
    request,
    generation,
    completed: {
      status: result.status,
      configurationFingerprint,
      classIdentity: baseline?.checkpoint.classIdentity ?? description.classIdentity,
      imageDigest: baseline?.checkpoint.imageDigest ?? description.imageDigest,
      ...(baseline === null
        ? {}
        : { repository: baseline.repository, checkpoint: baseline.checkpoint }),
    },
  };
  yield* state.storage.put("repositoryPreparation", execution);

  // Finish a retained receipt before following the event that woke it. Compare
  // the complete source generation, including replacement at an unchanged SHA.
  if (
    resumed &&
    !samePreparation(request, { repository, source: yield* sources.select(repository) })
  )
    yield* scheduleRepositoryPreparation(repository);
});

export const runRepositoryPreparationAlarms = Effect.gen(function* () {
  const gate = yield* RepositoryPreparationGate;
  const alarms = yield* DurableObjectAlarm.DurableObjectAlarm;
  return yield* alarms.deferWakes(
    DurableObjectAlarm.define({ "repository-preparation": Schema.NonEmptyString }).handlers(
      {
        "repository-preparation": ({ payload }) =>
          prepareRepository(payload).pipe(
            gate.withPermits(1),
            Effect.tapCause((cause) =>
              reportSanitizedCause(cause, { operation: "RepositoryPreparation.prepare" }),
            ),
          ),
      },
      { retryFailedAfter: "30 seconds" },
    ),
  );
});

export const executeRepositoryPreparation = Effect.fn("RepositoryPreparation.execute")(function* (
  input: typeof RepositoryPreparationExecution.Type,
) {
  const gate = yield* RepositoryPreparationGate;
  const name = yield* preparationOwner(input.request.repository, input.generation);
  return yield* prepareWorkspaceBaseline({
    sessionId: `${name}:${input.generation}`,
    turnId: input.generation,
    repositoryId: input.request.repository,
    source: input.request.source,
  }).pipe(
    gate.withPermits(1),
    Effect.provide(OutboundAuthority.noneLayer),
    Effect.catchCauseIf(isTerminalWorkspaceBaselinePreparationCause, (cause) =>
      reportSanitizedCause(cause, { operation: "RepositoryPreparation.execute" }).pipe(
        Effect.as({ status: "superseded" as const }),
      ),
    ),
  );
});
