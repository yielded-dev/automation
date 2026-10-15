import { fixtureArtifacts } from "../../../../test/fixtures/artifacts.worker";
import { Cause, Clock, Context, Effect, Layer, Option, Ref, Schema, Tracer } from "effect";
import {
  Artifacts,
  CloudflareTracer,
  DurableObject,
  DurableObjectState as ObjectState,
  WorkerEnvironment,
} from "effect-cf";
import {
  makeSandboxAgentContainer,
  makeSandboxAgentOutbound,
  decodeNativeReply,
  makeNativeSandbox,
  NativeSandboxNamespace,
  type RepositoryAuthorizationGrant,
  SessionSandbox,
  ArtifactsCheckout,
  SandboxAgentArtifacts,
  Sandboxes,
  SandboxAgentConfig,
  AgentEventSink,
  OutboundAuthority,
  SessionStateStore,
  sandboxWorkspaceLayer,
  RepositoryInstallAuthorization,
  NativeWorkspaceIdentity,
  prepareRepositorySetup,
  readRepositorySetup,
  WorkspaceBaseline,
  WorkspaceBaselineRepository,
  runTurn,
  runMessagePipeline,
  TurnSteering,
  GenerationSourceCheckpoint,
  SandboxOperationError,
  type TurnRequest,
  type LifecycleCapture,
  Harness,
  type HarnessTurnRequest,
  type HarnessTurnSteer,
  resourceHash,
  sandboxNameFor,
  MATERIALIZATION_MARKER,
  REPO_DIR,
} from "@yielded/sandbox/cloudflare";
import { type AgentEvent } from "@yielded/sandbox";
import { failureDiagnostic } from "@yielded/sandbox/observability/FailureDiagnostic";

const record = (event: object) => console.log("GIT_HISTORY_EVENT " + JSON.stringify(event));
const traceLayer = Layer.effect(
  Tracer.Tracer,
  Effect.gen(function* () {
    const native = yield* Effect.tracer;
    return Tracer.make({
      context: native.context,
      span(options) {
        const span = native.span({ ...options, sampled: true });
        const at = Date.now();
        const end = span.end.bind(span);
        span.end = (endTime, exit) => {
          end(endTime, exit);
          if (
            options.name.startsWith("SandboxContainer.") ||
            options.name.startsWith("RepositorySetup.") ||
            options.name.startsWith("ArtifactsCheckout.") ||
            options.name.startsWith("workspace-checkpoint-") ||
            options.name === "repository-source-validate" ||
            options.name === "sandboxFor" ||
            options.name === "fork" ||
            options.name === "container-boot+marker"
          )
            record({
              kind: "span",
              name: options.name,
              sampled: span.sampled,
              at,
              end: Date.now(),
              durationMillis: Number((endTime - options.startTime) / 1_000_000n),
              outcome: exit._tag,
            });
        };
        return span;
      },
    });
  }),
).pipe(Layer.provide(CloudflareTracer.layer));
const options = {
  namespaceBinding: "SANDBOX",
  allowedHosts: () => ["fixture.invalid"],
  eventLayer: traceLayer,
};
const NativeContainer = makeSandboxAgentContainer(options);
export class Sandbox extends NativeContainer {
  dropNextInspectionReply = false;
  checkpointFault: "interrupt" | "replace" | undefined;
  dropInspectionReply() {
    this.dropNextInspectionReply = true;
  }
  faultAfterCheckpoint(fault: "interrupt" | "replace") {
    this.checkpointFault = fault;
  }
}
for (const method of [
  "describe",
  "ensure",
  "allocateProcess",
  "exec",
  "inspect",
  "waitProcess",
  "acknowledgeProcess",
  "lease",
  "readFileString",
  "configureOutboundAuthority",
  "remove",
  "processes",
  "writeFile",
] as const) {
  const original = Reflect.get(NativeContainer.prototype, method);
  if (typeof original !== "function") continue;
  Object.defineProperty(Sandbox.prototype, method, {
    value: async function (this: Sandbox, ...args: unknown[]) {
      record({
        kind: "rpc",
        name: method,
        at: Date.now(),
        ...(method === "readFileString" || method === "writeFile"
          ? { marker: args[0] === MATERIALIZATION_MARKER }
          : {}),
      });
      const result = await Reflect.apply(original, this, args);
      if (
        method === "lease" &&
        args[1] === false &&
        typeof args[0] === "string" &&
        args[0].endsWith(":checkpoint-validation") &&
        this.checkpointFault !== undefined &&
        Schema.is(Schema.Struct({ _tag: Schema.Literal("Success") }))(result)
      ) {
        const fault = this.checkpointFault;
        this.checkpointFault = undefined;
        record({ kind: "checkpoint-boundary", fault, instanceId: args[2], at: Date.now() });
        if (fault === "interrupt")
          return {
            _tag: "Failure",
            diagnostic: { operation: "SandboxContainer.lease", failures: [], interruptions: 1 },
          };
        const destroyed = await NativeContainer.prototype.destroy.call(this);
        if (destroyed._tag === "Failure") return destroyed;
        const replacement = await NativeContainer.prototype.ensure.call(this, {});
        if (replacement._tag === "Failure") return replacement;
        record({ kind: "replacement", instanceId: replacement.value.instanceId, at: Date.now() });
      }
      if (method === "inspect" && this.dropNextInspectionReply) {
        this.dropNextInspectionReply = false;
        throw new Error("Injected lost inspection reply after native completion");
      }
      return result;
    },
  });
}
export const SandboxOutbound = makeSandboxAgentOutbound(options);

const PinnedReadFaultKind = Schema.Literals([
  "once",
  "always",
  "expired",
  "replaced-before",
  "replaced-after",
  "mixed",
]);
const PinnedReadFault = Context.Reference<{
  readonly kind: typeof PinnedReadFaultKind.Type;
  readonly operation: "readCommit" | "log";
  readonly target: string;
  readonly reads: string[];
  readonly defect: Error;
  firstReadAt: number | null;
} | null>("fixture/PinnedReadFault", { defaultValue: () => null });

const artifactsLayer = Layer.effect(
  SandboxAgentArtifacts,
  Effect.gen(function* () {
    const env = (yield* WorkerEnvironment) as { FIXTURE_ARTIFACTS: Fetcher };
    const client = Artifacts.makeClient({ binding: "FIXTURE_ARTIFACTS" })(
      fixtureArtifacts(env.FIXTURE_ARTIFACTS),
    );
    return {
      ...client,
      get: Effect.fnUntraced(function* (name: string) {
        const repo = yield* client.get(name);
        const failRead = Effect.fnUntraced(function* (
          operation: "readCommit" | "log",
          target: string,
        ) {
          const fault = yield* PinnedReadFault;
          if (fault?.operation !== operation || fault.target !== target) return;
          fault.firstReadAt ??= yield* Clock.currentTimeMillis;
          fault.reads.push(`${name}:${target}`);
          if (fault.kind === "mixed")
            return yield* Effect.failCause(
              Cause.fromReasons([
                Cause.makeFailReason(
                  new Artifacts.ArtifactsOperationError({
                    binding: "FIXTURE_ARTIFACTS",
                    operation,
                    code: "NOT_FOUND",
                    cause: "fixture missing commit",
                  }),
                ),
                Cause.makeDieReason(fault.defect),
                Cause.makeInterruptReason(),
              ]),
            );
          if (fault.reads.length === 1 || fault.kind === "always" || fault.kind === "expired")
            return yield* Effect.never;
        });
        return {
          ...repo,
          info: () =>
            Effect.gen(function* () {
              const info = yield* repo.info();
              const fault = yield* PinnedReadFault;
              return fault !== null &&
                ((fault.kind === "replaced-before" && fault.reads.length >= 1) ||
                  (fault.kind === "replaced-after" && fault.reads.length >= 2))
                ? { ...info, id: "replacement-generation" }
                : info;
            }),
          readCommit: Effect.fnUntraced(function* (commit: string) {
            yield* failRead("readCommit", commit);
            return yield* repo.readCommit(commit);
          }),
          log: Effect.fnUntraced(function* (options?: Artifacts.ArtifactsLogOptions) {
            yield* failRead("log", options?.ref ?? "main");
            return yield* repo.log(options);
          }),
        };
      }),
    };
  }),
);
const configLayer = SandboxAgentConfig.layer({
  repositorySetupPath: ".fixture-setup.json",
  defaultModel: "unused",
  artifactsNamespace: "fixture",
  artifactsHostname: "fixture.invalid",
});
const fixtureLayer = sandboxWorkspaceLayer.pipe(
  Layer.provideMerge(
    Layer.mergeAll(
      artifactsLayer,
      RepositoryInstallAuthorization.emptyLayer,
      WorkspaceBaselineRepository.noopLayer,
      OutboundAuthority.noneLayer,
      NativeSandboxNamespace.layer({ binding: "SANDBOX" }),
      Sandboxes.layer({ binding: "SANDBOX" }),
      configLayer,
    ),
  ),
);
const Source = Schema.Struct({
  repositoryName: Schema.String,
  repositoryId: Schema.String,
  commitSha: Schema.String,
  artifactsRef: Schema.optionalKey(Schema.String),
});
const Input = Schema.Struct({
  name: Schema.String,
  mode: Schema.Literals([
    "prepare",
    "turn",
    "fork",
    "fences",
    "pinned-read",
    "destroy",
    "outbound-grant",
    "outbound-reloaded",
    "outbound-regrant",
  ]),
  source: Source,
  requiredCommits: Schema.Array(Schema.String),
  baseline: Schema.optionalKey(WorkspaceBaseline),
  includeHistory: Schema.optionalKey(Schema.Boolean),
  divergent: Schema.optionalKey(Schema.String),
  publication: Schema.optionalKey(Source),
  recover: Schema.optionalKey(Schema.Boolean),
  deadlineAtMs: Schema.optionalKey(Schema.Number),
  startedAtMillis: Schema.optionalKey(Schema.Number),
  waitForFork: Schema.optionalKey(Schema.Boolean),
  sourcePinTimeout: Schema.optionalKey(Schema.Boolean),
  checkpointBoundary: Schema.optionalKey(Schema.Literals(["interrupt", "replace"])),
  api: Schema.optionalKey(Schema.Literal("legacy")),
  callbackFailure: Schema.optionalKey(Schema.Boolean),
  generationHandoff: Schema.optionalKey(
    Schema.Literals(["missing", "fail", "accept", "unchanged"]),
  ),
});
const command = Effect.fnUntraced(function* (args: readonly [string, ...string[]]) {
  const process = yield* (yield* SessionSandbox).exec(args, { cwd: REPO_DIR });
  const result = yield* process.outputText({ timeout: 30_000, maxBytes: 8192 });
  if (result.exitCode !== 0 || result.timedOut || result.truncated)
    return yield* Effect.die(`Git fixture command failed: ${result.exitCode}: ${result.stderr}`);
  return result.stdout.trim();
});
const errorTag = <A, E>(exit: import("effect").Exit.Exit<A, E>) => {
  if (exit._tag === "Success") return null;
  const error = Cause.findErrorOption(exit.cause);
  return Option.isSome(error) && Schema.is(Schema.Struct({ _tag: Schema.String }))(error.value)
    ? error.value._tag
    : "Defect";
};
const run = Effect.fn("GitHistoryFixture.run")(
  function* (input: unknown) {
    const value = yield* Schema.decodeUnknownEffect(Input)(input);
    const sandboxName = sandboxNameFor(yield* resourceHash(value.name));
    const unresolved = yield* makeNativeSandbox(sandboxName, { instance: "standard-1" });
    if (value.mode === "destroy") {
      const snapshotId = (yield* unresolved.describe).checkpoint?.snapshotId ?? null;
      yield* unresolved.destroy;
      return { destroyed: true, snapshotId };
    }
    const store = yield* SessionStateStore;
    const checkout = yield* ArtifactsCheckout;
    const events: AgentEvent[] = [];
    let pipelineFailure: string | null = null;
    let pipelineDiagnostic: ReturnType<typeof failureDiagnostic> | null = null;
    if (value.mode === "turn" || value.mode === "fork") {
      const env = (yield* WorkerEnvironment) as {
        FIXTURE_ARTIFACTS: Fetcher;
        SANDBOX: DurableObjectNamespace<Sandbox>;
      };
      const checkpointFault = value.checkpointBoundary;
      if (checkpointFault !== undefined)
        yield* Effect.tryPromise(() =>
          env.SANDBOX.getByName(sandboxName).faultAfterCheckpoint(checkpointFault),
        );
      const state = yield* ObjectState.DurableObjectState;
      const lifecycle =
        value.api === "legacy"
          ? yield* Ref.make<LifecycleCapture>({ preTurn: null, observed: null, postTurn: null })
          : null;
      const callbackError = new SandboxOperationError({
        operation: "fixture.host-callback",
        cause: "fixture-only",
      });
      const callbackDefect = new Error("fixture-callback-defect");
      const callbackCause = Cause.fromReasons([
        Cause.makeFailReason(callbackError),
        Cause.makeDieReason(callbackDefect),
        Cause.makeInterruptReason(),
      ]);
      const steering: unknown[] = [];
      const acceptSteering = Effect.fnUntraced(function* (steer: HarnessTurnSteer) {
        steering.push(yield* steer({ id: "fixture-steering", text: "Continue the fixture" }));
      });
      let attemptedHead: string | null = null;
      const acceptPrepared = Effect.fnUntraced(function* (head: string) {
        attemptedHead = head;
        yield* state.storage.kv.put("fixturePreparedHead", head).pipe(Effect.orDie);
      });
      const request: TurnRequest = {
        sessionId: value.name,
        turnId: `${value.name}:turn`,
        recover: value.recover,
        deadlineAtMs: value.deadlineAtMs,
        text: "Local Git fixture; no model request",
        ...(value.generationHandoff === undefined
          ? {
              repository: {
                _tag: "SeedRepository",
                repositoryName: value.source.repositoryName,
                artifactsRepositoryId: value.source.repositoryId,
                commitSha: value.source.commitSha,
                artifactsRef: value.source.artifactsRef,
              } as const,
            }
          : {
              generationSource: {
                repositoryName: value.source.repositoryName,
                repositoryId: value.source.repositoryId,
                acceptedSourceSha: value.source.commitSha,
              },
            }),
        requiredCommits: value.requiredCommits,
        initialCheckout: value.source.commitSha,
        ...(value.baseline === undefined ? {} : { continuationCheckpoint: value.baseline }),
        startedAtMillis: value.startedAtMillis ?? (yield* Clock.currentTimeMillis),
      };
      const turn =
        lifecycle === null
          ? runTurn(request)
          : runMessagePipeline({
              ...request,
              lifecycle,
              onSteeringReady: (steer) =>
                value.callbackFailure ? Effect.failCause(callbackCause) : acceptSteering(steer),
              ...(value.generationHandoff === undefined || value.generationHandoff === "missing"
                ? {}
                : {
                    onGenerationSourcePrepared: (head: string) => {
                      attemptedHead = head;
                      return value.generationHandoff === "fail"
                        ? Effect.failCause(callbackCause)
                        : acceptPrepared(head);
                    },
                  }),
            });
      const result = yield* turn.pipe(
        Effect.provideService(
          PinnedReadFault,
          value.sourcePinTimeout
            ? {
                kind: "once",
                operation: "log",
                target: (value.source.artifactsRef ?? "main").replace(/^refs\/heads\//u, ""),
                reads: [],
                defect: new Error("fixture read defect"),
                firstReadAt: null,
              }
            : null,
        ),
        Effect.provideService(TurnSteering, { ready: acceptSteering }),
        // Legacy calls must not inherit this authority when their callback is absent.
        Effect.provideService(GenerationSourceCheckpoint, { prepared: acceptPrepared }),
        Effect.provideService(AgentEventSink, {
          emit: Effect.fnUntraced(function* (event) {
            events.push(event);
            if (event.type === "agent.ready" && value.waitForFork) {
              yield* Effect.tryPromise(() =>
                env.FIXTURE_ARTIFACTS.fetch("http://fixture.invalid/before-model"),
              ).pipe(Effect.orDie);
            }
          }),
        }),
        Effect.provideService(Harness, {
          name: "codex",
          binary: "codex",
          version: "fixture-no-model",
          runTurn: Effect.fnUntraced(function* <EventError>(
            request: HarnessTurnRequest<EventError>,
          ) {
            if (request.recover) {
              const saved = yield* store.codexTurn.get;
              if (saved?.result) return saved.result;
            }
            record({
              kind: "harness",
              at: Date.now(),
              ...(value.mode === "fork" ? { forkIntent: yield* store.repositoryFork.get } : {}),
            });
            yield* request.onEvent({ type: "codex.event", event: { type: "fixture.start" } });
            if (request.onSteeringReady)
              yield* request.onSteeringReady(() =>
                Effect.succeed({ _tag: "Accepted", turnId: request.turnId }),
              );
            if (value.generationHandoff !== undefined && value.generationHandoff !== "unchanged")
              yield* command(["sh", "-c", "printf 'generation change\\n' > generation.txt"]);
            yield* request.onEvent({ type: "codex.event", event: { type: "fixture.finish" } });
            const result = {
              resumeToken: "fixture",
              terminalEventType: "fixture",
              lastAgentMessage: null,
              usage: null,
              stderrTail: "",
            };
            if (value.mode === "fork")
              yield* store.codexTurn.put({
                id: request.turnId,
                processId: "fixture-no-model",
                token: "fixture-only",
                configuration: {
                  command: ["false"],
                  cwd: REPO_DIR,
                  env: {},
                  mode: "codex",
                  port: 4500,
                  nativePort: 4501,
                },
                threadParams: {},
                turnParams: {},
                resumeToken: null,
                deadlineAt: request.deadlineAtMs ?? (yield* Clock.currentTimeMillis) + 60_000,
                threadId: "fixture",
                turnId: "fixture",
                previousTurnId: null,
                startDispatched: true,
                result,
              });
            if (value.mode === "fork") record({ kind: "harness-result", at: Date.now() });
            return result;
          }),
        }),
        Effect.exit,
      );
      pipelineFailure = errorTag(result);
      if (result._tag === "Failure") {
        pipelineDiagnostic = failureDiagnostic(result.cause, {
          operation: "GitHistoryFixture.turn",
        });
        console.error(JSON.stringify(pipelineDiagnostic));
      }
      if (value.mode === "fork")
        return {
          pipelineFailure,
          pipelineDiagnostic,
          ready: events.find((event) => event.type === "agent.ready") ?? null,
          forkIntent: yield* store.repositoryFork.get,
          materializationId: yield* store.materializationId.get,
          containerInstanceId: yield* store.containerInstanceId.get,
          persisted: (yield* store.pipelineTurn.get)?.persisted ?? null,
          completionCount: events.filter((event) => event.type === "agent.completed").length,
          dialectEvents: events.filter((event) => event.type === "codex.event"),
          steering,
          lifecycle: lifecycle === null ? null : yield* Ref.get(lifecycle),
          attemptedHead,
          preparedHead: (yield* state.storage.kv.get("fixturePreparedHead")) ?? null,
          nativeDeadline: (yield* store.codexTurn.get)?.deadlineAt ?? null,
          callbackCause:
            result._tag === "Failure"
              ? {
                  errorPreserved: result.cause.reasons.some(
                    (reason) => reason._tag === "Fail" && reason.error === callbackError,
                  ),
                  defectPreserved: result.cause.reasons.some(
                    (reason) => reason._tag === "Die" && reason.defect === callbackDefect,
                  ),
                  interruptPreserved: result.cause.reasons.some(
                    (reason) => reason._tag === "Interrupt",
                  ),
                }
              : null,
        };
    }
    const identity = yield* unresolved.ensure();
    const sandbox = yield* makeNativeSandbox(sandboxName, {
      instance: "standard-1",
      expectedInstanceId: identity.instanceId,
      recovery: "exactWorkspaceRequired",
    });
    return yield* Effect.gen(function* () {
      if (value.mode === "pinned-read") {
        const config = yield* SandboxAgentConfig;
        const artifacts = yield* SandboxAgentArtifacts;
        const reads = [];
        const requests = [
          ...PinnedReadFaultKind.literals.map((kind) => ({
            kind,
            operation: "readCommit" as const,
          })),
          ...(["once", "expired", "replaced-after", "always"] as const).map((kind) => ({
            kind,
            operation: "log" as const,
          })),
        ];
        for (const { operation, kind } of requests) {
          yield* store.repositoryFork.delete;
          const target =
            operation === "readCommit"
              ? value.source.commitSha
              : (value.source.artifactsRef ?? "main").replace(/^refs\/heads\//u, "");
          const fault = {
            kind,
            operation,
            target,
            reads: [] as string[],
            defect: new Error("fixture read defect"),
            firstReadAt: null as number | null,
          };
          const readOptions = {
            repositoryId: value.source.repositoryId,
            retryDeadlineAtMillis:
              kind === "expired"
                ? 0
                : (yield* Clock.currentTimeMillis) +
                  (operation === "log" && kind === "always" ? 1_500 : 10_000),
          };
          const result = yield* Effect.gen(function* () {
            const service = yield* ArtifactsCheckout;
            if (operation === "readCommit") {
              yield* service.ensureCommit(
                value.source.repositoryName,
                value.source.commitSha,
                readOptions,
              );
            } else {
              yield* service.retainFork(
                `${value.name}-${kind}`,
                value.source,
                readOptions.retryDeadlineAtMillis,
              );
            }
          }).pipe(
            Effect.provide(
              Layer.fresh(ArtifactsCheckout.layer).pipe(
                Layer.provide(
                  Layer.mergeAll(
                    Layer.succeed(SandboxAgentConfig, {
                      ...config,
                      deadlines: {
                        ...config.deadlines,
                        artifactsOperationMillis: 1_000,
                        forkReadyMillis: 5_000,
                      },
                    }),
                    Layer.succeed(SandboxAgentArtifacts, artifacts),
                  ),
                ),
              ),
            ),
            Effect.provideService(PinnedReadFault, fault),
            Effect.exit,
          );
          const retained = yield* store.repositoryFork.get;
          const timeoutBudgets =
            result._tag === "Failure"
              ? result.cause.reasons.flatMap((reason) =>
                  Cause.isFailReason(reason) &&
                  reason.error._tag === "ArtifactsOperationTimeoutError"
                    ? [reason.error.timeoutMillis]
                    : [],
                )
              : [];
          const retryTimeout = timeoutBudgets[1];
          const diagnostic =
            result._tag === "Failure"
              ? failureDiagnostic(result.cause, { operation: "GitHistoryFixture.run" })
              : null;
          reads.push({
            operation,
            kind,
            attempts: fault.reads.length,
            sameCommit: fault.reads.every(
              (read) => read === `${value.source.repositoryName}:${target}`,
            ),
            deadlinePreserved:
              retained === null ||
              (fault.firstReadAt !== null &&
                retained.requestedAtMillis <= fault.firstReadAt &&
                retained.deadlineAtMillis ===
                  Math.min(readOptions.retryDeadlineAtMillis, retained.requestedAtMillis + 5_000)),
            sourcePreserved:
              retained === null ||
              (retained.source.commitSha === value.source.commitSha &&
                retained.source.artifactsRef === value.source.artifactsRef &&
                retained.source.repositoryId === value.source.repositoryId &&
                retained.originalMainHead === value.source.commitSha),
            retryWithinDeadline:
              operation !== "log" ||
              kind !== "always" ||
              (timeoutBudgets.length === 2 &&
                retryTimeout !== undefined &&
                retryTimeout > 0 &&
                retryTimeout < 1_000),
            success: result._tag === "Success",
            timeouts:
              diagnostic?.failures.filter(
                (failure) => failure.type === "ArtifactsOperationTimeoutError",
              ).length ?? 0,
            generationMismatch:
              diagnostic?.failures.some(
                (failure) => failure.type === "ArtifactsRepositoryGenerationMismatchError",
              ) ?? false,
            defectPreserved:
              result._tag === "Failure" &&
              result.cause.reasons.some(
                (reason) => Cause.isDieReason(reason) && reason.defect === fault.defect,
              ),
          });
        }
        yield* store.repositoryFork.delete;
        return { reads };
      }
      if (
        value.mode === "outbound-grant" ||
        value.mode === "outbound-reloaded" ||
        value.mode === "outbound-regrant"
      ) {
        const remote = `https://fixture.invalid/git/fixture/${value.source.repositoryName}.git`;
        let grant: RepositoryAuthorizationGrant | undefined;
        if (value.mode !== "outbound-reloaded") {
          const repository = yield* (yield* SandboxAgentArtifacts).get(value.source.repositoryName);
          const token = yield* repository.createToken("read", 300);
          grant = yield* sandbox.setRepositoryAuthorization(
            remote,
            `Bearer ${token.plaintext}`,
            (yield* Clock.currentTimeMillis) + 300_000,
            token.id,
          );
        }
        return {
          identity,
          outbound: yield* sandbox
            .inspect(["git", "ls-remote", remote, "HEAD"], {
              cwd: REPO_DIR,
              timeout: 10_000,
              maxBytes: 1024,
            })
            .pipe(
              Effect.ensuring(
                value.mode === "outbound-regrant" && grant !== undefined
                  ? sandbox.clearRepositoryAuthorization(grant).pipe(Effect.orDie)
                  : Effect.void,
              ),
            ),
        };
      }
      if (value.mode === "prepare") {
        const cloned = yield* checkout.cloneGenerationIntoSandbox(value.source);
        if (cloned.head !== value.source.commitSha)
          return yield* Effect.die("Fixture source pin changed");
        if (value.includeHistory)
          yield* checkout.ensureCommit(value.source.repositoryName, cloned.head, {
            repositoryId: value.source.repositoryId,
            includeHistory: true,
          });
        const marker = crypto.randomUUID();
        yield* sandbox.writeFile(MATERIALIZATION_MARKER, marker);
        yield* store.materializationId.put(marker);
        yield* prepareRepositorySetup({
          sourceKey: value.source.repositoryName,
          sourceHead: cloned.head,
          snapshot: { repository: "fixture", identity, share: false },
        }).pipe(
          Effect.provideService(NativeWorkspaceIdentity, {
            path: MATERIALIZATION_MARKER,
            id: marker,
          }),
        );
      }
      let fences = null;
      if (value.mode === "fences") {
        const config = yield* SandboxAgentConfig;
        const absentRecipe = yield* readRepositorySetup(value.source.commitSha).pipe(
          Effect.provideService(SandboxAgentConfig, {
            ...config,
            repositorySetupPath: ".fixture-absent.json",
          }),
        );
        const missingRecipeCommit = yield* readRepositorySetup("0".repeat(40)).pipe(Effect.exit);
        const inspection = { cwd: REPO_DIR, timeout: 30_000, maxBytes: 128 };
        const stale = yield* makeNativeSandbox(sandboxName, {
          expectedInstanceId: crypto.randomUUID(),
          recovery: "exactWorkspaceRequired",
        });
        const staleEnsure = yield* stale.ensure().pipe(Effect.exit);
        const staleRead = yield* stale
          .inspect(["git", "rev-parse", "HEAD"], inspection)
          .pipe(Effect.exit);
        const staleLease = yield* stale.lease("stale-fixture", true).pipe(Effect.exit);
        const lost = yield* makeNativeSandbox(`${sandboxName}-missing`, {
          expectedInstanceId: identity.instanceId,
          recovery: "exactWorkspaceRequired",
        });
        const lostWorkspace = yield* lost.ensure().pipe(Effect.exit);
        const lostDescription = yield* lost.describe;
        const missingFile = yield* sandbox
          .readFileString("/workspace/fixture-missing-file")
          .pipe(Effect.exit);
        const projection = yield* decodeNativeReply("readFileString", {
          _tag: "Failure",
          code: "ETIMEDOUT",
          diagnostic: failureDiagnostic(
            Cause.fromReasons([
              Cause.makeFailReason(new Cause.TimeoutError("fixture-private-timeout")),
              Cause.makeDieReason(new TypeError("fixture-private-defect")),
              Cause.makeInterruptReason(),
            ]),
            { operation: "SandboxContainer.readFileString" },
          ),
        }).pipe(Effect.exit);
        const truncated = yield* sandbox.inspect(["git", "rev-parse", "HEAD"], {
          ...inspection,
          maxBytes: 4,
        });
        const missing = yield* sandbox.inspect(
          ["git", "cat-file", "-e", "0".repeat(40) + "^{commit}"],
          inspection,
        );
        const timeout = yield* sandbox
          .inspect(["sh", "-c", "sleep 5 & wait"], { ...inspection, timeout: 500 })
          .pipe(Effect.exit);
        const timeoutFailure =
          timeout._tag === "Failure" ? Cause.findErrorOption(timeout.cause) : Option.none();
        const env = (yield* WorkerEnvironment) as {
          SANDBOX: DurableObjectNamespace<Sandbox>;
        };
        yield* Effect.tryPromise(() => env.SANDBOX.getByName(sandboxName).dropInspectionReply());
        const readHead = sandbox.inspect(["git", "rev-parse", "HEAD"], inspection);
        const dropped = yield* readHead.pipe(Effect.exit);
        const retried = yield* readHead;
        const inspections = {
          staleEnsure: errorTag(staleEnsure),
          staleRead: errorTag(staleRead),
          staleLease: errorTag(staleLease),
          lostWorkspace: errorTag(lostWorkspace),
          lostDescription,
          diagnostics: Object.fromEntries(
            Object.entries({
              staleEnsure,
              staleRead,
              staleLease,
              lostWorkspace,
              missingFile,
              projection,
            }).map(([name, exit]) => [
              name,
              exit._tag === "Failure"
                ? failureDiagnostic(exit.cause, { operation: "GitHistoryFixture.run" })
                : null,
            ]),
          ),
          truncated,
          missing,
          timeout:
            timeout._tag === "Success"
              ? { timedOut: timeout.value.timedOut }
              : { code: Option.isSome(timeoutFailure) ? timeoutFailure.value.code : null },
          droppedReply: errorTag(dropped),
          retried,
          processes: yield* sandbox.listProcesses,
          instanceId: (yield* sandbox.ensure()).instanceId,
        };
        const rejectedGeneration = yield* checkout
          .ensureCommit(value.source.repositoryName, value.source.commitSha, {
            repositoryId: "replacement-generation",
            includeHistory: true,
          })
          .pipe(Effect.exit);
        const rejectedPin = yield* checkout
          .readSourcePin({ ...value.source, commitSha: "0".repeat(40) })
          .pipe(Effect.exit);
        const rejectedSnapshot = yield* checkout
          .restoreSnapshotCheckout(
            value.source.repositoryName,
            value.source.commitSha,
            "0".repeat(40),
          )
          .pipe(Effect.exit);
        if (value.divergent === undefined || value.publication === undefined)
          return yield* Effect.die("Missing publication fixture");
        yield* checkout.ensureCommit(value.source.repositoryName, value.divergent, {
          repositoryId: value.source.repositoryId,
          includeHistory: true,
        });
        const rejectedAncestry = yield* checkout
          .pushPreparedGeneration(value.publication, value.publication.commitSha, value.divergent)
          .pipe(Effect.exit);
        const published = yield* checkout.pushPreparedGeneration(
          value.publication,
          value.publication.commitSha,
          value.source.commitSha,
        );
        fences = {
          absentRecipe,
          missingRecipeCommit: errorTag(missingRecipeCommit),
          inspections,
          generation: errorTag(rejectedGeneration),
          pin: errorTag(rejectedPin),
          snapshot: errorTag(rejectedSnapshot),
          ancestry: errorTag(rejectedAncestry),
          published,
        };
      }
      const checkpoint = yield* store.workspaceCheckpoint.get;
      const receipt = yield* store.repositorySetupReceipt.get;
      const fork = yield* store.repositoryFork.get;
      const ready = events.find((event) => event.type === "agent.ready");
      return {
        mode: value.mode,
        pipelineFailure,
        pipelineDiagnostic,
        identity,
        head: yield* command(["git", "rev-parse", "HEAD"]),
        shallow: (yield* command(["git", "rev-parse", "--is-shallow-repository"])) === "true",
        history: (yield* command(["git", "rev-list", "HEAD"])).split("\n"),
        status: yield* command(["git", "status", "--porcelain"]),
        installCount: Number(
          yield* command(["sh", "-c", "wc -l < /workspace/fixture-install-count"]),
        ),
        ready: ready?.type === "agent.ready" ? ready : null,
        baseline:
          checkpoint && receipt
            ? { repository: "fixture", checkpoint, report: receipt.report }
            : null,
        fork: fork?.initialized
          ? {
              repositoryName: fork.forkName,
              repositoryId: fork.repositoryId,
              commitSha: value.source.commitSha,
            }
          : null,
        persisted: (yield* store.pipelineTurn.get)?.persisted ?? null,
        fences,
      };
    }).pipe(Effect.provideService(SessionSandbox, sandbox));
  },
  Effect.scoped,
  Effect.tapCause((cause) =>
    Effect.sync(() =>
      console.error(failureDiagnostic(cause, { operation: "GitHistoryFixture.run" })),
    ),
  ),
);

export const GitHistory = DurableObject.make(fixtureLayer, {
  eventLayer: traceLayer,
  rpc: { run },
});
export default {
  async fetch(
    request: Request,
    env: { GIT_HISTORY: DurableObjectNamespace<InstanceType<typeof GitHistory>> },
  ) {
    const input = Schema.decodeUnknownSync(Input)(await request.json());
    const result = await env.GIT_HISTORY.getByName(input.name).run(input);
    console.log("GIT_HISTORY_DONE");
    return Response.json(result);
  },
};
