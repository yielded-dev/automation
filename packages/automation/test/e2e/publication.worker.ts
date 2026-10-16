import { Cause, DateTime, Effect, Layer, Option, Redacted, Schema } from "effect";
import { HttpClient, HttpClientResponse } from "effect/http";
import { PgClient } from "@effect/sql-pg";
import { ConflictError, ServiceUnavailableError } from "@yielded/automation/domain/errors";
import { RepairPublicationRequest } from "@yielded/automation/domain/proof";
import { RunLaunch } from "@yielded/automation/domain/RepairExecution";
import { RepairReportJson } from "@yielded/automation/domain/RepairReport";
import { GithubAppAuth, GithubAppAuthError } from "@yielded/automation/server/GithubAppAuth";
import {
  GithubFollowUpConfig,
  GithubFollowUpHost,
  prepareGithubFollowUp,
} from "@yielded/automation/server/GithubFollowUps";
import {
  PublicationError,
  publishRepository,
  readPublication,
} from "@yielded/automation/server/Publication";
import { RepairAuthorization } from "@yielded/automation/server/RepairAuthorization";
import { RepairErrorReporter } from "@yielded/automation/server/RepairErrorReporter";
import { RepairExecutionContext } from "@yielded/automation/server/RepairExecutionContext";
import { RepairExecutionRepository } from "@yielded/automation/server/database/RepairExecutionRepository";
import { RepairGithubClient } from "@yielded/automation/server/RepairGithubClient";
import { mapRepairHostError } from "@yielded/automation/server/RepairHostError";
import { RepairModel } from "@yielded/automation/server/RepairModel";
import { RepairPresentation } from "@yielded/automation/server/RepairPresentation";
import {
  RepairSandboxTaskTransportError,
  RepairSandboxTaskClient,
} from "@yielded/automation/server/RepairSandboxTaskClient";
import { RunClient } from "@yielded/automation/server/RunClient";
import { RunConfig } from "@yielded/automation/server/RunConfig";
import { SandboxAgentArtifacts } from "@yielded/sandbox/cloudflare/Bindings";

const Input = Schema.Struct({
  launch: RunLaunch,
  publication: RepairPublicationRequest,
  remoteHead: Schema.String,
  mode: Schema.Literals([
    "direct",
    "publish",
    "abandon",
    "retire",
    "follow-up-review",
    "follow-up-checks",
  ]),
  fault: Schema.Literals(["none", "installation", "mixed", "cancel-pending", "cancel-mixed"]),
});
const content = { kind: "repair" as const, title: "Synthetic repair", body: "Synthetic evidence" };
const describe = (cause: Cause.Cause<unknown>) =>
  cause.reasons.map((reason) => {
    if (Cause.isDieReason(reason)) return { kind: "Die", value: reason.defect };
    if (Cause.isInterruptReason(reason)) return { kind: "Interrupt", fiberId: reason.fiberId };
    const error = reason.error;
    return {
      kind: "Fail",
      tag: Schema.is(ConflictError)(error)
        ? error._tag
        : Schema.is(PublicationError)(error)
          ? error._tag
          : Schema.is(ServiceUnavailableError)(error)
            ? error._tag
            : Schema.is(GithubAppAuthError)(error)
              ? error._tag
              : "other",
    };
  });

export default {
  async fetch(request: Request, env: { DATABASE: Hyperdrive }) {
    const input = Schema.decodeUnknownSync(Input)(await request.json());
    const reports: Array<{ operation: string; reasons: ReturnType<typeof describe> }> = [];
    const requests: Array<{ method: string; path: string }> = [];
    const cancellations: Array<unknown> = [];
    const database = PgClient.layer({
      url: Redacted.make(env.DATABASE.connectionString),
      maxConnections: 1,
    });
    const dependencies = Layer.mergeAll(
      database,
      RepairExecutionRepository.layer.pipe(Layer.provide(database)),
      Layer.succeed(RunConfig, {
        accountId: "fixture",
        artifactsNamespace: "fixture",
        githubInstallationId: "1",
      }),
      Layer.succeed(RepairAuthorization, { authorize: () => Effect.succeed(true) }),
      Layer.succeed(RepairErrorReporter, {
        report: (cause, operation) =>
          Effect.sync(() => reports.push({ operation, reasons: describe(cause) })).pipe(
            Effect.asVoid,
          ),
      }),
      // Host policy, foreign GitHub and immutable native snapshot ports are synthetic.
      // Missing mutation/model ports fail rather than imitating a container.
      Layer.mock(GithubAppAuth, {
        availability: "configured",
        repositoryInstallation: () =>
          input.fault === "mixed"
            ? Effect.failCause(
                Cause.fromReasons([
                  Cause.makeFailReason(
                    GithubAppAuthError.make({ operation: "fixture", message: "Fixture failure" }),
                  ),
                  Cause.makeDieReason("fixture-defect"),
                  Cause.makeInterruptReason(42),
                ]),
              )
            : Effect.succeed(
                Option.some({
                  id: input.fault === "installation" ? 2 : 1,
                  account: { login: "fixture", type: "Organization" as const },
                  permissions: { contents: "write", pull_requests: "write" },
                  suspended_at: null,
                }),
              ),
        repositoryWriteToken: () => Effect.succeed(Redacted.make("synthetic")),
        botCommitter: () =>
          Effect.succeed({ accountId: "1", name: "fixture[bot]", email: "fixture@example.com" }),
      }),
      Layer.mock(RepairModel, { usage: () => Effect.succeed(undefined) }),
      Layer.mock(RepairPresentation, { publicationContent: () => Effect.succeed(content) }),
      Layer.mock(SandboxAgentArtifacts, { definition: { binding: "UNUSED_ARTIFACTS" } }),
      Layer.mock(RepairSandboxTaskClient, {
        cancel: (id, commandId, admission, options) => {
          cancellations.push({ id, commandId, admission, options });
          if (input.fault === "cancel-mixed")
            return Effect.failCause(
              Cause.fromReasons([
                Cause.makeFailReason(RepairSandboxTaskTransportError.make({ operation: "Cancel" })),
                Cause.makeDieReason("fixture-cancel-defect"),
                Cause.makeInterruptReason(43),
              ]),
            );
          return Effect.succeed(
            input.fault === "cancel-pending"
              ? { _tag: "SandboxTaskCancellationDraining" as const, id, commandId }
              : { id, commandId, cancelled: true as const },
          );
        },
        snapshot: () =>
          Effect.gen(function* () {
            return {
              repositoryName: "fixture-source",
              verifiedHead: input.publication.candidateSha,
              materializationId: "fixture-materialization",
              containerSize: "standard" as const,
              pipeline: null,
              codexTurn: {
                id: "fixture-turn",
                result: {
                  terminalEventType: "turn.completed",
                  lastAgentMessage: yield* Schema.encodeEffect(RepairReportJson)({
                    ...input.publication,
                    notes: "Retained synthetic handoff",
                    reviewResponses: [],
                  }).pipe(Effect.orDie),
                },
              },
            };
          }),
      }),
      Layer.succeed(
        HttpClient.HttpClient,
        HttpClient.make((request, url) => {
          requests.push({ method: request.method, path: url.pathname });
          if (request.method !== "GET") return Effect.die("Unexpected GitHub mutation");
          const body = url.pathname.endsWith("/pulls")
            ? [
                {
                  number: 1,
                  title: content.title,
                  body: content.body,
                  html_url: "https://github.com/fixture/project/pull/1",
                  state: "open",
                  draft: false,
                  labels: [{ name: "bot-repair" }],
                  head: { sha: input.remoteHead, ref: "repair/fixture", repo: { id: 1 } },
                  base: { ref: "main", repo: { id: 1 } },
                  user: { login: "fixture[bot]" },
                },
              ]
            : url.pathname.endsWith("/git/ref/heads/repair/fixture")
              ? { object: { sha: input.remoteHead } }
              : undefined;
          return body === undefined
            ? Effect.die("Unexpected GitHub read")
            : Effect.succeed(HttpClientResponse.fromWeb(request, Response.json(body)));
        }),
      ),
    );
    const operation = Effect.gen(function* () {
      if (input.mode === "follow-up-review" || input.mode === "follow-up-checks") {
        const publication = yield* readPublication;
        if (publication === undefined) return yield* Effect.die("Missing fixture publication");
        const records = yield* RepairExecutionRepository;
        const now = yield* DateTime.now;
        return yield* prepareGithubFollowUp(input.launch.runId).pipe(
          Effect.provideService(GithubFollowUpConfig, {
            instructions: "Evaluate synthetic feedback",
            modelSettings: input.launch.launch.modelSettings,
            commentIdentity: "fixture-github-comment",
            promptPolicyPrefix: "fixture-github-",
            lifetimeMillis: 600_000,
          }),
          Effect.provideService(GithubFollowUpHost, {
            published: () =>
              Effect.succeed({
                launch: input.launch,
                publication,
                commentsSince: DateTime.formatIso(now),
              }),
            current: () =>
              Effect.succeed({
                launch: input.launch,
                authorityRef: "fixture",
                handledRequestIds: [],
              }),
            observe: () => Effect.void,
            accept: (_, __, launch) =>
              records.accept(launch).pipe(Effect.as(launch), mapRepairHostError("fixture.accept")),
          }),
          Effect.provide(
            Layer.mock(RepairGithubClient, {
              pullRequest: () =>
                Effect.succeed({
                  number: 1,
                  updated_at: DateTime.formatIso(now),
                  state: "open",
                  draft: false,
                  merged: false,
                  mergeable: true,
                  title: content.title,
                  body: content.body,
                  additions: 1,
                  deletions: 0,
                  changed_files: 1,
                  user: { login: "fixture[bot]" },
                  head: { sha: publication.sourceHead, ref: publication.branch, repo: { id: 1 } },
                  base: { sha: input.launch.launch.targetSha, ref: "main", repo: { id: 1 } },
                }),
              comments: (work) =>
                Effect.succeed(
                  input.mode === "follow-up-review"
                    ? [
                        {
                          ...work,
                          _tag: "comment",
                          installationId: null,
                          comment: {
                            kind: "issue_comment",
                            id: 1,
                            author: "fixture-reviewer",
                            threadId: null,
                            url: "https://github.com/fixture/project/pull/1#issuecomment-1",
                          },
                          question: "Evaluate this synthetic review",
                        },
                      ]
                    : [],
                ),
              collaborator: () => Effect.succeed(true),
              checks: () =>
                Effect.succeed({
                  headSha: publication.sourceHead,
                  fetchedAt: now,
                  complete: true,
                  items: [
                    { id: "1", name: "Check", state: "failed", description: "Synthetic failure" },
                  ],
                }),
              acknowledge: () => Effect.void,
            }),
          ),
        );
      }
      if (input.mode === "direct")
        return yield* publishRepository(input.publication.candidateSha, content);
      const client = yield* RunClient;
      if (input.mode === "abandon")
        return yield* client.abandonPublication(input.launch.runId, "fixture-abandonment");
      if (input.mode === "retire") return yield* client.retire(input.launch, "deadline");
      return yield* client.publish(input.launch.runId, input.publication);
    });
    const exit = await Effect.runPromiseExit(
      operation.pipe(
        Effect.provideService(RepairExecutionContext, input.launch.runId),
        Effect.provide(RunClient.layer.pipe(Layer.provideMerge(dependencies))),
      ),
    );
    return Response.json({
      exit: exit._tag,
      ...(exit._tag === "Failure" ? { reasons: describe(exit.cause) } : { value: exit.value }),
      reports,
      requests,
      cancellations,
    });
  },
};
