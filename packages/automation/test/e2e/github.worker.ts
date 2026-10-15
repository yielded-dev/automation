import { Effect, Exit, Layer, Redacted } from "effect";
import { FetchHttpClient } from "effect/http";
import {
  exchangeGithubAppManifest,
  githubAppManifest,
  readGithubWebhook,
} from "@yielded/automation/github";
import { GithubAppAuth } from "@yielded/automation/server/GithubAppAuth";
import { RepairErrorReporter } from "@yielded/automation/server/RepairErrorReporter";
import { RepairGithubClient } from "@yielded/automation/server/RepairGithubClient";
import { RepairGithubConfig } from "@yielded/automation/server/RepairGithubConfig";

interface Env {
  WEBHOOK_SECRET: string;
  APP_PRIVATE_KEY: string;
}

export default {
  async fetch(request: Request, env: Env) {
    const github = RepairGithubClient.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          GithubAppAuth.layer({
            appId: "104",
            installationId: "205",
            privateKey: Redacted.make(env.APP_PRIVATE_KEY),
          }),
          Layer.succeed(RepairGithubConfig, {
            githubInstallationId: "205",
          }),
          Layer.succeed(RepairErrorReporter, { report: () => Effect.void }),
          FetchHttpClient.layer,
        ),
      ),
    );
    const operation = Effect.gen(function* () {
      switch (new URL(request.url).pathname) {
        case "/webhook":
          return Response.json({
            event: yield* readGithubWebhook(request, Redacted.make(env.WEBHOOK_SECRET)),
          });
        case "/manifest":
          return Response.json(
            yield* githubAppManifest({
              name: "Native repairs fixture",
              publicOrigin: "https://fixture.invalid",
              webhookUrl: "https://fixture.invalid/webhook",
              redirectUrl: "https://fixture.invalid/callback",
              setupUrl: "https://fixture.invalid/install",
              public: false,
              publishChanges: true,
            }),
          );
        case "/register": {
          const registered = yield* exchangeGithubAppManifest(
            Redacted.make("one-use-fixture-code"),
          );
          return Response.json({ appId: registered.appId, slug: registered.slug });
        }
        case "/pr":
          return yield* Effect.gen(function* () {
            const client = yield* RepairGithubClient;
            return Response.json(
              yield* client.pullRequest({
                repository: "fixture/project",
                repositoryId: 306,
                pullRequestNumber: 17,
              }),
            );
          }).pipe(Effect.provide(github));
        default:
          return new Response(null, { status: 404 });
      }
    }).pipe(
      Effect.catchTag("GithubWebhookRejected", (error) =>
        Effect.succeed(new Response(null, { status: error.status })),
      ),
    );
    const result = await Effect.runPromiseExit(
      operation.pipe(Effect.provide(FetchHttpClient.layer)),
    );
    return Exit.isSuccess(result) ? result.value : new Response(null, { status: 503 });
  },
};
