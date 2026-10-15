import { generateKeyPairSync } from "node:crypto";
import { Effect, Option, Redacted } from "effect";
import { HttpClient, HttpClientResponse } from "effect/http";
import { expect, it } from "vite-plus/test";

import { makeGithubAppAuth } from "../../packages/automation/src/server/GithubAppAuth";

it("retains an approved workflow grant in the repository-scoped publication token", async () => {
  const { privateKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
  const auth = makeGithubAppAuth({
    appId: "1",
    installationId: "2",
    privateKey: Redacted.make(
      privateKey.export({ type: "pkcs8", format: "der" }).toString("base64"),
    ),
  });
  const tokens: unknown[] = [];
  const client = HttpClient.make((request, url) => {
    if (url.pathname === "/repos/fixture/project/installation")
      return Effect.succeed(
        HttpClientResponse.fromWeb(
          request,
          Response.json({
            id: 2,
            account: { login: "fixture", type: "Organization" },
            permissions: { contents: "write", pull_requests: "write", workflows: "write" },
            suspended_at: null,
          }),
        ),
      );
    if (url.pathname !== "/app/installations/2/access_tokens" || request.body._tag !== "Uint8Array")
      return Effect.die("Unexpected token request");
    tokens.push(JSON.parse(new TextDecoder().decode(request.body.body)));
    return Effect.succeed(
      HttpClientResponse.fromWeb(
        request,
        Response.json({ token: "synthetic", expires_at: "2099-01-01T00:00:00Z" }),
      ),
    );
  });

  await Effect.runPromise(
    Effect.gen(function* () {
      const installation = yield* auth.repositoryInstallation("https://github.com/fixture/project");
      if (Option.isNone(installation)) return yield* Effect.die("Missing installation");
      yield* auth.repositoryWriteToken(3, "2", {
        workflows: installation.value.permissions.workflows === "write",
      });
      yield* auth.repositoryWriteToken(3, "2");
      yield* auth.repositoryWriteToken(3, "2", { workflows: false });
      yield* auth.repositoryReadToken(3, "source", "2");
    }).pipe(Effect.provideService(HttpClient.HttpClient, client)),
  );
  expect(tokens).toEqual([
    {
      repository_ids: [3],
      permissions: { contents: "write", pull_requests: "write", workflows: "write" },
    },
    { repository_ids: [3], permissions: { contents: "write", pull_requests: "write" } },
    { repository_ids: [3], permissions: { contents: "write", pull_requests: "write" } },
    { repository_ids: [3], permissions: { contents: "read" } },
  ]);
});
