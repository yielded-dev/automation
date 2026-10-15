import {
  Context,
  DateTime,
  Effect,
  Layer,
  Option,
  Redacted,
  Result,
  Schema,
  Semaphore,
} from "effect";
import { Base64, Base64Url } from "effect/encoding";
import { HttpClient, HttpClientError, HttpClientRequest, HttpClientResponse } from "effect/http";

import { checkGithubRateLimit, GithubRateLimited } from "./GithubRateLimit";

/** Credentials bound to one auth instance at its composition root. */
export interface GithubAppCredentials {
  readonly appId: string;
  readonly installationId: string;
  readonly privateKey: Redacted.Redacted<string>;
}

export class GithubAppAuthError extends Schema.TaggedError<GithubAppAuthError>()(
  "GithubAppAuthError",
  {
    operation: Schema.String,
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/** Stable GitHub account and Git author identity for the app's bot. */
export interface GithubBotIdentity {
  readonly accountId: string;
  readonly name: string;
  readonly email: string;
}

const InstallationToken = Schema.Struct({
  token: Schema.String,
  expires_at: Schema.String,
});

const GithubApp = Schema.Struct({ slug: Schema.String.check(Schema.isPattern(/^[a-zA-Z0-9-]+$/)) });
const GithubUser = Schema.Struct({ id: Schema.Natural });
const GithubInstallation = Schema.Struct({
  id: Schema.Int.check(Schema.isGreaterThan(0)),
  account: Schema.Struct({
    login: Schema.NonEmptyString,
    type: Schema.Literals(["User", "Organization"]),
  }),
  permissions: Schema.Struct({
    contents: Schema.optionalKey(Schema.String),
    issues: Schema.optionalKey(Schema.String),
    pull_requests: Schema.optionalKey(Schema.String),
    workflows: Schema.optionalKey(Schema.String),
  }),
  suspended_at: Schema.NullOr(Schema.String),
});

const githubHeaders = {
  accept: "application/vnd.github+json",
  "user-agent": "reve-repairs",
  "x-github-api-version": "2022-11-28",
};

const encoder = new TextEncoder();

const authError = (operation: string) => (message: string, cause?: unknown) =>
  GithubAppAuthError.make({ operation: `GithubAppAuth.${operation}`, message, cause });

const responseError = (operation: string) => (cause: unknown) =>
  Schema.is(GithubRateLimited)(cause)
    ? cause
    : GithubAppAuthError.make({
        operation: `GithubAppAuth.${operation}`,
        message: "GitHub App request failed",
        cause,
      });

/** Strips PEM armor and decodes the base64 body to DER bytes. */
const decodePemBody = (pem: string): Uint8Array | undefined => {
  const body = pem
    .replace(/-----BEGIN [^-]+-----/g, "")
    .replace(/-----END [^-]+-----/g, "")
    .replace(/\s/g, "");

  return Result.getOrUndefined(Base64.decode(body));
};

// The secret is the base64-encoded PKCS#8 DER, but GitHub hands the key out
// as PEM, so tolerate a base64-encoded PEM that skipped the DER conversion.
const decodePrivateKey = (encoded: string): Uint8Array | undefined => {
  const decoded = Result.getOrUndefined(Base64.decode(encoded.replace(/\s/g, "")));

  if (decoded === undefined) return undefined;

  const text = new TextDecoder().decode(decoded);

  if (!text.startsWith("-----BEGIN")) return decoded;

  return decodePemBody(text);
};

const appJwt = Effect.fn("GithubAppAuth.appJwt")(function* (credentials: GithubAppCredentials) {
  const error = authError("appJwt");
  const keyBytes = decodePrivateKey(Redacted.value(credentials.privateKey));

  if (keyBytes === undefined) {
    return yield* error("The GitHub App private key is not a base64-encoded PKCS#8 key");
  }
  const key = yield* Effect.tryPromise({
    try: () =>
      crypto.subtle.importKey(
        "pkcs8",
        keyBytes as BufferSource,
        { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
        false,
        ["sign"],
      ),
    catch: (cause) => error("Could not import the GitHub App private key", cause),
  });
  const issuedAt = Math.floor(DateTime.toEpochMillis(yield* DateTime.now) / 1_000);
  // oxlint-disable-next-line no-restricted-properties -- fixed JOSE header segment of GitHub's external JWT protocol.
  const header = Base64Url.encode(JSON.stringify({ alg: "RS256", typ: "JWT" }));
  // Backdated iat absorbs clock skew; exp stays short of GitHub's 10-minute cap.
  const payload = Base64Url.encode(
    // oxlint-disable-next-line no-restricted-properties -- JWT claims segment of GitHub's external protocol.
    JSON.stringify({ iat: issuedAt - 60, exp: issuedAt + 540, iss: credentials.appId }),
  );
  const signingInput = `${header}.${payload}`;
  const signature = yield* Effect.tryPromise({
    try: () =>
      crypto.subtle.sign("RSASSA-PKCS1-v1_5", key, encoder.encode(signingInput) as BufferSource),
    catch: (cause) => error("Could not sign the GitHub App JWT", cause),
  });

  return `${signingInput}.${Base64Url.encode(new Uint8Array(signature))}`;
});

const mintInstallationToken = Effect.fn("GithubAppAuth.mintInstallationToken")(function* (
  credentials: GithubAppCredentials,
  repositoryId?: number,
  access:
    | "source"
    | "review"
    | "issues"
    | "checks"
    | "deployments"
    | "publication"
    | "discussion"
    | "actions" = "source",
  installationId: string = credentials.installationId,
  workflows = false,
  discussion: "issues" | "pull_requests" = "pull_requests",
) {
  const jwt = yield* appJwt(credentials);
  const request = HttpClientRequest.post(
    `https://api.github.com/app/installations/${installationId}/access_tokens`,
    { headers: { ...githubHeaders, authorization: `Bearer ${jwt}` } },
  );
  const response = yield* HttpClient.execute(
    repositoryId === undefined
      ? request
      : request.pipe(
          HttpClientRequest.bodyJsonUnsafe({
            repository_ids: [repositoryId],
            permissions:
              access === "discussion"
                ? { [discussion]: "write" }
                : access === "issues"
                  ? { issues: "read" }
                  : {
                      ...(access === "checks"
                        ? { checks: "read", statuses: "read", pull_requests: "read" }
                        : { contents: access === "publication" ? "write" : "read" }),
                      ...(access === "deployments" || access === "actions"
                        ? { deployments: "read" }
                        : {}),
                      ...(access === "actions" ? { actions: "write" } : {}),
                      ...(access === "publication" ? { pull_requests: "write" } : {}),
                      ...(access === "publication" && workflows ? { workflows: "write" } : {}),
                      ...(access === "review" ? { pull_requests: "read" } : {}),
                    },
          }),
        ),
  ).pipe(
    Effect.tap(checkGithubRateLimit),
    Effect.flatMap(HttpClientResponse.filterStatusOk),
    Effect.mapError(responseError("mintInstallationToken")),
  );
  const minted = yield* HttpClientResponse.schemaBodyJson(InstallationToken)(response).pipe(
    Effect.mapError(responseError("decodeInstallationToken")),
  );
  const expiresAt = DateTime.make(minted.expires_at);

  if (Option.isNone(expiresAt)) {
    return yield* authError("mintInstallationToken")("Invalid installation token expiry");
  }

  return { token: minted.token, expiresAtMillis: DateTime.toEpochMillis(expiresAt.value) };
});

// Installation tokens live one hour; refresh with margin so slow git
// operations don't outlive their credential mid-flight.
const refreshMarginMillis = 5 * 60 * 1_000;

export interface GithubAppAuthShape {
  /** Whether this instance owns a complete credential tuple. */
  readonly availability: "configured" | "unavailable";
  /** Credential-bound numeric App ID. Required by binding-aware adapters; optional for legacy hosts. */
  readonly appId?: string;
  readonly installationToken: () => Effect.Effect<
    string,
    GithubAppAuthError | GithubRateLimited,
    HttpClient.HttpClient
  >;
  /** Fresh read-only token restricted to exactly one repository and operation. */
  readonly repositoryReadToken: (
    repositoryId: number,
    access: "source" | "review" | "issues" | "checks" | "deployments",
    installationId?: string,
  ) => Effect.Effect<
    Redacted.Redacted<string>,
    GithubAppAuthError | GithubRateLimited,
    HttpClient.HttpClient
  >;
  /** Discussion tokens cannot write contents; publication retains its explicit workflow grant. */
  readonly repositoryWriteToken: (
    repositoryId: number,
    installationId: string,
    permissions?:
      | { readonly workflows: boolean }
      | { readonly discussion: "issues" | "pull_requests" },
  ) => Effect.Effect<
    Redacted.Redacted<string>,
    GithubAppAuthError | GithubRateLimited,
    HttpClient.HttpClient
  >;
  /**
   * Fresh credential restricted to one repository's workflow runs and deployments:
   * it can start and re-run workflows, never change contents. None when the
   * installation has not been granted the Actions permission.
   */
  readonly repositoryActionsToken: (
    repositoryId: number,
    installationId: string,
  ) => Effect.Effect<
    Option.Option<Redacted.Redacted<string>>,
    GithubAppAuthError | GithubRateLimited,
    HttpClient.HttpClient
  >;
  /** Resolve the App installation that actually includes this repository. */
  readonly repositoryInstallation: (
    repositoryUrl: string,
  ) => Effect.Effect<
    Option.Option<typeof GithubInstallation.Type>,
    GithubAppAuthError | GithubRateLimited,
    HttpClient.HttpClient
  >;
  /**
   * Replace the cached installation token only when it is still the token an
   * upstream request rejected. A concurrent request may already have refreshed
   * it; in that case reuse the newer token instead of invalidating it.
   */
  readonly refreshInstallationToken: (rejectedToken: string) => Effect.Effect<
    {
      readonly token: string;
      readonly disposition: "reminted" | "newer-token-reused";
    },
    GithubAppAuthError | GithubRateLimited,
    HttpClient.HttpClient
  >;
  /** Public install/configure screen for this credential-bound app. */
  readonly installationUrl: () => Effect.Effect<
    string,
    GithubAppAuthError | GithubRateLimited,
    HttpClient.HttpClient
  >;
  /** Login of this credential-bound app's bot, without resolving its Git identity. */
  readonly botLogin: () => Effect.Effect<
    string,
    GithubAppAuthError | GithubRateLimited,
    HttpClient.HttpClient
  >;
  /** Git committer identity of the installed app's bot user. */
  readonly botCommitter: (
    token: Redacted.Redacted<string>,
  ) => Effect.Effect<
    GithubBotIdentity,
    GithubAppAuthError | GithubRateLimited,
    HttpClient.HttpClient
  >;
}

export interface GithubAppAuthOptions {
  readonly mintInstallationToken?: () => Effect.Effect<
    { readonly token: string; readonly expiresAtMillis: number },
    GithubAppAuthError | GithubRateLimited,
    HttpClient.HttpClient
  >;
}

export const makeGithubAppAuth = (
  credentials: GithubAppCredentials,
  options: GithubAppAuthOptions = {},
): GithubAppAuthShape => {
  const mintToken = options.mintInstallationToken ?? (() => mintInstallationToken(credentials));
  let minted: { readonly token: string; readonly expiresAtMillis: number } | undefined;
  const mintLock = Semaphore.makeUnsafe(1);
  const freshToken = Effect.gen(function* () {
    const now = DateTime.toEpochMillis(yield* DateTime.now);

    return minted !== undefined && now < minted.expiresAtMillis - refreshMarginMillis
      ? minted.token
      : undefined;
  });

  const installationToken = Effect.fn("GithubAppAuth.installationToken")(function* () {
    const cached = yield* freshToken;

    if (cached !== undefined) return cached;

    return yield* Semaphore.withPermit(
      mintLock,
      Effect.gen(function* () {
        // Whoever held the permit before us usually minted for everyone.
        const refreshed = yield* freshToken;

        if (refreshed !== undefined) return refreshed;

        const next = yield* mintToken();

        minted = next;

        return next.token;
      }),
    );
  });

  const refreshInstallationToken = Effect.fn("GithubAppAuth.refreshInstallationToken")(function* (
    rejectedToken: string,
  ) {
    return yield* Semaphore.withPermit(
      mintLock,
      Effect.gen(function* () {
        const current = yield* freshToken;

        if (current !== undefined && minted?.token !== rejectedToken) {
          return { token: current, disposition: "newer-token-reused" as const };
        }

        // Do not leave a credential that GitHub has rejected eligible for a
        // later cache hit when minting its replacement fails.
        minted = undefined;
        const next = yield* mintToken();

        minted = next;

        return { token: next.token, disposition: "reminted" as const };
      }),
    );
  });

  // The app slug and its bot user id never change for an installation, so
  // one resolution serves the auth instance's lifetime.
  let bot: GithubBotIdentity | undefined;
  const botLock = Semaphore.makeUnsafe(1);

  const fetchApp = Effect.fn("GithubAppAuth.fetchApp")(function* () {
    const jwt = yield* appJwt(credentials);
    const appResponse = yield* HttpClient.get("https://api.github.com/app", {
      headers: { ...githubHeaders, authorization: `Bearer ${jwt}` },
    }).pipe(
      Effect.tap(checkGithubRateLimit),
      Effect.flatMap(HttpClientResponse.filterStatusOk),
      Effect.mapError(responseError("fetchApp")),
    );
    const app = yield* HttpClientResponse.schemaBodyJson(GithubApp)(appResponse).pipe(
      Effect.mapError(responseError("decodeApp")),
    );

    return app;
  });

  const botLogin = Effect.fn("GithubAppAuth.botLogin")(function* () {
    const app = yield* fetchApp();

    return `${app.slug}[bot]`;
  });

  const fetchBotCommitter = Effect.fn("GithubAppAuth.fetchBotCommitter")(function* (
    token: Redacted.Redacted<string>,
  ) {
    const login = yield* botLogin();
    const userResponse = yield* HttpClient.get(
      `https://api.github.com/users/${encodeURIComponent(login)}`,
      { headers: { ...githubHeaders, authorization: `Bearer ${Redacted.value(token)}` } },
    ).pipe(
      Effect.tap(checkGithubRateLimit),
      Effect.flatMap(HttpClientResponse.filterStatusOk),
      Effect.mapError(responseError("fetchBotUser")),
    );
    const user = yield* HttpClientResponse.schemaBodyJson(GithubUser)(userResponse).pipe(
      Effect.mapError(responseError("decodeBotUser")),
    );

    return {
      accountId: String(user.id),
      name: login,
      email: `${user.id}+${login}@users.noreply.github.com`,
    } satisfies GithubBotIdentity;
  });

  const botCommitter = Effect.fn("GithubAppAuth.botCommitter")(function* (
    token: Redacted.Redacted<string>,
  ) {
    if (bot !== undefined) return bot;

    return yield* Semaphore.withPermit(
      botLock,
      Effect.gen(function* () {
        if (bot !== undefined) return bot;

        const resolved = yield* fetchBotCommitter(token);

        bot = resolved;

        return resolved;
      }),
    );
  });

  return {
    availability: "configured",
    appId: credentials.appId,
    installationUrl: () =>
      fetchApp().pipe(Effect.map((app) => `https://github.com/apps/${app.slug}/installations/new`)),
    installationToken,
    repositoryInstallation: Effect.fn("GithubAppAuth.repositoryInstallation")(
      function* (repositoryUrl) {
        const slug = repositoryUrl.slice("https://github.com/".length).replace(/\.git$/u, "");
        const jwt = yield* appJwt(credentials);
        const response = yield* HttpClient.get(
          `https://api.github.com/repos/${slug.split("/").map(encodeURIComponent).join("/")}/installation`,
          { headers: { ...githubHeaders, authorization: `Bearer ${jwt}` } },
        ).pipe(
          Effect.tap(checkGithubRateLimit),
          Effect.mapError(responseError("repositoryInstallation")),
        );

        if (response.status === 404) return Option.none();
        yield* HttpClientResponse.filterStatusOk(response).pipe(
          Effect.mapError(responseError("repositoryInstallation")),
        );

        return yield* HttpClientResponse.schemaBodyJson(GithubInstallation)(response).pipe(
          Effect.map(Option.some),
          Effect.mapError(responseError("decodeRepositoryInstallation")),
        );
      },
    ),
    repositoryReadToken: (repositoryId, access, installationId) =>
      mintInstallationToken(credentials, repositoryId, access, installationId).pipe(
        Effect.map(({ token }) => Redacted.make(token)),
      ),
    repositoryWriteToken: (repositoryId, installationId, permissions) =>
      mintInstallationToken(
        credentials,
        repositoryId,
        permissions !== undefined && "discussion" in permissions ? "discussion" : "publication",
        installationId,
        permissions !== undefined && "workflows" in permissions ? permissions.workflows : false,
        permissions !== undefined && "discussion" in permissions
          ? permissions.discussion
          : undefined,
      ).pipe(Effect.map(({ token }) => Redacted.make(token))),
    repositoryActionsToken: (repositoryId, installationId) =>
      mintInstallationToken(credentials, repositoryId, "actions", installationId).pipe(
        Effect.map(({ token }) => Option.some(Redacted.make(token))),
        // GitHub refuses permissions an installation was never granted with 422.
        Effect.catchIf(
          (error) =>
            Schema.is(GithubAppAuthError)(error) &&
            HttpClientError.isHttpClientError(error.cause) &&
            error.cause.response?.status === 422,
          () => Effect.succeed(Option.none()),
        ),
      ),
    refreshInstallationToken,
    botLogin,
    botCommitter,
  };
};

/** @effect-expect-leaking HttpClient */
export class GithubAppAuth extends Context.Service<GithubAppAuth, GithubAppAuthShape>()(
  "kommunikasie/GithubAppAuth",
) {
  /** Every layer build owns one credential-bound cache and synchronization boundary. */
  static readonly layer = (credentials: GithubAppCredentials, options: GithubAppAuthOptions = {}) =>
    Layer.sync(GithubAppAuth, () => makeGithubAppAuth(credentials, options));

  /** Local composition for an intentionally absent credential tuple. */
  static readonly layerUnavailable = Layer.succeed(
    GithubAppAuth,
    GithubAppAuth.of({
      availability: "unavailable",
      installationUrl: () =>
        Effect.fail(authError("installationUrl")("GitHub App authentication is not configured")),
      installationToken: () =>
        Effect.fail(
          GithubAppAuthError.make({
            operation: "GithubAppAuth.installationToken",
            message: "GitHub App authentication is not configured",
          }),
        ),
      repositoryReadToken: () =>
        Effect.fail(
          GithubAppAuthError.make({
            operation: "GithubAppAuth.repositoryReadToken",
            message: "GitHub App authentication is not configured",
          }),
        ),
      repositoryWriteToken: () =>
        Effect.fail(
          authError("repositoryWriteToken")("GitHub App authentication is not configured"),
        ),
      repositoryActionsToken: () =>
        Effect.fail(
          authError("repositoryActionsToken")("GitHub App authentication is not configured"),
        ),
      repositoryInstallation: () =>
        Effect.fail(
          authError("repositoryInstallation")("GitHub App authentication is not configured"),
        ),
      refreshInstallationToken: () =>
        Effect.fail(
          GithubAppAuthError.make({
            operation: "GithubAppAuth.refreshInstallationToken",
            message: "GitHub App authentication is not configured",
          }),
        ),
      botLogin: () =>
        Effect.fail(authError("botLogin")("GitHub App authentication is not configured")),
      botCommitter: () =>
        Effect.fail(
          GithubAppAuthError.make({
            operation: "GithubAppAuth.botCommitter",
            message: "GitHub App authentication is not configured",
          }),
        ),
    }),
  );
}
