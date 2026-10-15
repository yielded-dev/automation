import { Effect, Schema } from "effect";

const HttpsUrl = Schema.String.check(
  Schema.isMaxLength(2048),
  Schema.makeFilter((value) => {
    const url = URL.parse(value);
    return url !== null && url.protocol === "https:" && !url.username && !url.password && !url.hash;
  }),
);

export const GithubAppManifestOptions = Schema.Struct({
  name: Schema.NonEmptyString.check(Schema.isMaxLength(100)),
  publicOrigin: HttpsUrl,
  webhookUrl: HttpsUrl,
  redirectUrl: HttpsUrl,
  setupUrl: HttpsUrl,
  public: Schema.Boolean,
  /** Enables candidate publication; discussion-only installations do not publish candidates. */
  publishChanges: Schema.Boolean,
  /** Editing workflow files requires an explicit grant, independent of running Actions. */
  workflows: Schema.optionalKey(Schema.Boolean),
}).check(
  Schema.makeFilter(
    (input) =>
      !input.workflows || input.publishChanges || "Workflow editing requires candidate publication",
  ),
);
export type GithubAppManifestOptions = typeof GithubAppManifestOptions.Type;

/** Pure configuration generation: the host starts registration and validates callback state. */
export const githubAppManifest = Effect.fnUntraced(function* (options: GithubAppManifestOptions) {
  const input = yield* Schema.decodeEffect(GithubAppManifestOptions)(options);
  return {
    name: input.name,
    url: input.publicOrigin,
    hook_attributes: { url: input.webhookUrl, active: true },
    redirect_url: input.redirectUrl,
    setup_url: input.setupUrl,
    public: input.public,
    request_oauth_on_install: false,
    setup_on_update: true,
    default_permissions: {
      contents: input.publishChanges ? ("write" as const) : ("read" as const),
      issues: "write" as const,
      pull_requests: input.publishChanges ? ("write" as const) : ("read" as const),
      checks: "read" as const,
      statuses: "read" as const,
      ...(input.publishChanges && input.workflows ? { workflows: "write" as const } : {}),
    },
    // GitHub sends installation lifecycle events independently of this subscription list.
    default_events: [
      "issue_comment",
      "pull_request",
      "pull_request_review",
      "pull_request_review_comment",
      "push",
      "check_run",
      "check_suite",
      "status",
    ],
  };
});

export type GithubAppManifest = Effect.Success<ReturnType<typeof githubAppManifest>>;
