import { Cause, Effect, Redacted, Schema } from "effect";
import { FetchHttpClient, HttpClient, HttpClientRequest, HttpClientResponse } from "effect/http";

const GithubId = Schema.Int.check(
  Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }),
);

export class GithubRegistrationError extends Schema.TaggedError<GithubRegistrationError>()(
  "GithubRegistrationError",
  { cause: Schema.Redacted(Schema.Unknown, { disallowJsonEncode: true }) },
) {}

const Registration = Schema.Struct({
  id: GithubId,
  slug: Schema.NonEmptyString.check(Schema.isMaxLength(100)),
  client_id: Schema.NonEmptyString,
  client_secret: Schema.NonEmptyString,
  webhook_secret: Schema.NonEmptyString,
  pem: Schema.NonEmptyString,
});
export interface GithubAppRegistration {
  readonly appId: number;
  readonly slug: string;
  readonly clientId: string;
  readonly clientSecret: Redacted.Redacted<string>;
  readonly webhookSecret: Redacted.Redacted<string>;
  /** Preserve the returned PEM. GithubAppAuth requires host-normalized base64 PKCS#8. */
  readonly privateKeyPem: Redacted.Redacted<string>;
}

/** Explicit one-shot exchange; never retry an ambiguous conversion automatically. */
export const exchangeGithubAppManifest = Effect.fn("Github.exchangeAppManifest")(function* (
  code: Redacted.Redacted<string>,
): Effect.fn.Return<GithubAppRegistration, GithubRegistrationError, HttpClient.HttpClient> {
  const acceptedCode = yield* Schema.decodeUnknownEffect(
    Schema.String.check(Schema.isPattern(/^[a-zA-Z0-9_-]{1,256}$/)),
  )(Redacted.value(code)).pipe(
    Effect.catchCause((cause) =>
      Effect.failCause(
        Cause.map(cause, (error) => GithubRegistrationError.make({ cause: Redacted.make(error) })),
      ),
    ),
  );
  const registration = yield* HttpClient.execute(
    HttpClientRequest.post(`https://api.github.com/app-manifests/${acceptedCode}/conversions`, {
      headers: {
        accept: "application/vnd.github+json",
        "user-agent": "reve-repairs",
        "x-github-api-version": "2022-11-28",
      },
    }),
  ).pipe(
    Effect.flatMap(HttpClientResponse.filterStatusOk),
    Effect.flatMap(HttpClientResponse.schemaBodyJson(Registration)),
    Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual" }),
    // The one-use conversion code is a credential embedded in GitHub's URL path.
    Effect.provideService(HttpClient.TracerDisabledWhen, () => true),
    Effect.timeout("10 seconds"),
    Effect.catchCause((cause) =>
      Effect.failCause(
        Cause.map(cause, (error) => GithubRegistrationError.make({ cause: Redacted.make(error) })),
      ),
    ),
  );
  return {
    appId: registration.id,
    slug: registration.slug,
    clientId: registration.client_id,
    clientSecret: Redacted.make(registration.client_secret),
    webhookSecret: Redacted.make(registration.webhook_secret),
    privateKeyPem: Redacted.make(registration.pem),
  };
});
