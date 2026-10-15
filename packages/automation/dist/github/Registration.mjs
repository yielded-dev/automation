import { Cause, Effect, Redacted, Schema } from "effect";
import { FetchHttpClient, HttpClient, HttpClientRequest, HttpClientResponse } from "effect/http";
//#region src/github/Registration.ts
const GithubId = Schema.Int.check(Schema.isBetween({
	minimum: 1,
	maximum: Number.MAX_SAFE_INTEGER
}));
var GithubRegistrationError = class extends Schema.TaggedError()("GithubRegistrationError", { cause: Schema.Redacted(Schema.Unknown, { disallowJsonEncode: true }) }) {};
const Registration = Schema.Struct({
	id: GithubId,
	slug: Schema.NonEmptyString.check(Schema.isMaxLength(100)),
	client_id: Schema.NonEmptyString,
	client_secret: Schema.NonEmptyString,
	webhook_secret: Schema.NonEmptyString,
	pem: Schema.NonEmptyString
});
/** Explicit one-shot exchange; never retry an ambiguous conversion automatically. */
const exchangeGithubAppManifest = Effect.fn("Github.exchangeAppManifest")(function* (code) {
	const acceptedCode = yield* Schema.decodeUnknownEffect(Schema.String.check(Schema.isPattern(/^[a-zA-Z0-9_-]{1,256}$/)))(Redacted.value(code)).pipe(Effect.catchCause((cause) => Effect.failCause(Cause.map(cause, (error) => GithubRegistrationError.make({ cause: Redacted.make(error) })))));
	const registration = yield* HttpClient.execute(HttpClientRequest.post(`https://api.github.com/app-manifests/${acceptedCode}/conversions`, { headers: {
		accept: "application/vnd.github+json",
		"user-agent": "reve-repairs",
		"x-github-api-version": "2022-11-28"
	} })).pipe(Effect.flatMap(HttpClientResponse.filterStatusOk), Effect.flatMap(HttpClientResponse.schemaBodyJson(Registration)), Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual" }), Effect.provideService(HttpClient.TracerDisabledWhen, () => true), Effect.timeout("10 seconds"), Effect.catchCause((cause) => Effect.failCause(Cause.map(cause, (error) => GithubRegistrationError.make({ cause: Redacted.make(error) })))));
	return {
		appId: registration.id,
		slug: registration.slug,
		clientId: registration.client_id,
		clientSecret: Redacted.make(registration.client_secret),
		webhookSecret: Redacted.make(registration.webhook_secret),
		privateKeyPem: Redacted.make(registration.pem)
	};
});
//#endregion
export { GithubRegistrationError, exchangeGithubAppManifest };
