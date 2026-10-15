import { GithubRateLimited, checkGithubRateLimit } from "./GithubRateLimit.mjs";
import { Context, DateTime, Effect, Layer, Option, Redacted, Result, Schema, Semaphore } from "effect";
import { Base64, Base64Url } from "effect/encoding";
import { HttpClient, HttpClientError, HttpClientRequest, HttpClientResponse } from "effect/http";
//#region src/server/GithubAppAuth.ts
var GithubAppAuthError = class extends Schema.TaggedError()("GithubAppAuthError", {
	operation: Schema.String,
	message: Schema.String,
	cause: Schema.optionalKey(Schema.Defect())
}) {};
const InstallationToken = Schema.Struct({
	token: Schema.String,
	expires_at: Schema.String
});
const GithubApp = Schema.Struct({ slug: Schema.String.check(Schema.isPattern(/^[a-zA-Z0-9-]+$/)) });
const GithubUser = Schema.Struct({ id: Schema.Natural });
const GithubInstallation = Schema.Struct({
	id: Schema.Int.check(Schema.isGreaterThan(0)),
	account: Schema.Struct({
		login: Schema.NonEmptyString,
		type: Schema.Literals(["User", "Organization"])
	}),
	permissions: Schema.Struct({
		contents: Schema.optionalKey(Schema.String),
		issues: Schema.optionalKey(Schema.String),
		pull_requests: Schema.optionalKey(Schema.String),
		workflows: Schema.optionalKey(Schema.String)
	}),
	suspended_at: Schema.NullOr(Schema.String)
});
const githubHeaders = {
	accept: "application/vnd.github+json",
	"user-agent": "reve-repairs",
	"x-github-api-version": "2022-11-28"
};
const encoder = new TextEncoder();
const authError = (operation) => (message, cause) => GithubAppAuthError.make({
	operation: `GithubAppAuth.${operation}`,
	message,
	cause
});
const responseError = (operation) => (cause) => Schema.is(GithubRateLimited)(cause) ? cause : GithubAppAuthError.make({
	operation: `GithubAppAuth.${operation}`,
	message: "GitHub App request failed",
	cause
});
/** Strips PEM armor and decodes the base64 body to DER bytes. */
const decodePemBody = (pem) => {
	const body = pem.replace(/-----BEGIN [^-]+-----/g, "").replace(/-----END [^-]+-----/g, "").replace(/\s/g, "");
	return Result.getOrUndefined(Base64.decode(body));
};
const decodePrivateKey = (encoded) => {
	const decoded = Result.getOrUndefined(Base64.decode(encoded.replace(/\s/g, "")));
	if (decoded === void 0) return void 0;
	const text = new TextDecoder().decode(decoded);
	if (!text.startsWith("-----BEGIN")) return decoded;
	return decodePemBody(text);
};
const appJwt = Effect.fn("GithubAppAuth.appJwt")(function* (credentials) {
	const error = authError("appJwt");
	const keyBytes = decodePrivateKey(Redacted.value(credentials.privateKey));
	if (keyBytes === void 0) return yield* error("The GitHub App private key is not a base64-encoded PKCS#8 key");
	const key = yield* Effect.tryPromise({
		try: () => crypto.subtle.importKey("pkcs8", keyBytes, {
			name: "RSASSA-PKCS1-v1_5",
			hash: "SHA-256"
		}, false, ["sign"]),
		catch: (cause) => error("Could not import the GitHub App private key", cause)
	});
	const issuedAt = Math.floor(DateTime.toEpochMillis(yield* DateTime.now) / 1e3);
	const signingInput = `${Base64Url.encode(JSON.stringify({
		alg: "RS256",
		typ: "JWT"
	}))}.${Base64Url.encode(JSON.stringify({
		iat: issuedAt - 60,
		exp: issuedAt + 540,
		iss: credentials.appId
	}))}`;
	const signature = yield* Effect.tryPromise({
		try: () => crypto.subtle.sign("RSASSA-PKCS1-v1_5", key, encoder.encode(signingInput)),
		catch: (cause) => error("Could not sign the GitHub App JWT", cause)
	});
	return `${signingInput}.${Base64Url.encode(new Uint8Array(signature))}`;
});
const mintInstallationToken = Effect.fn("GithubAppAuth.mintInstallationToken")(function* (credentials, repositoryId, access = "source", installationId = credentials.installationId, workflows = false, discussion = "pull_requests") {
	const jwt = yield* appJwt(credentials);
	const request = HttpClientRequest.post(`https://api.github.com/app/installations/${installationId}/access_tokens`, { headers: {
		...githubHeaders,
		authorization: `Bearer ${jwt}`
	} });
	const response = yield* HttpClient.execute(repositoryId === void 0 ? request : request.pipe(HttpClientRequest.bodyJsonUnsafe({
		repository_ids: [repositoryId],
		permissions: access === "discussion" ? { [discussion]: "write" } : access === "issues" ? { issues: "read" } : {
			...access === "checks" ? {
				checks: "read",
				statuses: "read",
				pull_requests: "read"
			} : { contents: access === "publication" ? "write" : "read" },
			...access === "deployments" || access === "actions" ? { deployments: "read" } : {},
			...access === "actions" ? { actions: "write" } : {},
			...access === "publication" ? { pull_requests: "write" } : {},
			...access === "publication" && workflows ? { workflows: "write" } : {},
			...access === "review" ? { pull_requests: "read" } : {}
		}
	}))).pipe(Effect.tap(checkGithubRateLimit), Effect.flatMap(HttpClientResponse.filterStatusOk), Effect.mapError(responseError("mintInstallationToken")));
	const minted = yield* HttpClientResponse.schemaBodyJson(InstallationToken)(response).pipe(Effect.mapError(responseError("decodeInstallationToken")));
	const expiresAt = DateTime.make(minted.expires_at);
	if (Option.isNone(expiresAt)) return yield* authError("mintInstallationToken")("Invalid installation token expiry");
	return {
		token: minted.token,
		expiresAtMillis: DateTime.toEpochMillis(expiresAt.value)
	};
});
const refreshMarginMillis = 3e5;
const makeGithubAppAuth = (credentials, options = {}) => {
	const mintToken = options.mintInstallationToken ?? (() => mintInstallationToken(credentials));
	let minted;
	const mintLock = Semaphore.makeUnsafe(1);
	const freshToken = Effect.gen(function* () {
		const now = DateTime.toEpochMillis(yield* DateTime.now);
		return minted !== void 0 && now < minted.expiresAtMillis - refreshMarginMillis ? minted.token : void 0;
	});
	const installationToken = Effect.fn("GithubAppAuth.installationToken")(function* () {
		const cached = yield* freshToken;
		if (cached !== void 0) return cached;
		return yield* Semaphore.withPermit(mintLock, Effect.gen(function* () {
			const refreshed = yield* freshToken;
			if (refreshed !== void 0) return refreshed;
			const next = yield* mintToken();
			minted = next;
			return next.token;
		}));
	});
	const refreshInstallationToken = Effect.fn("GithubAppAuth.refreshInstallationToken")(function* (rejectedToken) {
		return yield* Semaphore.withPermit(mintLock, Effect.gen(function* () {
			const current = yield* freshToken;
			if (current !== void 0 && minted?.token !== rejectedToken) return {
				token: current,
				disposition: "newer-token-reused"
			};
			minted = void 0;
			const next = yield* mintToken();
			minted = next;
			return {
				token: next.token,
				disposition: "reminted"
			};
		}));
	});
	let bot;
	const botLock = Semaphore.makeUnsafe(1);
	const fetchApp = Effect.fn("GithubAppAuth.fetchApp")(function* () {
		const jwt = yield* appJwt(credentials);
		const appResponse = yield* HttpClient.get("https://api.github.com/app", { headers: {
			...githubHeaders,
			authorization: `Bearer ${jwt}`
		} }).pipe(Effect.tap(checkGithubRateLimit), Effect.flatMap(HttpClientResponse.filterStatusOk), Effect.mapError(responseError("fetchApp")));
		return yield* HttpClientResponse.schemaBodyJson(GithubApp)(appResponse).pipe(Effect.mapError(responseError("decodeApp")));
	});
	const botLogin = Effect.fn("GithubAppAuth.botLogin")(function* () {
		return `${(yield* fetchApp()).slug}[bot]`;
	});
	const fetchBotCommitter = Effect.fn("GithubAppAuth.fetchBotCommitter")(function* (token) {
		const login = yield* botLogin();
		const userResponse = yield* HttpClient.get(`https://api.github.com/users/${encodeURIComponent(login)}`, { headers: {
			...githubHeaders,
			authorization: `Bearer ${Redacted.value(token)}`
		} }).pipe(Effect.tap(checkGithubRateLimit), Effect.flatMap(HttpClientResponse.filterStatusOk), Effect.mapError(responseError("fetchBotUser")));
		const user = yield* HttpClientResponse.schemaBodyJson(GithubUser)(userResponse).pipe(Effect.mapError(responseError("decodeBotUser")));
		return {
			accountId: String(user.id),
			name: login,
			email: `${user.id}+${login}@users.noreply.github.com`
		};
	});
	const botCommitter = Effect.fn("GithubAppAuth.botCommitter")(function* (token) {
		if (bot !== void 0) return bot;
		return yield* Semaphore.withPermit(botLock, Effect.gen(function* () {
			if (bot !== void 0) return bot;
			const resolved = yield* fetchBotCommitter(token);
			bot = resolved;
			return resolved;
		}));
	});
	return {
		availability: "configured",
		appId: credentials.appId,
		installationUrl: () => fetchApp().pipe(Effect.map((app) => `https://github.com/apps/${app.slug}/installations/new`)),
		installationToken,
		repositoryInstallation: Effect.fn("GithubAppAuth.repositoryInstallation")(function* (repositoryUrl) {
			const slug = repositoryUrl.slice(19).replace(/\.git$/u, "");
			const jwt = yield* appJwt(credentials);
			const response = yield* HttpClient.get(`https://api.github.com/repos/${slug.split("/").map(encodeURIComponent).join("/")}/installation`, { headers: {
				...githubHeaders,
				authorization: `Bearer ${jwt}`
			} }).pipe(Effect.tap(checkGithubRateLimit), Effect.mapError(responseError("repositoryInstallation")));
			if (response.status === 404) return Option.none();
			yield* HttpClientResponse.filterStatusOk(response).pipe(Effect.mapError(responseError("repositoryInstallation")));
			return yield* HttpClientResponse.schemaBodyJson(GithubInstallation)(response).pipe(Effect.map(Option.some), Effect.mapError(responseError("decodeRepositoryInstallation")));
		}),
		repositoryReadToken: (repositoryId, access, installationId) => mintInstallationToken(credentials, repositoryId, access, installationId).pipe(Effect.map(({ token }) => Redacted.make(token))),
		repositoryWriteToken: (repositoryId, installationId, permissions) => mintInstallationToken(credentials, repositoryId, permissions !== void 0 && "discussion" in permissions ? "discussion" : "publication", installationId, permissions !== void 0 && "workflows" in permissions ? permissions.workflows : false, permissions !== void 0 && "discussion" in permissions ? permissions.discussion : void 0).pipe(Effect.map(({ token }) => Redacted.make(token))),
		repositoryActionsToken: (repositoryId, installationId) => mintInstallationToken(credentials, repositoryId, "actions", installationId).pipe(Effect.map(({ token }) => Option.some(Redacted.make(token))), Effect.catchIf((error) => Schema.is(GithubAppAuthError)(error) && HttpClientError.isHttpClientError(error.cause) && error.cause.response?.status === 422, () => Effect.succeed(Option.none()))),
		refreshInstallationToken,
		botLogin,
		botCommitter
	};
};
/** @effect-expect-leaking HttpClient */
var GithubAppAuth = class GithubAppAuth extends Context.Service()("kommunikasie/GithubAppAuth") {
	/** Every layer build owns one credential-bound cache and synchronization boundary. */
	static layer = (credentials, options = {}) => Layer.sync(GithubAppAuth, () => makeGithubAppAuth(credentials, options));
	/** Local composition for an intentionally absent credential tuple. */
	static layerUnavailable = Layer.succeed(GithubAppAuth, GithubAppAuth.of({
		availability: "unavailable",
		installationUrl: () => Effect.fail(authError("installationUrl")("GitHub App authentication is not configured")),
		installationToken: () => Effect.fail(GithubAppAuthError.make({
			operation: "GithubAppAuth.installationToken",
			message: "GitHub App authentication is not configured"
		})),
		repositoryReadToken: () => Effect.fail(GithubAppAuthError.make({
			operation: "GithubAppAuth.repositoryReadToken",
			message: "GitHub App authentication is not configured"
		})),
		repositoryWriteToken: () => Effect.fail(authError("repositoryWriteToken")("GitHub App authentication is not configured")),
		repositoryActionsToken: () => Effect.fail(authError("repositoryActionsToken")("GitHub App authentication is not configured")),
		repositoryInstallation: () => Effect.fail(authError("repositoryInstallation")("GitHub App authentication is not configured")),
		refreshInstallationToken: () => Effect.fail(GithubAppAuthError.make({
			operation: "GithubAppAuth.refreshInstallationToken",
			message: "GitHub App authentication is not configured"
		})),
		botLogin: () => Effect.fail(authError("botLogin")("GitHub App authentication is not configured")),
		botCommitter: () => Effect.fail(GithubAppAuthError.make({
			operation: "GithubAppAuth.botCommitter",
			message: "GitHub App authentication is not configured"
		}))
	}));
};
//#endregion
export { GithubAppAuth, GithubAppAuthError, makeGithubAppAuth };
