import { decodeGithubEvent } from "../domain/RepairGithub.mjs";
import { verifyGithubSignature } from "../server/RepairGithubWebhook.mjs";
import { Effect, Redacted, Schema, Stream } from "effect";
//#region src/github/Webhook.ts
const githubWebhookMaxBytes = 1048576;
var GithubWebhookRejected = class extends Schema.TaggedError()("GithubWebhookRejected", {
	status: Schema.Literals([
		400,
		401,
		405,
		413
	]),
	cause: Schema.optionalKey(Schema.Redacted(Schema.Unknown, { disallowJsonEncode: true }))
}) {};
/** Verified facts only. The host maps repository/installation to its own authority and durably enqueues a wake before acknowledging it. */
const readGithubWebhook = Effect.fn("Github.readWebhook")(function* (request, secret) {
	if (request.method !== "POST") return yield* GithubWebhookRejected.make({ status: 405 });
	const bytes = new Uint8Array(githubWebhookMaxBytes);
	const body = request.body;
	const size = body === null ? 0 : yield* Stream.runFoldEffect(Stream.fromReadableStream({
		evaluate: () => body,
		onError: (cause) => GithubWebhookRejected.make({
			status: 400,
			cause: Redacted.make(cause)
		})
	}), () => 0, (size, chunk) => {
		if (size + chunk.byteLength > 1048576) return Effect.fail(GithubWebhookRejected.make({ status: 413 }));
		bytes.set(chunk, size);
		return Effect.succeed(size + chunk.byteLength);
	});
	const text = yield* Effect.try({
		try: () => new TextDecoder("utf-8", {
			fatal: true,
			ignoreBOM: true
		}).decode(bytes.subarray(0, size)),
		catch: (cause) => GithubWebhookRejected.make({
			status: 400,
			cause: Redacted.make(cause)
		})
	});
	if (!(yield* verifyGithubSignature(text, request.headers.get("x-hub-signature-256"), secret))) return yield* GithubWebhookRejected.make({ status: 401 });
	return yield* decodeGithubEvent(request.headers.get("x-github-event") ?? "", text).pipe(Effect.mapError((cause) => GithubWebhookRejected.make({
		status: 400,
		cause: Redacted.make(cause)
	})));
});
//#endregion
export { GithubWebhookRejected, githubWebhookMaxBytes, readGithubWebhook };
