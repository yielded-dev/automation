import "../SessionSandbox.mjs";
import "./ArtifactsFork.mjs";
import { ArtifactsCheckout } from "./ArtifactsCheckout.mjs";
import { Context, Effect, Layer, Schema } from "effect";
//#region src/cloudflare/RepositorySourceCheckout.ts
/** The restored repository did not match the source accepted by the host. */
var RepositorySourceRestoreHeadMismatchError = class extends Schema.TaggedError()("RepositorySourceRestoreHeadMismatchError", {
	repositoryName: Schema.String,
	acceptedSourceSha: Schema.String,
	restoredHead: Schema.String
}) {};
/**
* Restricted generation-bound repository checkout capability for one sandbox turn.
* @effect-expect-leaking SessionSandbox
*/
var RepositorySourceCheckout = class RepositorySourceCheckout extends Context.Service()("kommunikasie/RepositorySourceCheckout") {
	static layer = Layer.effect(RepositorySourceCheckout, Effect.gen(function* () {
		const checkout = yield* ArtifactsCheckout;
		const forSource = (access) => {
			const generation = {
				repositoryName: access.repositoryName,
				repositoryId: access.repositoryId
			};
			const verifySource = Effect.gen(function* () {
				const head = yield* checkout.readGenerationMainHead(generation);
				if (head !== access.acceptedSourceSha) return yield* RepositorySourceRestoreHeadMismatchError.make({
					repositoryName: access.repositoryName,
					acceptedSourceSha: access.acceptedSourceSha,
					restoredHead: head
				});
			});
			const restore = Effect.gen(function* () {
				const restored = yield* checkout.cloneGenerationIntoSandbox(generation);
				if (restored.head !== access.acceptedSourceSha) return yield* RepositorySourceRestoreHeadMismatchError.make({
					repositoryName: access.repositoryName,
					acceptedSourceSha: access.acceptedSourceSha,
					restoredHead: restored.head
				});
			});
			const mainHead = checkout.readGenerationMainHead(generation);
			const persist = (beforePush) => checkout.persistGeneration(generation, access.acceptedSourceSha, beforePush);
			const pushPrepared = (preparedHead) => checkout.pushPreparedGeneration(generation, access.acceptedSourceSha, preparedHead);
			return {
				verifySource,
				mainHead,
				restore,
				persist,
				pushPrepared
			};
		};
		return RepositorySourceCheckout.of({ forSource });
	}));
};
//#endregion
export { RepositorySourceCheckout, RepositorySourceRestoreHeadMismatchError };
