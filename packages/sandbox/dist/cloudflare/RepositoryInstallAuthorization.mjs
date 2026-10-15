import { Context, Data, Effect, Layer } from "effect";
//#region src/cloudflare/RepositoryInstallAuthorization.ts
var RepositoryInstallAuthorizationError = class extends Data.TaggedError("RepositoryInstallAuthorizationError") {};
/** Hosts authorize dependencies for their accepted source; recipe contents grant no authority. */
var RepositoryInstallAuthorization = class extends Context.Service()("@reve/repairs/RepositoryInstallAuthorization") {
	static emptyLayer = Layer.succeed(this, { authorize: () => Effect.succeed([]) });
};
//#endregion
export { RepositoryInstallAuthorization, RepositoryInstallAuthorizationError };
