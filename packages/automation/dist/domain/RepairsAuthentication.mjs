import { ServiceUnavailableError } from "./errors.mjs";
import { HttpApiError, HttpApiMiddleware, HttpApiSchema } from "effect/http-api";
//#region src/domain/RepairsAuthentication.ts
/** Hosts authenticate and authorize each request using their existing identity system. */
var RepairsAuthentication = class extends HttpApiMiddleware.Service()("@reve/repairs/RepairsAuthentication", {
	requiredForClient: true,
	error: [
		HttpApiError.Unauthorized,
		HttpApiError.Forbidden,
		ServiceUnavailableError.pipe(HttpApiSchema.status(503))
	]
}) {};
//#endregion
export { RepairsAuthentication };
