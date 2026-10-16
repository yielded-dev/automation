import { HttpApiError, HttpApiMiddleware, HttpApiSchema } from "effect/http-api";
import { ServiceUnavailableError } from "./errors";

/** Hosts authenticate and authorize each request using their existing identity system. */
export class RepairsAuthentication extends HttpApiMiddleware.Service<RepairsAuthentication>()(
  "@reve/repairs/RepairsAuthentication",
  {
    requiredForClient: true,
    error: [
      HttpApiError.Unauthorized,
      HttpApiError.Forbidden,
      ServiceUnavailableError.pipe(HttpApiSchema.status(503)),
    ],
  },
) {}
