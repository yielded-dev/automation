import { Context, Data, Effect, Layer, type Redacted } from "effect";

import type { SessionSandbox } from "../SessionSandbox";
import type { SessionStateStore } from "./SessionStateStore";

export interface RepositoryInstallSource {
  readonly sourceKey: string;
  readonly sourceHead: string;
  readonly execution?: {
    readonly sessionId: string;
    readonly turnId: string;
    readonly instanceId: string;
    /** Original repository identity from the host's accepted launch, including continuations. */
    readonly repositoryId?: string;
  };
}

export class RepositoryInstallAuthorizationError extends Data.TaggedError(
  "RepositoryInstallAuthorizationError",
)<{ readonly cause: unknown }> {}

/** Hosts authorize dependencies for their accepted source; recipe contents grant no authority. */
export class RepositoryInstallAuthorization extends Context.Service<
  RepositoryInstallAuthorization,
  {
    readonly authorize: (
      input: RepositoryInstallSource & { readonly deadlineAt: number },
    ) => Effect.Effect<
      ReadonlyArray<{ readonly remote: string; readonly authorization: Redacted.Redacted<string> }>,
      RepositoryInstallAuthorizationError,
      SessionSandbox | SessionStateStore
    >;
  }
>()("@reve/repairs/RepositoryInstallAuthorization") {
  static readonly emptyLayer = Layer.succeed(this, { authorize: () => Effect.succeed([]) });
}
