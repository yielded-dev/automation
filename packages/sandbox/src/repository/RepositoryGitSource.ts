import { Context, type Stream } from "effect";
import type { HttpClientError } from "effect/http";

import type { RepositoryGitTransferFailure } from "./RepositoryGitTransfer";

/** Produce the complete missing graph as a non-thin pack from the caller's pinned source. */
export class RepositoryGitSource extends Context.Service<
  RepositoryGitSource,
  {
    readonly pack: (input: {
      readonly commitSha: string;
      readonly haves: ReadonlySet<string>;
      readonly ofsDelta: boolean;
    }) => Stream.Stream<Uint8Array, RepositoryGitTransferFailure | HttpClientError.HttpClientError>;
  }
>()("kommunikasie/RepositoryGitSource") {}
