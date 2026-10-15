import { Context, Stream } from "effect";

import type { RepositoryGitTransferFailure } from "./RepositoryGitTransfer";

export type RepositoryGitPackPart<E> =
  | {
      readonly _tag: "Direct";
      readonly body: Stream.Stream<Uint8Array, E | RepositoryGitTransferFailure>;
      readonly stagingHead?: string;
    }
  | {
      readonly _tag: "Staged";
      readonly body: Stream.Stream<Uint8Array, E | RepositoryGitTransferFailure>;
      readonly stagingHead: string;
    };

/** Destination-specific pack preparation; ordinary Git receivers accept the source encoding. */
export const RepositoryGitPack = Context.Reference<{
  /** Consume each body before advancing the stream that owns its preparation scope. */
  readonly prepare: <E>(
    pack: Stream.Stream<Uint8Array, E>,
  ) => Stream.Stream<RepositoryGitPackPart<E>, E | RepositoryGitTransferFailure>;
}>("kommunikasie/RepositoryGitPack", {
  defaultValue: () => ({
    prepare: <E>(pack: Stream.Stream<Uint8Array, E>) =>
      Stream.succeed<RepositoryGitPackPart<E>>({ _tag: "Direct", body: pack }),
  }),
});
