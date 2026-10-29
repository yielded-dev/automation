import * as SecureRandom from "effect/crypto/SecureRandom";
import { Cause, Effect, Exit, Layer, Schema, Stream } from "effect";

import { SandboxOperationError } from "../cloudflare/NativeSandbox";
import { SessionSandbox } from "../SessionSandbox";
import { RepositoryGitPack, type RepositoryGitPackPart } from "./RepositoryGitPack";
import {
  RepositoryGitPackManifest,
  repositoryGitPackFiles,
  repositoryGitPackScript,
} from "./RepositoryGitPackEncoding";
import { RepositoryGitTransferFailure } from "./RepositoryGitTransfer";

const cleanupCause = (cause: Cause.Cause<unknown>): Cause.Cause<never> =>
  Cause.fromReasons(
    cause.reasons.map((reason) =>
      Cause.isFailReason(reason) ? Cause.makeDieReason(reason.error) : reason,
    ),
  );

const preparationCause = (cause: Cause.Cause<unknown>) =>
  Cause.map(cause, (error) =>
    RepositoryGitTransferFailure.make({
      // The transfer owner can reacquire a failed sandbox channel and refetch
      // the pinned pack. Git or manifest failures remain terminal.
      reason: error instanceof SandboxOperationError ? "unavailable" : "repack",
      message: "Repository Git pack preparation did not complete",
      cause: error,
    }),
  );

/** Flatten incoming objects so Artifacts forks retain them. Credentials stay in the host. */
export const RepositoryGitPackSandboxLive = Layer.effect(
  RepositoryGitPack,
  Effect.gen(function* () {
    const sandbox = yield* SessionSandbox;
    const random = yield* SecureRandom.SecureRandom;
    const execute = Effect.fn("RepositoryGitPackSandbox.command")(function* (
      command: readonly [string, ...Array<string>],
      cwd?: string,
    ) {
      const process = yield* sandbox.exec([...command], {
        ...(cwd === undefined ? {} : { cwd }),
        timeout: 120_000,
      });
      const result = yield* process
        .outputText({ timeout: 120_000, maxBytes: 0 })
        .pipe(
          Effect.onExit((exit) =>
            Exit.isFailure(exit)
              ? process
                  .kill()
                  .pipe(Effect.catchCause((cause) => Effect.failCause(cleanupCause(cause))))
              : Effect.void,
          ),
        );

      if (result.exitCode !== 0)
        return yield* RepositoryGitTransferFailure.make({
          reason: "repack",
          message: "Repository Git pack preparation did not complete",
        });
    });

    return {
      prepare: <E>(
        pack: Stream.Stream<Uint8Array, E>,
      ): Stream.Stream<RepositoryGitPackPart<E>, E | RepositoryGitTransferFailure> =>
        Stream.unwrap(
          Effect.gen(function* () {
            const directory = `/tmp/repository-git-pack-${yield* random.uuidV4()}`;

            yield* sandbox.ensure().pipe(Effect.withSpan("RepositoryGitPackSandbox.start"));
            yield* Effect.acquireRelease(sandbox.mkdir(directory), () =>
              execute(["rm", "-rf", "--", directory]).pipe(
                Effect.catchCause((cause) => Effect.failCause(cleanupCause(cause))),
              ),
            );
            let sourceFailure: Cause.Cause<E> | undefined;
            let inputBytes = 0;
            const input = yield* Stream.toReadableStreamEffect(
              pack.pipe(
                Stream.tap((chunk) =>
                  Effect.sync(() => {
                    inputBytes += chunk.byteLength;
                  }),
                ),
                Stream.tapCause((cause) =>
                  Effect.sync(() => {
                    sourceFailure = cause;
                  }),
                ),
              ),
            );

            yield* sandbox.writeFile(`${directory}/input.pack`, input).pipe(
              Effect.catchCause((cause) =>
                Effect.failCause(
                  sourceFailure === undefined ? cause : Cause.combine(cause, sourceFailure),
                ),
              ),
              Effect.ensuring(
                Effect.suspend(() =>
                  Effect.annotateCurrentSpan("repository.git.pack.input_bytes", inputBytes),
                ),
              ),
              Effect.withSpan("RepositoryGitPackSandbox.writeInput"),
            );
            // Enumerate only the received pack before creating the synthetic root.
            // Git partitions and encodes full objects without walking commit history.
            yield* execute(
              ["bash", "-euo", "pipefail", "-c", repositoryGitPackScript],
              directory,
            ).pipe(Effect.withSpan("RepositoryGitPackSandbox.repack"));
            // Git can exceed --max-pack-size for a single large object. Validate
            // every file before emitting any part that could mutate the receiver.
            const manifest = yield* Schema.decodeEffect(RepositoryGitPackManifest)(
              yield* sandbox.readFileString(`${directory}/manifest.json`),
            );
            const files = repositoryGitPackFiles(manifest);
            let outputBytes = 0;
            const readPart = (name: string, bytes: number, index: number) =>
              Stream.unwrap(
                Effect.gen(function* () {
                  const output = yield* sandbox.readFile(`${directory}/output/${name}`);

                  if (output.body === null)
                    return yield* RepositoryGitTransferFailure.make({
                      reason: "repack",
                      message: "Repository Git pack output is unavailable",
                    });
                  const body = output.body;
                  let partBytes = 0;

                  return Stream.fromReadableStream({
                    evaluate: () => body,
                    onError: (cause) => cause,
                  }).pipe(
                    Stream.tap((chunk) =>
                      Effect.sync(() => {
                        partBytes += chunk.byteLength;
                        outputBytes += chunk.byteLength;
                      }),
                    ),
                    Stream.ensuring(
                      Effect.suspend(() =>
                        Effect.annotateCurrentSpan("repository.git.pack.output_bytes", partBytes),
                      ),
                    ),
                  );
                }),
              ).pipe(
                Stream.catchCause((cause) => Stream.failCause(preparationCause(cause))),
                Stream.withSpan("RepositoryGitPackSandbox.readOutput", {
                  attributes: {
                    "repository.git.pack.part_index": index,
                    "repository.git.pack.part_bytes": bytes,
                  },
                }),
              );
            const parts: Array<RepositoryGitPackPart<E>> = files.map((file, index) => ({
              _tag: file.mode,
              body: readPart(file.name, file.bytes, index),
              stagingHead: file.stagingHead,
            }));

            yield* Effect.annotateCurrentSpan({
              "repository.git.pack.part_count": files.length,
              "repository.git.pack.data_part_count": manifest.parts.length,
              "repository.git.pack.max_part_bytes": Math.max(...files.map((file) => file.bytes)),
            });

            return Stream.fromIterable(parts).pipe(
              Stream.ensuring(
                Effect.suspend(() =>
                  Effect.annotateCurrentSpan("repository.git.pack.output_bytes", outputBytes),
                ),
              ),
            );
          }),
        ).pipe(
          Stream.catchCause((cause) => Stream.failCause(preparationCause(cause))),
          Stream.withSpan("RepositoryGitPackSandbox.prepare", {
            attributes: { "repository.git.pack.encoding": "full" },
          }),
        ),
    };
  }),
);
