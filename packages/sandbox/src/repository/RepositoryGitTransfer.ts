import { Cause, Effect, Pull, Redacted, Schema, Stream } from "effect";
import {
  FetchHttpClient,
  HttpBody,
  HttpClient,
  HttpClientError,
  HttpClientRequest,
} from "effect/http";

import { GithubRepositoryBranch, RepositoryCommitSha } from "../model/RepositorySource";

import { RepositoryGitPack, type RepositoryGitPackPart } from "./RepositoryGitPack";
import { RepositoryGitSource } from "./RepositoryGitSource";
import { RepositoryMutationFence } from "./RepositoryMutationFence";

export class RepositoryGitTransferFailure extends Schema.TaggedError<RepositoryGitTransferFailure>()(
  "RepositoryGitTransferFailure",
  {
    reason: Schema.Literals([
      "protocol",
      "unavailable",
      "uncertain",
      "write-access",
      "branch-conflict",
      "rejected",
      "delta-depth",
      "pack-size",
      "repack",
    ]),
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

const invalid = () =>
  RepositoryGitTransferFailure.make({
    reason: "protocol",
    message: "Git source transfer did not complete safely",
  });

const uncertain = (cause: unknown) =>
  RepositoryGitTransferFailure.make({
    reason: "uncertain",
    message: "Git receiver outcome is uncertain; verify the exact destination ref",
    cause,
  });

type TransferPhase = "source-discovery" | "target-discovery" | "upload-pack" | "receive-pack";
const protocolFailure = (
  phase: TransferPhase,
  reason:
    | "http-status"
    | "content-type"
    | "body-limit"
    | "packet-header"
    | "packet-length"
    | "report",
  details: {
    readonly status?: number;
    readonly bytes?: number;
    readonly length?: number;
    readonly lines?: number;
  } = {},
) =>
  // The outer boundary reports the failure; retain only content-free transport attribution here.
  Effect.logWarning("Repository Git protocol rejected", { phase, reason, ...details }).pipe(
    Effect.andThen(
      reason === "http-status" && [408, 429, 500, 502, 503, 504].includes(details.status ?? 0)
        ? RepositoryGitTransferFailure.make({
            reason: "unavailable",
            message: "Git transport is temporarily unavailable",
          })
        : invalid(),
    ),
  );
const encoder = new TextEncoder();
const decoder = new TextDecoder();
const zeroId = "0".repeat(40);
// Git pack v2 with zero objects and its SHA-1 trailer, for graphs already at the receiver.
const emptyPack = new Uint8Array([
  80, 65, 67, 75, 0, 0, 0, 2, 0, 0, 0, 0, 2, 157, 8, 130, 59, 216, 168, 234, 181, 16, 173, 106, 199,
  92, 130, 60, 253, 62, 211, 30,
]);
const packetLength = Schema.String.check(Schema.isPattern(/^[0-9a-f]{4}$/u));
const acknowledgement = Schema.String.check(Schema.isPattern(/^(?:NAK|ACK [0-9a-f]{40})\n?$/u));
const packetChannel = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 3 }));
const refLine = Schema.Tuple([RepositoryCommitSha, Schema.NonEmptyString]);
const firstRef = Schema.Tuple([Schema.NonEmptyString, Schema.NonEmptyString]);
// oxlint-disable-next-line no-restricted-properties -- Git wire fields have untrusted tuple lengths and values.
const decodeFirstRef = Schema.decodeUnknownEffect(firstRef);
// oxlint-disable-next-line no-restricted-properties -- Git reference fields arrive as a variable-length wire array.
const decodeRefLine = Schema.decodeUnknownEffect(refLine);

const packet = (value: string) => {
  const bytes = encoder.encode(value);

  return `${(bytes.length + 4).toString(16).padStart(4, "0")}${value}`;
};

// Only control messages are buffered. Repository object data never enters this decoder.
const control = Effect.fn("RepositoryGitTransfer.control")(function* (
  request: HttpClientRequest.HttpClientRequest,
  contentType: string,
  phase: TransferPhase,
) {
  const client = HttpClient.withScope(yield* HttpClient.HttpClient);
  const response = yield* client.execute(request);

  if (response.status === 401 || response.status === 403)
    return yield* RepositoryGitTransferFailure.make({
      reason: "write-access",
      message:
        "Git transport authorization was denied. Check the selected publisher's GitHub access and repository write policy.",
    });

  if (response.status !== 200)
    return yield* protocolFailure(phase, "http-status", { status: response.status });
  if (response.headers["content-type"] !== contentType)
    return yield* protocolFailure(phase, "content-type");
  const chunks = yield* response.stream.pipe(
    Stream.runFoldEffect(
      () => ({ size: 0, chunks: [] as Array<Uint8Array> }),
      (state, chunk) => {
        const size = state.size + chunk.length;

        if (size > 8 * 1024 * 1024) return protocolFailure(phase, "body-limit", { bytes: size });
        state.chunks.push(chunk);

        return Effect.succeed({ size, chunks: state.chunks });
      },
    ),
  );
  const bytes = new Uint8Array(chunks.size);
  let offset = 0;

  for (const chunk of chunks.chunks) {
    bytes.set(chunk, offset);
    offset += chunk.length;
  }
  const lines: Array<string | null> = [];

  for (offset = 0; offset < bytes.length;) {
    const header = yield* Schema.decodeEffect(packetLength)(
      decoder.decode(bytes.subarray(offset, offset + 4)),
    ).pipe(Effect.catch(() => protocolFailure(phase, "packet-header")));
    const length = Number.parseInt(header, 16);

    if (length === 0) {
      lines.push(null);
      offset += 4;
      continue;
    }
    if (length < 4 || length > 65520 || offset + length > bytes.length)
      return yield* protocolFailure(phase, "packet-length", { length, bytes: bytes.length });
    lines.push(decoder.decode(bytes.subarray(offset + 4, offset + length)).replace(/\n$/u, ""));
    offset += length;
  }

  return lines;
});

const discover = Effect.fn("RepositoryGitTransfer.discover")(function* (
  url: string,
  authorization: Redacted.Redacted<string>,
  service: "git-upload-pack" | "git-receive-pack",
  commitSha: string,
) {
  const lines = yield* control(
    HttpClientRequest.get(`${url}/info/refs`, {
      urlParams: { service },
      headers: { authorization: Redacted.value(authorization), "cache-control": "no-cache" },
    }),
    `application/x-${service}-advertisement`,
    service === "git-upload-pack" ? "source-discovery" : "target-discovery",
  );

  if (lines.shift() !== `# service=${service}` || lines.shift() !== null || lines.pop() !== null)
    return yield* invalid();
  if (lines[0] === "version 1") lines.shift();
  const [first, advertised] = yield* decodeFirstRef(lines.shift()?.split("\0")).pipe(
    Effect.mapError(invalid),
  );
  const capabilities = new Set(advertised.split(" "));
  let wantedAdvertised = false;
  const refs = new Map<string, string>();

  for (const line of [first, ...lines]) {
    const [sha, ref] = yield* decodeRefLine(line?.split(" ")).pipe(Effect.mapError(invalid));

    if (sha === commitSha) wantedAdvertised = true;
    if (refs.has(ref)) return yield* invalid();
    refs.set(ref, sha);
  }
  if (
    service === "git-upload-pack" &&
    !wantedAdvertised &&
    !capabilities.has("allow-tip-sha1-in-want") &&
    !capabilities.has("allow-reachable-sha1-in-want") &&
    !capabilities.has("allow-any-sha1-in-want")
  )
    return yield* invalid();
  if (service === "git-receive-pack" && !capabilities.has("report-status")) return yield* invalid();

  return { capabilities, refs };
});

// Git sideband frames are bounded; never buffer the repository's complete pack.
const uploadPack = Effect.fn("RepositoryGitTransfer.uploadPack")(function* <E>(
  body: Stream.Stream<Uint8Array, E>,
  sideband: boolean,
  haves: ReadonlySet<string>,
) {
  const pull = yield* Stream.toPull(body);
  let chunks: ReadonlyArray<Uint8Array> = [];
  let index = 0;
  let offset = 0;
  const read = Effect.fnUntraced(function* (length: number) {
    const bytes = new Uint8Array(length);
    let written = 0;

    while (written < length) {
      const chunk = chunks[index];

      if (chunk === undefined) {
        chunks = yield* pull.pipe(Pull.catchDone(invalid));
        index = 0;
        offset = 0;
        continue;
      }
      const count = Math.min(length - written, chunk.length - offset);

      bytes.set(chunk.subarray(offset, offset + count), written);
      written += count;
      offset += count;
      if (offset === chunk.length) {
        index++;
        offset = 0;
      }
    }

    return bytes;
  });
  const remaining = Stream.suspend(() =>
    Stream.fromIterable(
      chunks
        .slice(index)
        .map((chunk, position) => (position === 0 ? chunk.subarray(offset) : chunk)),
    ).pipe(Stream.concat(Stream.fromPull(Effect.succeed(pull)))),
  );
  const length = Effect.gen(function* () {
    const header = yield* Schema.decodeEffect(packetLength)(decoder.decode(yield* read(4))).pipe(
      Effect.mapError(invalid),
    );

    return Number.parseInt(header, 16);
  });
  const unread = (prefix: Uint8Array) => {
    chunks = [
      prefix,
      ...chunks
        .slice(index)
        .map((chunk, position) => (position === 0 ? chunk.subarray(offset) : chunk)),
    ];
    index = 0;
    offset = 0;
  };
  let acknowledgements = 0;

  // A pipelined list of haves may produce more than one plain ACK before the pack.
  // Read only bounded control packets; retain the first pack/frame bytes for streaming.
  while (true) {
    const header = yield* read(4);

    if (!sideband && decoder.decode(header) === "PACK") {
      if (acknowledgements === 0) return yield* invalid();
      unread(header);
      break;
    }
    const size = Number.parseInt(
      yield* Schema.decodeEffect(packetLength)(decoder.decode(header)).pipe(
        Effect.mapError(invalid),
      ),
      16,
    );

    if (size < 5 || size > 65520) return yield* invalid();
    const first = yield* read(1);

    if (sideband && (first[0] === 1 || first[0] === 2 || first[0] === 3)) {
      if (acknowledgements === 0) return yield* invalid();
      const prefix = new Uint8Array(5);

      prefix.set(header);
      prefix.set(first, 4);
      unread(prefix);
      break;
    }
    if (size < 7 || size > 49 || acknowledgements >= haves.size + 1) return yield* invalid();
    const acknowledged = yield* Schema.decodeEffect(acknowledgement)(
      decoder.decode(first) + decoder.decode(yield* read(size - 5)),
    ).pipe(Effect.mapError(invalid));

    if (acknowledged.startsWith("ACK ") && !haves.has(acknowledged.slice(4).trim()))
      return yield* invalid();
    acknowledgements++;
  }
  yield* Effect.annotateCurrentSpan("repository.git.ack_count", acknowledgements);
  const frames: Stream.Stream<Uint8Array, E | RepositoryGitTransferFailure> = sideband
    ? Stream.fromPull(
        Effect.succeed(
          Effect.gen(function* () {
            while (true) {
              const size = yield* length;

              if (size === 0) {
                yield* Stream.runForEach(remaining, (chunk) =>
                  chunk.length === 0 ? Effect.void : invalid(),
                );

                return yield* Cause.done();
              }
              if (size < 5 || size > 65520) return yield* invalid();
              const frame = yield* read(size - 4);
              const firstByte = frame[0];
              if (firstByte === undefined) return yield* invalid();
              const channel = yield* Schema.decodeEffect(packetChannel)(firstByte).pipe(
                Effect.mapError(invalid),
              );

              if (channel === 3) return yield* invalid();
              if (channel === 1 && frame.length > 1) return [frame.subarray(1)] as const;
              // Progress is untrusted provider text; discard it without decoding or logging it.
            }
          }),
        ),
      )
    : remaining;
  const prefix = encoder.encode("PACK");
  let matched = 0;

  return frames.pipe(
    Stream.mapEffect((chunk) => {
      const count = Math.min(prefix.length - matched, chunk.length);

      for (let position = 0; position < count; position++) {
        if (chunk[position] !== prefix[matched + position]) return invalid();
      }
      const previous = matched;

      matched += count;
      if (matched < prefix.length) return Effect.succeed(new Uint8Array());
      if (previous === prefix.length) return Effect.succeed(chunk);
      const first = new Uint8Array(prefix.length + chunk.length - count);

      first.set(prefix);
      first.set(chunk.subarray(count), prefix.length);

      return Effect.succeed(first);
    }),
    Stream.filter((chunk) => chunk.length > 0),
    Stream.concat(
      Stream.fromEffect(
        Effect.suspend(() => (matched === prefix.length ? Effect.void : invalid())),
      ).pipe(Stream.drain),
    ),
  );
});

export interface RepositoryGitPublication {
  readonly target: { readonly url: string; readonly authorization: Redacted.Redacted<string> };
  readonly commitSha: string;
  readonly branch: string;
  readonly requireEmptyTarget: boolean;
  readonly expectedHead?: string;
  readonly preferredHave?: string;
}

/** One receiver protocol for HTTP and local Git sources. The caller owns identity and authority. */
export const publishRepositoryGit = Effect.fnUntraced(
  function* (input: RepositoryGitPublication) {
    const commitSha = yield* Schema.decodeEffect(RepositoryCommitSha)(input.commitSha).pipe(
      Effect.mapError(invalid),
    );
    const branch = yield* Schema.decodeEffect(GithubRepositoryBranch)(input.branch).pipe(
      Effect.mapError(invalid),
    );
    const expectedHead =
      input.expectedHead === undefined
        ? undefined
        : yield* Schema.decodeEffect(RepositoryCommitSha)(input.expectedHead).pipe(
            Effect.mapError(invalid),
          );
    const preferredHave =
      input.preferredHave === undefined
        ? undefined
        : yield* Schema.decodeEffect(RepositoryCommitSha)(input.preferredHave).pipe(
            Effect.mapError(invalid),
          );

    if (input.requireEmptyTarget && expectedHead !== undefined) return yield* invalid();
    const targetRef = `refs/heads/${branch}`;
    const target = yield* discover(
      input.target.url,
      input.target.authorization,
      "git-receive-pack",
      commitSha,
    );
    const existing = target.refs.get(targetRef);

    if (existing === commitSha) return;
    if (existing !== expectedHead)
      return yield* RepositoryGitTransferFailure.make({
        reason: "branch-conflict",
        message: "The publication branch moved unexpectedly; it was not overwritten.",
      });
    // Stable across retries, isolated from other refs/commits in a shared source mirror.
    // oxlint-disable-next-line no-restricted-properties -- Fingerprint of the private transfer-ref identity.
    const stageIdentity = JSON.stringify([targetRef, commitSha]);
    const stageKey = new Uint8Array(
      yield* Effect.tryPromise({
        try: () => crypto.subtle.digest("SHA-256", encoder.encode(stageIdentity)),
        catch: (cause) =>
          RepositoryGitTransferFailure.make({
            reason: "protocol",
            message: "The private Git transfer identity could not be prepared",
            cause,
          }),
      }),
    );
    const stageRef = `refs/kommunikasie/transfers/${Array.from(stageKey, (byte) =>
      byte.toString(16).padStart(2, "0"),
    ).join("")}`;
    let stageHead = target.refs.get(stageRef);

    // An interrupted bounded transfer may retain its private ref. Its exact head
    // is verified against pack preparation before granting it any write authority.
    if (
      input.requireEmptyTarget &&
      [...target.refs].some(([ref, sha]) => sha !== zeroId && ref !== stageRef)
    )
      return yield* invalid();
    const targetCapabilities = target.capabilities;
    // Negotiate only commits the destination advertises. Unknown source commits are
    // ignored by upload-pack; no common history still produces a complete pack.
    const haves = new Set<string>();
    const preferredAdvertised =
      preferredHave !== undefined &&
      [...target.refs].some(
        ([ref, sha]) => sha === preferredHave && (ref === "HEAD" || ref.startsWith("refs/heads/")),
      );

    if (preferredAdvertised) haves.add(preferredHave);

    for (const [ref, sha] of target.refs) {
      if (sha === zeroId || (ref !== "HEAD" && !ref.startsWith("refs/heads/"))) continue;
      haves.add(sha);
      if (haves.size === 256) break;
    }
    const reuseObjects = stageHead === undefined && [...target.refs.values()].includes(commitSha);

    yield* Effect.annotateCurrentSpan({
      "repository.git.have_count": haves.size,
      "repository.git.preferred_have_selected": preferredAdvertised,
      "repository.git.zero_object_reuse": reuseObjects,
      "repository.git.ref_update": expectedHead === undefined ? "create" : "compare-and-set",
    });
    // A retained stage must pass pack-preparation identity checks before it can be removed.
    const prepared = reuseObjects
      ? Stream.succeed<RepositoryGitPackPart<never>>({
          _tag: "Direct",
          body: Stream.make(emptyPack.slice()),
        })
      : Stream.unwrap(
          Effect.gen(function* () {
            return (yield* RepositoryGitPack).prepare(
              (yield* RepositoryGitSource).pack({
                commitSha,
                haves,
                ofsDelta: targetCapabilities.has("ofs-delta"),
              }),
            );
          }),
        );
    let totalPackBytes = 0;
    let maximumPackBytes = 0;
    let requests = 0;
    const receive = Effect.fn("RepositoryGitTransfer.receivePack")(function* <E>(
      body: Stream.Stream<Uint8Array, E>,
      updates: ReadonlyArray<{ readonly ref: string; readonly old: string; readonly head: string }>,
      atomic: boolean,
      phase: "direct" | "stage" | "publish",
    ) {
      const packPull = yield* Stream.toPull(body);
      const first = yield* packPull.pipe(Pull.catchDone(invalid));
      let sourceFailure: Cause.Cause<E> | undefined;
      let bytes = 0;
      const pack = Stream.fromIterable(first).pipe(
        Stream.concat(Stream.fromPull(Effect.succeed(packPull))),
        Stream.tap((chunk) =>
          Effect.sync(() => {
            bytes += chunk.byteLength;
            totalPackBytes += chunk.byteLength;
            maximumPackBytes = Math.max(maximumPackBytes, bytes);
          }),
        ),
        Stream.tapCause((cause) =>
          Effect.sync(() => {
            sourceFailure = cause;
          }),
        ),
      );
      const commands = updates
        .map(({ ref, old, head }, index) =>
          packet(
            `${old} ${head} ${ref}${index === 0 ? `\0report-status${atomic ? " atomic" : ""}` : ""}\n`,
          ),
        )
        .join("");

      yield* (yield* RepositoryMutationFence).check;
      yield* Effect.annotateCurrentSpan({
        "repository.git.receive.phase": phase,
        "repository.git.receive.index": requests++,
        "repository.git.receive.atomic": atomic,
      });
      const report = yield* control(
        HttpClientRequest.post(`${input.target.url}/git-receive-pack`, {
          headers: { authorization: Redacted.value(input.target.authorization) },
          body: HttpBody.stream(
            Stream.make(encoder.encode(`${commands}0000`)).pipe(Stream.concat(pack)),
            "application/x-git-receive-pack-request",
          ),
        }),
        "application/x-git-receive-pack-result",
        "receive-pack",
      ).pipe(
        Effect.ensuring(
          Effect.suspend(() => Effect.annotateCurrentSpan("repository.git.pack_bytes", bytes)),
        ),
        Effect.catchCause((cause) => {
          if (sourceFailure !== undefined)
            return Effect.failCause(Cause.combine(cause, sourceFailure));

          // Only receiver response failures are uncertain. Preserve source failures,
          // authorization denials, interruptions, and mixed defects without recovery.
          return cause.reasons.length > 0 &&
            cause.reasons.every(
              (reason) =>
                Cause.isFailReason(reason) &&
                (HttpClientError.isHttpClientError(reason.error) ||
                  (Schema.is(RepositoryGitTransferFailure)(reason.error) &&
                    ["protocol", "unavailable"].includes(reason.error.reason))),
            )
            ? uncertain(cause)
            : Effect.failCause(cause);
        }),
      );

      if (sourceFailure !== undefined) return yield* Effect.failCause(sourceFailure);
      if (report.includes("unpack receive-pack request exceeds maximum size"))
        return yield* RepositoryGitTransferFailure.make({
          reason: "pack-size",
          message: "The Git pack exceeds the receiver's request limit",
        });
      if (
        report.some(
          (line) =>
            line === "unpack error delta chain depth exceeds configured limit" ||
            line === "unpack error stored delta chain exceeds the configured limit",
        )
      )
        return yield* RepositoryGitTransferFailure.make({
          reason: "delta-depth",
          message: "The repository receiver rejected the Git pack's delta depth",
        });
      if (report.some((line) => updates.some(({ ref }) => line?.startsWith(`ng ${ref} `))))
        return yield* RepositoryGitTransferFailure.make({
          reason: "rejected",
          message:
            "The Git receiver rejected the retained ref update; no force update was attempted",
        });
      // GitHub can append a second terminal flush to a successful report-status response.
      const resultCount = updates.length + 1;

      if (
        (report.length !== resultCount + 1 && report.length !== resultCount + 2) ||
        report[0] !== "unpack ok" ||
        updates.some(({ ref }, index) => report[index + 1] !== `ok ${ref}`) ||
        report.slice(resultCount).some((line) => line !== null)
      )
        return yield* protocolFailure("receive-pack", "report", { lines: report.length }).pipe(
          Effect.mapError(uncertain),
        );
    }, Effect.scoped);
    let mode: "unstarted" | "direct" | "staged" = "unstarted";
    let preparedStageHead: string | undefined;
    const publish = { ref: targetRef, old: expectedHead ?? zeroId, head: commitSha };

    yield* Effect.gen(function* () {
      yield* Stream.runForEach(prepared, (part) =>
        Effect.gen(function* () {
          if (mode === "direct") return yield* invalid();
          if (mode === "staged" && part._tag !== "Staged") return yield* invalid();
          if (part.stagingHead !== undefined && !Schema.is(RepositoryCommitSha)(part.stagingHead))
            return yield* invalid();
          if (mode === "unstarted") {
            preparedStageHead = part.stagingHead;

            if (stageHead !== undefined && stageHead !== preparedStageHead)
              return yield* RepositoryGitTransferFailure.make({
                reason: "branch-conflict",
                message: "The private Git transfer ref does not match its retained preparation",
              });
          } else if (preparedStageHead !== part.stagingHead) return yield* invalid();
          if (
            (part._tag === "Staged" || stageHead !== undefined) &&
            (!targetCapabilities.has("atomic") || !targetCapabilities.has("delete-refs"))
          )
            return yield* RepositoryGitTransferFailure.make({
              reason: "protocol",
              message: "The Git receiver cannot atomically publish a bounded transfer",
            });

          if (part._tag === "Direct") {
            mode = "direct";
            yield* receive(
              part.body,
              stageHead === undefined
                ? [publish]
                : [publish, { ref: stageRef, old: stageHead, head: zeroId }],
              stageHead !== undefined,
              "direct",
            );
          } else {
            mode = "staged";
            yield* receive(
              part.body,
              [{ ref: stageRef, old: stageHead ?? zeroId, head: part.stagingHead }],
              false,
              "stage",
            );
            stageHead = part.stagingHead;
          }
        }),
      );

      if (mode === "unstarted") return yield* invalid();
      if (mode === "staged") {
        if (stageHead === undefined || stageHead !== preparedStageHead) return yield* invalid();
        // All objects arrived; this is the only mutation of the requested branch.
        // An uncertain final response reconciles that branch and no staging ref remains.
        yield* receive(
          Stream.make(emptyPack.slice()),
          [publish, { ref: stageRef, old: stageHead, head: zeroId }],
          true,
          "publish",
        );
      }
      yield* Effect.logInfo("Repository Git pack transfer completed");
    }).pipe(
      Effect.ensuring(
        Effect.suspend(() =>
          Effect.annotateCurrentSpan({
            "repository.git.pack_bytes": totalPackBytes,
            "repository.git.pack.maximum_bytes": maximumPackBytes,
            "repository.git.receive.count": requests,
            "repository.git.transfer.mode": reuseObjects ? "ref-only" : mode,
          }),
        ),
      ),
    );
  },
  Effect.scoped,
  Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual" }),
);

/** Authorize/discover the HTTP source before using the shared bounded publisher. */
export const transferRepositoryGit = Effect.fn("RepositoryGitTransfer.transfer")(
  function* (
    input: RepositoryGitPublication & {
      readonly source: { readonly url: string; readonly authorization: Redacted.Redacted<string> };
    },
  ) {
    const client = HttpClient.withScope(yield* HttpClient.HttpClient);
    const commitSha = yield* Schema.decodeEffect(RepositoryCommitSha)(input.commitSha).pipe(
      Effect.mapError(invalid),
    );
    const source = yield* discover(
      input.source.url,
      input.source.authorization,
      "git-upload-pack",
      commitSha,
    );
    const sourceCapabilities = source.capabilities;
    const sideband = sourceCapabilities.has("side-band-64k");

    return yield* publishRepositoryGit(input).pipe(
      Effect.provideService(RepositoryGitSource, {
        pack: ({ commitSha, haves, ofsDelta }) =>
          Stream.unwrap(
            Effect.gen(function* () {
              const capabilities = [
                ...(sideband ? ["side-band-64k"] : []),
                ...(sourceCapabilities.has("no-progress") ? ["no-progress"] : []),
                ...(sourceCapabilities.has("ofs-delta") && ofsDelta ? ["ofs-delta"] : []),
              ];
              const fetched = yield* client.post(`${input.source.url}/git-upload-pack`, {
                headers: { authorization: Redacted.value(input.source.authorization) },
                // No shallow, filter or thin-pack: retain the complete missing graph.
                body: HttpBody.text(
                  `${packet(`want ${commitSha} ${capabilities.join(" ")}\n`)}0000${[...haves]
                    .map((sha) => packet(`have ${sha}\n`))
                    .join("")}${packet("done\n")}`,
                  "application/x-git-upload-pack-request",
                ),
              });

              if (fetched.status !== 200)
                return yield* protocolFailure("upload-pack", "http-status", {
                  status: fetched.status,
                });
              if (fetched.headers["content-type"] !== "application/x-git-upload-pack-result")
                return yield* protocolFailure("upload-pack", "content-type");

              return yield* uploadPack(fetched.stream, sideband, haves);
            }),
          ),
      }),
    );
  },
  Effect.scoped,
  Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual" }),
);
