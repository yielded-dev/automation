import { Schema } from "effect";
import { v5 as uuidV5 } from "uuid";

import { RepairModelSettings } from "./proof";
import { RepairRequestKind } from "./RepairRequestKind";

const Identifier = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256));

export const RepairSourceRevision = Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/));

/** JSON tuple encoding prevents ambiguous identities when source keys contain separators. */
export const repairIdentity = (...parts: ReadonlyArray<string | number>): string =>
  // oxlint-disable-next-line no-restricted-properties -- canonical tuple fingerprint for durable UUID identities.
  uuidV5(JSON.stringify(["kommunikasie-repair", ...parts]), uuidV5.URL);

/** Host-selected immutable launch authority, persisted before external submission. */
export const RepairLaunchIntent = Schema.Struct({
  /** Frozen routing context, when the originating investigation classified the request. */
  requestKind: Schema.optionalKey(RepairRequestKind),
  rebase: Schema.optionalKey(
    Schema.Struct({
      pullRequestNumber: Schema.Int.check(Schema.isGreaterThan(0)),
      branch: Schema.NonEmptyString,
      headSha: RepairSourceRevision,
      /** Explicit retries keep their receipt even if the base branch moves. */
      requestId: Schema.optionalKey(Schema.String.check(Schema.isUUID())),
    }),
  ),
  continuation: Schema.optionalKey(
    Schema.Struct({
      runId: Schema.String.check(Schema.isUUID()),
      candidateSha: RepairSourceRevision,
      publication: Schema.NullOr(
        Schema.Struct({
          attemptId: Schema.String.check(Schema.isUUID()),
          candidateSha: RepairSourceRevision,
          pullRequestNumber: Schema.Natural,
        }),
      ),
    }),
  ),
  seedRepository: Identifier,
  githubRepositoryId: Schema.Natural,
  targetSha: RepairSourceRevision,
  targetRef: Identifier,
  title: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(160)),
  prompt: Schema.NonEmptyString,
  modelSettings: RepairModelSettings,
  evidenceHash: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  allowedScopes: Schema.Array(
    Schema.Literals([
      "repository_read",
      "repository_write",
      "evidence_read",
      "validation",
      "host_publication",
    ]),
  ),
  promptPolicy: Identifier,
  /** Retained only to preserve the identity of launches accepted before CI owned validation. */
  proofPolicy: Schema.optionalKey(Schema.Json),
});
export type RepairLaunchIntent = typeof RepairLaunchIntent.Type;

export const repairRunIdentities = (attemptId: string) => ({
  commandId: repairIdentity("dispatch", attemptId),
  runId: repairIdentity("run", attemptId),
});
