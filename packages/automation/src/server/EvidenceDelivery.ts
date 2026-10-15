import { Cause, Context, DateTime, Effect, Schema } from "effect";

import type { RepairHostError } from "./RepairHostError";

export const ProviderReadReason = Schema.Literals([
  "not-configured",
  "not-authorized",
  "not-retained",
  "rate-limited",
  "provider-error",
  "invalid-response",
  "response-too-large",
  "timeout",
]);

export interface EvidenceDeliveryReceipt {
  readonly id: string;
  readonly claimId: string;
  readonly createdAtMs: number;
  readonly failureCount: number;
  readonly failureReason: string | null;
}

/** Use the existing inbox's fenced write and content-free diagnostic sink. */
export class EvidenceDeliveryHost extends Context.Service<
  EvidenceDeliveryHost,
  {
    readonly retainFailure: (input: {
      readonly id: string;
      readonly claimId: string;
      readonly reason: typeof ProviderReadReason.Type;
      readonly backoffMs: number;
      readonly retry: boolean;
    }) => Effect.Effect<boolean, RepairHostError>;
    readonly logFailure: (cause: Cause.Cause<unknown>) => Effect.Effect<void>;
    readonly reportFailure: (cause: Cause.Cause<unknown>) => Effect.Effect<void>;
  }
>()("@reve/repairs/EvidenceDeliveryHost") {}

/** Provider classification is supplied by the adapter that decoded the response. */
export const retryEvidenceDelivery = Effect.fn("EvidenceDelivery.retry")(function* (
  receipt: EvidenceDeliveryReceipt,
  failure: {
    readonly reason: typeof ProviderReadReason.Type;
    readonly expected: boolean;
    readonly retryAfterMs: number;
    readonly cause: Cause.Cause<unknown>;
  },
) {
  const host = yield* EvidenceDeliveryHost;
  const exhausted =
    failure.expected &&
    failure.reason === "not-retained" &&
    receipt.failureCount >= 2 &&
    (yield* DateTime.nowAsDate).getTime() - receipt.createdAtMs >= 3_600_000;

  yield* host.logFailure(failure.cause);
  const retained = yield* host
    .retainFailure({
      id: receipt.id,
      claimId: receipt.claimId,
      reason: failure.reason,
      retry: !exhausted,
      backoffMs: Math.max(
        failure.retryAfterMs,
        Math.min(1_800_000, 30_000 * 2 ** Math.min(receipt.failureCount, 6)),
      ),
    })
    .pipe(Effect.tapCause(() => host.reportFailure(failure.cause)));

  // Suppress reminders only after the retry is durable; never hide lost ownership or a failed write.
  if (
    !retained ||
    !failure.expected ||
    exhausted ||
    (failure.reason !== "rate-limited" &&
      failure.reason !== "not-retained" &&
      (receipt.failureCount === 0 ||
        receipt.failureReason !== failure.reason ||
        receipt.failureCount % 16 === 0))
  )
    yield* host.reportFailure(failure.cause);
});
