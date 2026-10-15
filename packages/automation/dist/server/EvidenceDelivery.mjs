import { __exportAll } from "../_virtual/_rolldown/runtime.mjs";
import { Context, DateTime, Effect, Schema } from "effect";
//#region src/server/EvidenceDelivery.ts
var EvidenceDelivery_exports = /* @__PURE__ */ __exportAll({
	EvidenceDeliveryHost: () => EvidenceDeliveryHost,
	ProviderReadReason: () => ProviderReadReason,
	retryEvidenceDelivery: () => retryEvidenceDelivery
});
const ProviderReadReason = Schema.Literals([
	"not-configured",
	"not-authorized",
	"not-retained",
	"rate-limited",
	"provider-error",
	"invalid-response",
	"response-too-large",
	"timeout"
]);
/** Use the existing inbox's fenced write and content-free diagnostic sink. */
var EvidenceDeliveryHost = class extends Context.Service()("@reve/repairs/EvidenceDeliveryHost") {};
/** Provider classification is supplied by the adapter that decoded the response. */
const retryEvidenceDelivery = Effect.fn("EvidenceDelivery.retry")(function* (receipt, failure) {
	const host = yield* EvidenceDeliveryHost;
	const exhausted = failure.expected && failure.reason === "not-retained" && receipt.failureCount >= 2 && (yield* DateTime.nowAsDate).getTime() - receipt.createdAtMs >= 36e5;
	yield* host.logFailure(failure.cause);
	if (!(yield* host.retainFailure({
		id: receipt.id,
		claimId: receipt.claimId,
		reason: failure.reason,
		retry: !exhausted,
		backoffMs: Math.max(failure.retryAfterMs, Math.min(18e5, 3e4 * 2 ** Math.min(receipt.failureCount, 6)))
	}).pipe(Effect.tapCause(() => host.reportFailure(failure.cause)))) || !failure.expected || exhausted || failure.reason !== "rate-limited" && failure.reason !== "not-retained" && (receipt.failureCount === 0 || receipt.failureReason !== failure.reason || receipt.failureCount % 16 === 0)) yield* host.reportFailure(failure.cause);
});
//#endregion
export { EvidenceDeliveryHost, EvidenceDelivery_exports, ProviderReadReason, retryEvidenceDelivery };
