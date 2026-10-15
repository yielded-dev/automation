import { __exportAll } from "../_virtual/_rolldown/runtime.mjs";
import { RepairEvidenceReference } from "../domain/Investigation.mjs";
import { Schema } from "effect";
//#region src/server/DecisionLifecycle.ts
var DecisionLifecycle_exports = /* @__PURE__ */ __exportAll({
	reuseAcceptedDecision: () => reuseAcceptedDecision,
	selectOccurrence: () => selectOccurrence,
	selectRetainedEvidence: () => selectRetainedEvidence,
	selectSettlementOccurrence: () => selectSettlementOccurrence
});
/** The host has already accepted this occurrence's ordering and retirement cutoff. */
const selectOccurrence = (current, occurrence, protection) => {
	if (current.activeAttemptId !== null || protection.pendingDecision || protection.openPullRequest || current.decisionId !== null && !current.retired && occurrence.release === current.source.release && occurrence.sourceRevision === current.source.sourceRevision) return null;
	return {
		selectedEventId: occurrence.sourceEventId,
		release: occurrence.release,
		sourceRevision: occurrence.sourceRevision,
		evidenceId: null,
		decisionId: null
	};
};
/** priorAutomatic is the latest non-observation record for this incident/evidence/settings. */
const reuseAcceptedDecision = (request, existing, priorAutomatic) => {
	if (existing) return existing.incidentId === request.incidentId && existing.settingsRevision === request.settingsRevision ? {
		status: "reconcile",
		value: existing
	} : {
		status: "blocked",
		reason: "conflicting_identity"
	};
	if (!request.requestId && priorAutomatic) return {
		status: "reconcile",
		value: priorAutomatic
	};
	return { status: "new" };
};
const selectRetainedEvidence = (current, evidence, existing, retirement) => {
	if (retirement !== void 0 && (retirement.occurredAtMs === void 0 || retirement.occurredAtMs <= retirement.cutoffMs)) return { status: "reject" };
	if (existing && !Schema.toEquivalence(RepairEvidenceReference)(existing, evidence)) return { status: "reject" };
	return current.source.selectedEventId === evidence.sourceEventId && current.source.release === evidence.release && current.source.sourceRevision === evidence.sourceRevision && !current.activeAttemptId && current.decisionId === null ? {
		status: "select",
		evidenceId: evidence.manifestId
	} : { status: "retain" };
};
/** Called after settlement releases the handoff, with the newest eligible retained occurrence. */
const selectSettlementOccurrence = (current, latest) => {
	if (!latest || latest.selectedEventId === current.selectedEventId || latest.release === current.release && latest.sourceRevision === current.sourceRevision) return null;
	return {
		selectedEventId: latest.selectedEventId,
		release: latest.release,
		sourceRevision: latest.sourceRevision,
		evidenceId: latest.evidenceId,
		decisionId: null
	};
};
//#endregion
export { DecisionLifecycle_exports, reuseAcceptedDecision, selectOccurrence, selectRetainedEvidence, selectSettlementOccurrence };
