import { Schema } from "effect";

import { RepairEvidenceReference } from "../domain/Investigation";

/** Apply these transitions to validated records inside the host's acceptance transaction. */
interface SourceRevision {
  readonly release: string;
  readonly sourceRevision: string;
}

interface SelectedSource extends SourceRevision {
  readonly selectedEventId: string;
}

interface DecisionCheckpoint {
  readonly source: SelectedSource;
  readonly decisionId: string | null;
  readonly activeAttemptId: string | null;
}

interface Selection extends SelectedSource {
  readonly evidenceId: string | null;
  readonly decisionId: null;
}

/** The host has already accepted this occurrence's ordering and retirement cutoff. */
export const selectOccurrence = (
  current: DecisionCheckpoint & { readonly retired: boolean },
  occurrence: SourceRevision & { readonly sourceEventId: string },
  protection: { readonly pendingDecision: boolean; readonly openPullRequest: boolean },
): Selection | null => {
  if (
    current.activeAttemptId !== null ||
    protection.pendingDecision ||
    protection.openPullRequest ||
    (current.decisionId !== null &&
      !current.retired &&
      occurrence.release === current.source.release &&
      occurrence.sourceRevision === current.source.sourceRevision)
  )
    return null;

  return {
    selectedEventId: occurrence.sourceEventId,
    release: occurrence.release,
    sourceRevision: occurrence.sourceRevision,
    evidenceId: null,
    decisionId: null,
  };
};

interface AcceptedDecision {
  readonly incidentId: string;
  readonly settingsRevision: number;
}

/** priorAutomatic is the latest non-observation record for this incident/evidence/settings. */
export const reuseAcceptedDecision = <A extends AcceptedDecision>(
  request: AcceptedDecision & { readonly requestId?: string },
  existing: A | undefined,
  priorAutomatic: A | undefined,
):
  | { readonly status: "new" }
  | { readonly status: "reconcile"; readonly value: A }
  | { readonly status: "blocked"; readonly reason: "conflicting_identity" } => {
  if (existing)
    return existing.incidentId === request.incidentId &&
      existing.settingsRevision === request.settingsRevision
      ? { status: "reconcile", value: existing }
      : { status: "blocked", reason: "conflicting_identity" };
  // Preserve the accepted record, including terminal failures, request ID and deadlines.
  if (!request.requestId && priorAutomatic) return { status: "reconcile", value: priorAutomatic };
  return { status: "new" };
};

export const selectRetainedEvidence = (
  current: DecisionCheckpoint,
  evidence: RepairEvidenceReference,
  existing: RepairEvidenceReference | undefined,
  retirement: { readonly cutoffMs: number; readonly occurredAtMs: number | undefined } | undefined,
):
  | { readonly status: "reject" }
  | { readonly status: "retain" }
  | { readonly status: "select"; readonly evidenceId: string } => {
  if (
    retirement !== undefined &&
    (retirement.occurredAtMs === undefined || retirement.occurredAtMs <= retirement.cutoffMs)
  )
    return { status: "reject" };
  if (existing && !Schema.toEquivalence(RepairEvidenceReference)(existing, evidence))
    return { status: "reject" };
  // Recaptures are retained but cannot replace an accepted decision's evidence.
  return current.source.selectedEventId === evidence.sourceEventId &&
    current.source.release === evidence.release &&
    current.source.sourceRevision === evidence.sourceRevision &&
    !current.activeAttemptId &&
    current.decisionId === null
    ? { status: "select", evidenceId: evidence.manifestId }
    : { status: "retain" };
};

/** Called after settlement releases the handoff, with the newest eligible retained occurrence. */
export const selectSettlementOccurrence = (
  current: SelectedSource,
  latest: (SelectedSource & { readonly evidenceId: string | null }) | undefined,
): Selection | null => {
  if (
    !latest ||
    latest.selectedEventId === current.selectedEventId ||
    (latest.release === current.release && latest.sourceRevision === current.sourceRevision)
  )
    return null;
  return {
    selectedEventId: latest.selectedEventId,
    release: latest.release,
    sourceRevision: latest.sourceRevision,
    evidenceId: latest.evidenceId,
    decisionId: null,
  };
};
