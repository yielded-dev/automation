import { Schema } from "effect";

import { RepairSourceRevision } from "./launch";
import { RepositoryPath } from "./RepositoryPath";

export const investigationTitleLength = 160;
export const InvestigationTitle = Schema.NonEmptyString.check(
  Schema.isMaxLength(investigationTitleLength),
);

const Text = Schema.NonEmptyString;
const Items = Schema.Array(Text);

/** Findings are hypotheses supported by retained evidence and actual source reads. */
export const InvestigationReport = Schema.Struct({
  summary: Text,
  observedBehavior: Text,
  expectedBehavior: Text,
  rootCause: Text,
  suggestedFix: Text,
  reproduction: Text,
  supportingSources: Items,
  affectedPaths: Schema.Array(RepositoryPath),
  missingEvidence: Items,
  alreadyFixed: Schema.Boolean,
});
export type InvestigationReport = typeof InvestigationReport.Type;

/** The title names the incident; the report remains the retained investigation and repair handoff. */
export const InvestigationOutput = Schema.Struct({
  title: InvestigationTitle.annotate({
    description:
      "A short incident name describing the affected component and observed failure or requested capability. Prefer 5–12 words. No IDs, status labels, credentials or user content. Do not assert an unproven root cause.",
  }),
  ...InvestigationReport.fields,
});

/** One retained report contract, with writing guidance chosen by the router. */
export const FeaturePlanOutput = Schema.Struct({
  ...InvestigationOutput.fields,
  summary: Text.annotate({
    description:
      "A concise feature proposal in one or two paragraphs. Lead with the user outcome and recommended scope. Do not embed the full plan, section headings, testing instructions or a sources appendix here.",
  }),
  observedBehavior: Text.annotate({
    description: "Current capabilities and relevant extension points in the inspected target code.",
  }),
  expectedBehavior: Text.annotate({
    description:
      "The requested experience and the smallest useful scope, including explicit boundaries.",
  }),
  rootCause: Text.annotate({
    description:
      "For this feature, explain the capability gap and design constraints. This is not a defect diagnosis.",
  }),
  suggestedFix: Text.annotate({
    description:
      "A concrete implementation plan in dependency order: affected components, data/API/UI changes, and meaningful design choices or tradeoffs. Respect the repository's ownership rules.",
  }),
  reproduction: Text.annotate({
    description:
      "Concrete acceptance checks for the requested workflow and existing behavior at risk. Do not repeat repository testing policy or disclaim that no suite is proposed. Use existing checks and direct product verification by default; regression automation needs an established regression or explicitly requested automation with a concrete coverage gap.",
  }),
  missingEvidence: Items.annotate({
    description:
      "Only unresolved product or technical questions that affect the plan. Empty when there are none. No boilerplate about absent errors, unrun tests, missing historical reports or unavailable production access.",
  }),
  supportingSources: Items.annotate({
    description:
      "Exact IDs of the request and inspected implementation sources that informed this plan. Put source IDs here only; the prose fields must not contain source IDs, citation markers or a references appendix.",
  }),
  alreadyFixed: Schema.Boolean.annotate({
    description: "True only when the inspected target already provides the requested capability.",
  }),
});

export const InvestigationSource = Schema.Struct({
  id: Schema.NonEmptyString.check(Schema.isMaxLength(700)),
  kind: Schema.Literals(["evidence", "repository", "repair"]),
  path: Schema.NonEmptyString.check(Schema.isMaxLength(512)),
  revision: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40,64}$/)),
  excerpt: Schema.String,
  truncated: Schema.Boolean,
});
export type InvestigationSource = typeof InvestigationSource.Type;

export class RepairSourceLimit extends Schema.TaggedError<RepairSourceLimit>()(
  "RepairSourceLimit",
  { reason: Schema.Literal("tree_size") },
) {}

export class RepairSourceReadError extends Schema.TaggedError<RepairSourceReadError>()(
  "RepairSourceReadError",
  {
    reason: Schema.Literals(["source_limit", "source_unavailable", "internal_failure"]),
    cause: Schema.Defect(),
  },
) {}

export class InvestigationFailure extends Schema.TaggedError<InvestigationFailure>()(
  "RepairInvestigationFailure",
  {
    reason: Schema.Literals([
      "provider_failure",
      "provider_timeout",
      "invalid_output",
      "not_allowlisted",
      "source_unavailable",
      "investigation_input_limit",
      "runtime_unavailable",
      "deadline",
      "interrupted",
    ]),
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

const EvidenceIdentifier = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256));

export const RepairEvidenceReference = Schema.Struct({
  incidentId: Schema.String.check(Schema.isUUID()),
  manifestId: Schema.String.check(Schema.isUUID()),
  manifestHash: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  objectKey: EvidenceIdentifier,
  sourceEventId: EvidenceIdentifier,
  release: EvidenceIdentifier,
  sourceRevision: RepairSourceRevision,
  completeness: Schema.Literals(["complete", "partial", "unavailable", "empty"]),
  decisive: Schema.Boolean,
  capturedAtMs: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
});
export type RepairEvidenceReference = typeof RepairEvidenceReference.Type;
