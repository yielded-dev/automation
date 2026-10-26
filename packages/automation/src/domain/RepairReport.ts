import { Schema } from "effect";
import { toCodecOpenAI } from "effect/ai/OpenAiStructuredOutput";

import { RepositoryPath } from "./RepositoryPath";

const line = (maximum: number) =>
  Schema.NonEmptyString.check(
    Schema.isMaxLength(maximum),
    Schema.isPattern(/^[^\r\n]+$/),
    Schema.makeFilter((text) => text.trim() === text || "Use trimmed, nonempty text"),
  );

// Preserve nonempty domain validation while exposing ordinary JSON arrays to OpenAI.
const nonEmptyJsonArray = <S extends Schema.Constraint>(item: S, maximum: number) => {
  const bound = Schema.isMaxLength(maximum);
  return Schema.NonEmptyArray(item)
    .check(bound)
    .pipe(Schema.encodeTo(Schema.Array(item).check(Schema.isMinLength(1), bound)));
};

const BehaviorChange = Schema.Struct({
  nodes: nonEmptyJsonArray(
    Schema.Struct({ label: line(80), kind: Schema.Literals(["action", "decision"]) }),
    6,
  ),
  edges: nonEmptyJsonArray(
    Schema.Struct({ from: Schema.Natural, to: Schema.Natural, label: Schema.NullOr(line(24)) }),
    8,
  ),
}).check(
  Schema.makeFilter(
    (flow) =>
      flow.edges.every((edge) => edge.from < flow.nodes.length && edge.to < flow.nodes.length) ||
      "Every edge must refer to a node in the diagram",
  ),
);

export const repairReviewDirectory = "/tmp/repair-review";
export const repairReviewAssetLimit = 10 * 1024 * 1024;
export const RepairReviewAsset = Schema.Struct({
  name: Schema.String.check(Schema.isPattern(/^[a-z0-9][a-z0-9-]{0,79}\.(?:png|mp4)$/)),
  sha256: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  bytes: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: repairReviewAssetLimit })),
  caption: line(240),
  revision: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/)),
  role: Schema.Literals(["before", "after"]),
  reviewedForPublication: Schema.Literal(true),
});
export type RepairReviewAsset = typeof RepairReviewAsset.Type;
export const repairReviewObjectKey = (
  runId: string,
  candidateSha: string,
  asset: RepairReviewAsset,
) => `review/${runId}/${candidateSha}/${asset.sha256}/${asset.name}`;
export const repairCapturePath = (incidentId: string, runId: string, name: string) =>
  `/api/incidents/${incidentId}/runs/${runId}/captures/${name}`;
export const RepairVisualEvidence = Schema.Struct({
  captures: Schema.Array(RepairReviewAsset).check(Schema.isMaxLength(4)),
  blocker: Schema.NullOr(line(480)),
}).check(
  Schema.makeFilter(
    (evidence) =>
      ((evidence.captures.some((capture) => capture.role === "after") ||
        evidence.blocker !== null) &&
        new Set(evidence.captures.map((capture) => capture.name)).size ===
          evidence.captures.length) ||
      "Visible changes need a final capture or a concrete blocker, with unique file names",
  ),
);
const exampleLanguage = Schema.String.check(Schema.isPattern(/^[a-z][a-z0-9-]{0,23}$/));
const CodeExample = Schema.Struct({
  path: RepositoryPath,
  language: Schema.optionalKey(exampleLanguage),
  diff: Schema.NonEmptyString.check(
    Schema.isMaxLength(2_400),
    Schema.makeFilter((text) => text.split("\n").length <= 30 || "Use at most 30 excerpt lines"),
  ),
});

/** Automation owns the template. Agents supply bounded content, never an arbitrary PR body. */
export const RepairSummary = Schema.Struct({
  title: line(100).annotate({
    description: "Name the concrete feature or fix; never an issue ID or status.",
  }),
  problem: line(480).annotate({
    description:
      "For a feature, briefly describe the requested capability and why it is useful. For a bug, state the trigger, failure and impact. Write for a reviewer without the original context, usually in one sentence.",
  }),
  fix: line(480).annotate({
    description:
      "Briefly state the final change and resulting behavior. Usually one sentence; no implementation history or check results.",
  }),
  behaviorChange: Schema.NullOr(BehaviorChange).annotate({
    description:
      "Follow open-pull-request/references/explanation.md: show meaningful ownership, boundary or data-flow changes when a diagram explains them clearly. Name actual components and interactions. Null when prose is sufficient.",
  }),
  keyChange: Schema.NullOr(CodeExample).annotate({
    description:
      "A small code excerpt or caller example clarifying a changed contract or subtle behavior. The diff field contains the code; language selects its syntax (diff when omitted). Path names its source. Label illustrative inputs and outputs in the explanation. Null when prose is sufficient.",
  }),
  reviewNotes: Schema.Array(line(240)).check(Schema.isMaxLength(3)).annotate({
    description:
      "Concrete risks, unresolved uncertainty or manual steps that affect review. Empty when none; never generic review advice or CI status.",
  }),
  visualEvidence: Schema.optionalKey(Schema.NullOr(RepairVisualEvidence)).annotate({
    description:
      "Null for nonvisual changes. For visible changes, reviewed captures in /tmp/repair-review, or a concrete capture blocker. SHA-256, bytes and revision identify the exact inspected file. Never include private incident images or unreviewed media.",
  }),
});
export type RepairSummary = typeof RepairSummary.Type;

const RepairScenarioObservation = Schema.Struct({
  revision: Schema.String.check(
    Schema.isPattern(/^[a-f0-9]{40}$/u),
    Schema.isMaxLength(40),
  ).annotate({
    description: "Exact commit actually tested for the accepted-incident scenario.",
  }),
  command: Schema.NonEmptyString.check(Schema.isMaxLength(2_000), Schema.isPattern(/\S/u)).annotate(
    {
      description:
        "Command actually run to exercise the accepted-incident scenario in the reported runtime, not an unrelated check. No credentials.",
    },
  ),
  observed: Schema.NonEmptyString.check(
    Schema.isMaxLength(4_000),
    Schema.isPattern(/\S/u),
  ).annotate({
    description:
      "Actual observed outcome of the accepted-incident scenario, not an expected result or unrelated check summary. No credentials or raw user content.",
  }),
});

/** Agent-reported incident evidence, not independently executed proof. Hosts own publication policy. */
export const RepairVerification = Schema.Struct({
  status: Schema.Literals(["fixed", "unresolved"]).annotate({
    description:
      "Reported outcome for the accepted incident. Use unresolved when the incident remains or its fix could not be verified; unrelated passing checks do not establish fixed.",
  }),
  evidenceHash: Schema.String.check(
    Schema.isPattern(/^[a-f0-9]{64}$/u),
    Schema.isMaxLength(64),
  ).annotate({
    description:
      "Exact SHA-256 hash of the accepted incident evidence supplied by the host; never substitute a hash of new observations or check output.",
  }),
  runtime: line(64).annotate({
    description:
      "Host runtime identifier for the accepted-incident scenario, such as workerd or cloudflare. Both observations must come from this runtime; unrelated runtime checks do not qualify.",
  }),
  before: Schema.NullOr(RepairScenarioObservation).annotate({
    description:
      "Actual observation of the accepted-incident scenario before the repair, in the reported runtime. Null if it was not observed there.",
  }),
  after: Schema.NullOr(RepairScenarioObservation).annotate({
    description:
      "Actual observation of the same accepted-incident scenario after the repair, in the same reported runtime. Null if it was not observed there.",
  }),
}).annotate({
  description:
    "Private, agent-reported incident verification, not independently executed proof. The host evaluates evidence identity, runtime, tested commits and outcomes under its publication policy.",
});
export type RepairVerification = typeof RepairVerification.Type;

/** Detects a reported fix with before/after observations, not independently verified success. */
export const hasRepairVerification = (report: {
  readonly verification?: RepairVerification | null;
}) => {
  const verification = report.verification;

  return (
    verification?.status === "fixed" && verification.before !== null && verification.after !== null
  );
};

/** A final handoff is bound to the saved checkout; detailed notes stay in private tool storage. */
export const RepairReport = Schema.Struct({
  candidateSha: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/)),
  summary: Schema.NullOr(RepairSummary),
  verification: Schema.optionalKey(Schema.NullOr(RepairVerification)),
  notes: Schema.NonEmptyString.check(Schema.isMaxLength(24_000)).annotate({
    description:
      "Private Markdown handoff. For a feature: delivered scope, implementation decisions, acceptance checks and remaining work. For a bug: diagnosis, reproduction, fix and verification. Include inspected files and actual check outcomes. Distinguish observations from hypotheses. No credentials or raw user content.",
  }),
  reviewResponses: Schema.Array(
    Schema.Struct({
      commentId: Schema.String.check(Schema.isUUID()),
      outcome: Schema.Literals(["addressed", "declined"]),
      note: line(480).annotate({
        description:
          "One or two sentences for the reviewer: what changed for this comment, or why it was declined.",
      }),
    }),
  )
    .check(Schema.isMaxLength(50))
    .annotate({
      description:
        "Empty unless the developer request lists review comments; then one entry per listed comment ID.",
    }),
});
export type RepairReport = typeof RepairReport.Type;
export const RepairReportJson = Schema.fromJsonString(RepairReport);
// Retained reports may predate captures or incident verification. New outputs must
// explicitly supply null or structured data without invalidating earlier receipts.
const reportSchema = toCodecOpenAI(
  Schema.Struct({
    ...RepairReport.fields,
    verification: Schema.NullOr(RepairVerification),
    summary: Schema.NullOr(
      Schema.Struct({
        ...RepairSummary.fields,
        keyChange: Schema.NullOr(
          Schema.Struct({ ...CodeExample.fields, language: exampleLanguage }),
        ),
        visualEvidence: Schema.NullOr(RepairVisualEvidence),
      }),
    ),
  }),
);

// The provider schema is lossy; RepairReportJson remains the host validation boundary.
export const repairReportOutputSchema = reportSchema.jsonSchema;

const markdownText = (text: string) => text.replace(/[\\`*_[\]<>#!|~]/g, "\\$&");
const fenced = (language: string, text: string) => {
  const fence = "`".repeat(
    Math.max(3, ...[...text.matchAll(/`+/g)].map((match) => match[0].length + 1)),
  );

  return `${fence}${language}\n${text}\n${fence}`;
};
const diagramLabel = (text: string) =>
  text
    .replaceAll("&", "#38;")
    .replaceAll('"', "#quot;")
    .replaceAll("<", "#lt;")
    .replaceAll(">", "#gt;");

export const repairSummaryMarkdown = (summary: RepairSummary): string => {
  const sections = [markdownText(summary.problem), markdownText(summary.fix)];

  if (summary.behaviorChange !== null) {
    const flow = summary.behaviorChange;
    const diagram = [
      "flowchart LR",
      ...flow.nodes.map((node, index) =>
        node.kind === "decision"
          ? `  N${index}{"${diagramLabel(node.label)}"}`
          : `  N${index}["${diagramLabel(node.label)}"]`,
      ),
      ...flow.edges.map((edge) =>
        edge.label === null
          ? `  N${edge.from} --> N${edge.to}`
          : `  N${edge.from} -->|"${diagramLabel(edge.label)}"| N${edge.to}`,
      ),
    ].join("\n");

    sections.push("## Architecture", fenced("mermaid", diagram));
  }
  if (summary.keyChange !== null)
    sections.push(
      "## Key change",
      markdownText(summary.keyChange.path),
      fenced(summary.keyChange.language ?? "diff", summary.keyChange.diff),
    );
  if (summary.visualEvidence?.blocker)
    sections.push("## Visual evidence", markdownText(summary.visualEvidence.blocker));
  if (summary.reviewNotes.length > 0)
    sections.push(
      "## Worth a closer look",
      summary.reviewNotes.map((note) => `- ${markdownText(note)}`).join("\n"),
    );

  return sections.join("\n\n");
};

export const RepairChangeKind = Schema.Literals(["bug", "eval", "feature"]);
export type RepairChangeKind = typeof RepairChangeKind.Type;

export interface RepairPublicationContext {
  readonly runId: string;
  readonly changeKind: RepairChangeKind;
  readonly evaluation?: {
    readonly outcome: "passed" | "unproven" | "no_candidate";
    readonly runs: ReadonlyArray<{
      readonly id: string;
      readonly mode: "baseline" | "candidate";
      readonly passed: number;
      readonly total: number;
      readonly failed: number;
      readonly skipped: number;
      readonly errors: number;
      readonly current: boolean;
    }>;
  };
}

/** Trusted host routes, never model output. Use public HTTP(S) URLs without credentials. */
export interface RepairPublicationLinks {
  readonly repair: (runId: string) => string;
  readonly source?: { readonly label: string; readonly url: string };
  readonly evaluation: (runId: string) => string;
}

const changeKindLabels = { bug: "Bug fix", eval: "Eval change", feature: "Feature" } as const;
const publicationLink = (label: string, url: string) =>
  `[${markdownText(label)}](${url
    .replace(/[()]/g, (character) => (character === "(" ? "%28" : "%29"))
    .replace(/[\s<>\\]/g, (character) => encodeURIComponent(character))})`;

/** Render the shared PR template with host-owned classification, evidence and destinations. */
export const repairPublicationMarkdown = (
  summary: RepairSummary,
  context: RepairPublicationContext,
  links: RepairPublicationLinks,
): string => {
  const sections = [
    `**${changeKindLabels[context.changeKind]}**`,
    repairSummaryMarkdown(summary),
    [
      `- Repair run: ${publicationLink(context.runId, links.repair(context.runId))}`,
      ...(links.source ? [`- ${publicationLink(links.source.label, links.source.url)}`] : []),
    ].join("\n"),
  ];

  if (context.evaluation) {
    const evaluation = context.evaluation;
    const outcomes = {
      passed:
        "The latest historical prompt experiment passed its selected checks. CI must verify the committed regression and implementation together.",
      unproven:
        "The latest prompt trial has not established a passing correction. Review the recorded failures or missing evidence before adopting this proposal.",
      no_candidate:
        "This proposal has no candidate prompt trial. Review its recorded baseline and separate source-code validation.",
    } as const;

    sections.push(
      "## Prompt evaluation",
      outcomes[evaluation.outcome],
      evaluation.runs
        .map(
          (run) =>
            `- ${publicationLink(`${run.mode} ${run.id}`, links.evaluation(run.id))}: ${run.passed}/${run.total} passed, ${run.failed} failed, ${run.skipped} skipped, ${run.errors} errors${run.current ? "" : "; stale"}.`,
        )
        .join("\n"),
      "These are historical prompt trials; the repair notes identify the selected policy. Match the proposed policy to its passing trial before adoption. Replays use the recorded deployed application revision and do not certify changed source code. No prompt was activated and no code was merged.",
    );
  }

  return sections.filter(Boolean).join("\n\n");
};

export const repairReportMarkdown = (report: RepairReport, capturesBase?: string) => {
  const captures = report.summary?.visualEvidence?.captures ?? [];
  const files = captures.map(
    (capture) =>
      `${capturesBase ? `[${markdownText(capture.caption)}](${capturesBase}/${capture.name})` : `${markdownText(capture.caption)}: \`${repairReviewDirectory}/${capture.name}\``} — ${capture.role}, revision \`${capture.revision}\`, SHA-256 \`${capture.sha256}\``,
  );

  return `${report.summary === null ? "# Run report" : `# ${markdownText(report.summary.title)}\n\n${repairSummaryMarkdown(report.summary)}`}${files.length === 0 ? "" : `\n\n## Captures\n\n${files.join("\n\n")}`}\n\n## Handoff notes\n\n${report.notes}\n\nCheckout: \`${report.candidateSha}\`\n`;
};
