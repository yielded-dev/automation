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

/** The host owns the template. Agents supply bounded content, never an arbitrary PR body. */
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

/** A final handoff is bound to the saved checkout; detailed notes stay in private tool storage. */
export const RepairReport = Schema.Struct({
  candidateSha: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/)),
  summary: Schema.NullOr(RepairSummary),
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
// Retained reports may predate captures; every newly generated report must decide
// whether the change is visual. This does not invalidate earlier run receipts.
const reportSchema = toCodecOpenAI(
  Schema.Struct({
    ...RepairReport.fields,
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

export const repairReportMarkdown = (report: RepairReport, capturesBase?: string) => {
  const captures = report.summary?.visualEvidence?.captures ?? [];
  const files = captures.map(
    (capture) =>
      `${capturesBase ? `[${markdownText(capture.caption)}](${capturesBase}/${capture.name})` : `${markdownText(capture.caption)}: \`${repairReviewDirectory}/${capture.name}\``} — ${capture.role}, revision \`${capture.revision}\`, SHA-256 \`${capture.sha256}\``,
  );

  return `${report.summary === null ? "# Run report" : `# ${markdownText(report.summary.title)}\n\n${repairSummaryMarkdown(report.summary)}`}${files.length === 0 ? "" : `\n\n## Captures\n\n${files.join("\n\n")}`}\n\n## Handoff notes\n\n${report.notes}\n\nCheckout: \`${report.candidateSha}\`\n`;
};
