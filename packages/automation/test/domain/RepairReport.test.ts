import { Predicate, Schema } from "effect";
import { expect, it } from "vite-plus/test";
import {
  RepairReport,
  RepairReportJson,
  RepairVerification,
  repairReportOutputSchema,
  repairPublicationMarkdown,
} from "../../src/domain/RepairReport";
import { RepairPublicationRequest } from "../../src/domain/proof";

// Independent of Effect's transformer: validate every schema node, including definitions.
// https://developers.openai.com/api/docs/guides/structured-outputs#supported-schemas
const keywords = new Set([
  "$defs",
  "$ref",
  "type",
  "title",
  "description",
  "enum",
  "anyOf",
  "properties",
  "required",
  "additionalProperties",
  "items",
  "pattern",
  "format",
  "multipleOf",
  "minimum",
  "exclusiveMinimum",
  "maximum",
  "exclusiveMaximum",
  "minItems",
  "maxItems",
]);
const types = ["object", "array", "string", "number", "integer", "boolean", "null"];
const formats = [
  "date-time",
  "time",
  "date",
  "duration",
  "email",
  "hostname",
  "ipv4",
  "ipv6",
  "uuid",
];

it("emits an entire Codex output schema within OpenAI's supported subset", () => {
  let properties = 0;
  let enumValues = 0;
  let stringSize = 0;
  const root = repairReportOutputSchema;
  const visit = (input: unknown, path: string, depth: number) => {
    if (!Predicate.isObject(input)) throw new Error(`Expected schema object at ${path}`);
    const schema = input;
    expect(
      Object.keys(schema).filter((key) => !keywords.has(key)),
      path,
    ).toEqual([]);
    expect(depth, path).toBeLessThanOrEqual(10);
    if (schema.type !== undefined)
      for (const type of Array.isArray(schema.type) ? schema.type : [schema.type])
        expect(types, path).toContain(type);
    if (schema.format !== undefined) expect(formats, path).toContain(schema.format);
    if (schema.$ref !== undefined) {
      expect(schema.$ref, path).toMatch(/^#\/\$defs\/[^/]+$/);
      expect(root.$defs, path).toHaveProperty(String(schema.$ref).slice(8));
    }
    if (schema.type === "object") {
      expect(schema.additionalProperties, path).toBe(false);
      if (!Predicate.isObject(schema.properties)) throw new Error(`Missing properties at ${path}`);
      const names = Object.keys(schema.properties);
      expect(schema.required, path).toEqual(names);
      properties += names.length;
      stringSize += names.join("").length;
      for (const [name, child] of Object.entries(schema.properties))
        visit(child, `${path}.properties.${name}`, depth + 1);
    }
    if (schema.type === "array") visit(schema.items, `${path}.items`, depth + 1);
    if (schema.anyOf !== undefined) {
      expect(Array.isArray(schema.anyOf), path).toBe(true);
      for (const [index, child] of (schema.anyOf as unknown[]).entries())
        visit(child, `${path}.anyOf[${index}]`, depth);
    }
    if (schema.$defs !== undefined) {
      if (!Predicate.isObject(schema.$defs)) throw new Error(`Invalid definitions at ${path}`);
      for (const [name, child] of Object.entries(schema.$defs)) {
        stringSize += name.length;
        visit(child, `${path}.$defs.${name}`, depth);
      }
    }
    if (schema.enum !== undefined) {
      expect(Array.isArray(schema.enum), path).toBe(true);
      const values = schema.enum as unknown[];
      enumValues += values.length;
      const size = values.filter(Predicate.isString).join("").length;
      stringSize += size;
      if (values.length > 250) expect(size, path).toBeLessThanOrEqual(15_000);
    }
  };
  expect(root.type).toBe("object");
  expect(root.anyOf).toBeUndefined();
  expect(root.required).toContain("verification");
  expect(root).toHaveProperty("properties.verification.anyOf.0.required", [
    "status",
    "evidenceHash",
    "runtime",
    "before",
    "after",
  ]);
  expect(root).toHaveProperty("properties.verification.anyOf.1.type", "null");
  const flow = "properties.summary.anyOf.0.properties.behaviorChange.anyOf.0.properties";
  expect(root).not.toHaveProperty(`${flow}.nodes.prefixItems`);
  expect(root).not.toHaveProperty(`${flow}.edges.prefixItems`);
  expect(root).not.toHaveProperty(`${flow}.edges.items.properties.from.allOf`);
  expect(root).toHaveProperty(`${flow}.nodes.type`, "array");
  expect(root).toHaveProperty(`${flow}.nodes.minItems`, 1);
  expect(root).toHaveProperty(`${flow}.edges.type`, "array");
  expect(root).toHaveProperty(`${flow}.edges.minItems`, 1);
  expect(root).toHaveProperty(`${flow}.edges.items.properties.from.type`, "integer");
  expect(root).toHaveProperty(`${flow}.edges.items.properties.from.minimum`, 0);
  visit(root, "$", 1);
  expect(properties).toBeLessThanOrEqual(5000);
  expect(enumValues).toBeLessThanOrEqual(1000);
  expect(stringSize).toBeLessThanOrEqual(120_000);
});

const report = {
  candidateSha: "a".repeat(40),
  summary: {
    title: "Fix report schema",
    problem: "Repairs fail before coding.",
    fix: "Generate a supported output schema.",
    behaviorChange: {
      nodes: [{ label: "Report", kind: "action" }],
      edges: [{ from: 0, to: 0, label: null }],
    },
    keyChange: { path: "src/report.ts", language: "ts", diff: "const fixed = true;" },
    reviewNotes: [],
    visualEvidence: null,
  },
  notes: "Verified the report boundary.",
  reviewResponses: [],
};
const decode = (input: unknown) =>
  Schema.decodeUnknownSync(RepairReportJson)(JSON.stringify(input));

it("links the exact repair and source using the host's routes", () => {
  const summary = decode(report).summary!;
  const body = repairPublicationMarkdown(
    summary,
    { runId: "repair-1", changeKind: "bug" },
    {
      repair: (id) => `https://admin.example.com/bugs/source?repair=${id}`,
      source: { label: "Bug [report]", url: "https://admin.example.com/bugs/source(1)" },
      evaluation: (id) => `https://admin.example.com/evals?run=${id}`,
    },
  );
  expect(body).toContain("**Bug fix**");
  expect(body).toContain("[repair-1](https://admin.example.com/bugs/source?repair=repair-1)");
  expect(body).toContain("[Bug \\[report\\]](https://admin.example.com/bugs/source%281%29)");
  expect(body).not.toContain("## Prompt evaluation");
});

it("distinguishes eval changes and links failed and stale historical trials", () => {
  const body = repairPublicationMarkdown(
    decode(report).summary!,
    {
      runId: "repair-2",
      changeKind: "eval",
      evaluation: {
        outcome: "unproven",
        runs: [
          {
            id: "trial-1",
            mode: "candidate",
            passed: 1,
            total: 3,
            failed: 1,
            skipped: 0,
            errors: 1,
            current: false,
          },
        ],
      },
    },
    {
      repair: (id) => `https://console.example.org/repairs/${id}`,
      evaluation: (id) => `https://console.example.org/runs/${id}`,
    },
  );
  expect(body).toContain("**Eval change**");
  expect(body).toContain("[repair-2](https://console.example.org/repairs/repair-2)");
  expect(body).toContain(
    "[candidate trial-1](https://console.example.org/runs/trial-1): 1/3 passed, 1 failed, 0 skipped, 1 errors; stale.",
  );
  expect(body).toContain("has not established a passing correction");
  expect(body).toContain("do not certify changed source code");
});
const PublicationJson = Schema.fromJsonString(RepairPublicationRequest);

it("keeps ordinary report arrays and legacy optional fields decodable", () => {
  expect(decode(report)).toEqual(report);
  const { visualEvidence: _, ...summary } = report.summary;
  const legacy = {
    ...report,
    summary: { ...summary, keyChange: { path: "src/report.ts", diff: "x" } },
  };
  expect(decode(legacy)).toEqual(legacy);
  expect(decode({ ...legacy, verification: null })).toEqual({ ...legacy, verification: null });
  const publication = { candidateSha: legacy.candidateSha, summary: legacy.summary };
  expect(Schema.decodeUnknownSync(PublicationJson)(JSON.stringify(publication))).toEqual(
    publication,
  );
});

const verification: RepairVerification = {
  status: "fixed",
  evidenceHash: "c".repeat(64),
  runtime: "workerd",
  before: {
    revision: "b".repeat(40),
    command: "vp run test:duplicate-delivery",
    observed: "Repeated delivery created two records.",
  },
  after: {
    revision: report.candidateSha,
    command: "vp run test:duplicate-delivery",
    observed: "Repeated delivery created one record.",
  },
};

it.each([
  ["fixed", verification],
  ["unresolved", { ...verification, status: "unresolved", runtime: "cloudflare", after: null }],
  ["unobserved", { ...verification, status: "unresolved", before: null, after: null }],
  [
    "multiline",
    {
      ...verification,
      after: {
        ...verification.after,
        command: "vp run test:duplicate-delivery \\\n  --runtime workerd",
        observed: "First delivery created one record.\nRepeated delivery reused that record.",
      },
    },
  ],
  ["null", null],
])("round-trips %s verification through report and publication JSON", (_, value) => {
  const input = { ...report, verification: value };
  const decoded = decode(input);
  expect(decoded).toEqual(input);
  expect(decode(JSON.parse(Schema.encodeSync(RepairReportJson)(decoded)))).toEqual(input);

  const publication = {
    candidateSha: decoded.candidateSha,
    summary: decoded.summary,
    verification: decoded.verification,
  };
  const accepted = Schema.decodeUnknownSync(RepairPublicationRequest)(publication);
  expect(accepted).toEqual(publication);
  expect(
    Schema.decodeUnknownSync(PublicationJson)(Schema.encodeSync(PublicationJson)(accepted)),
  ).toEqual(publication);
});

it("retains host-only report constraints after provider schema conversion", () => {
  const flow = report.summary.behaviorChange;
  for (const behaviorChange of [
    { ...flow, nodes: [] },
    { ...flow, edges: [] },
    { ...flow, nodes: Array(7).fill(flow.nodes[0]) },
    { ...flow, edges: Array(9).fill(flow.edges[0]) },
    { ...flow, edges: [{ from: -1, to: 0, label: null }] },
    { ...flow, edges: [{ from: 0.5, to: 0, label: null }] },
    { ...flow, edges: [{ from: 0, to: 1, label: null }] },
  ]) {
    const invalid = { ...report, summary: { ...report.summary, behaviorChange } };
    expect(() => decode(invalid)).toThrow();
    expect(Schema.is(RepairReport)(invalid)).toBe(false);
  }
  for (const summary of [
    { ...report.summary, title: " untrimmed" },
    { ...report.summary, title: "x".repeat(101) },
    { ...report.summary, keyChange: { ...report.summary.keyChange, path: "../private" } },
    { ...report.summary, keyChange: { ...report.summary.keyChange, diff: "x\n".repeat(31) } },
    { ...report.summary, visualEvidence: { captures: [], blocker: null } },
  ])
    expect(() => decode({ ...report, summary })).toThrow();

  for (const invalid of [
    { ...verification, status: "passed" },
    { ...verification, evidenceHash: "c".repeat(63) },
    { ...verification, evidenceHash: `${"c".repeat(64)}\n` },
    { ...verification, evidenceHash: "z".repeat(64) },
    ...["", " workerd", "workerd ", "x".repeat(65)].map((runtime) => ({
      ...verification,
      runtime,
    })),
    { ...verification, before: { ...verification.before, revision: "b".repeat(39) } },
    { ...verification, before: { ...verification.before, revision: `${"b".repeat(40)}\n` } },
    { ...verification, after: { ...verification.after, command: "" } },
    { ...verification, after: { ...verification.after, command: " \t\n " } },
    { ...verification, after: { ...verification.after, command: "x".repeat(2_001) } },
    { ...verification, after: { ...verification.after, observed: "" } },
    { ...verification, after: { ...verification.after, observed: " \t\n " } },
    { ...verification, after: { ...verification.after, observed: "x".repeat(4_001) } },
  ]) {
    expect(() => decode({ ...report, verification: invalid })).toThrow();
    expect(Schema.is(RepairVerification)(invalid)).toBe(false);
  }
});
