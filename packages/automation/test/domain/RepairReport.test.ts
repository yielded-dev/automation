import { Predicate, Schema } from "effect";
import { expect, it } from "vite-plus/test";
import {
  RepairReport,
  RepairReportJson,
  repairReportOutputSchema,
} from "../../src/domain/RepairReport";

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

it("keeps ordinary report arrays and legacy optional fields decodable", () => {
  expect(decode(report)).toEqual(report);
  const { visualEvidence: _, ...summary } = report.summary;
  const legacy = {
    ...report,
    summary: { ...summary, keyChange: { path: "src/report.ts", diff: "x" } },
  };
  expect(decode(legacy)).toEqual(legacy);
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
});
