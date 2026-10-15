import { Schema } from "effect";

export const RepairRequestKind = Schema.Literals(["bug", "feature"]);
export type RepairRequestKind = typeof RepairRequestKind.Type;

export const requestKindLabel = {
  bug: "Bug",
  feature: "Feature request",
} satisfies Record<RepairRequestKind, string>;
