import { Schema } from "effect";
//#region src/domain/RepairRequestKind.ts
const RepairRequestKind = Schema.Literals(["bug", "feature"]);
const requestKindLabel = {
	bug: "Bug",
	feature: "Feature request"
};
//#endregion
export { RepairRequestKind, requestKindLabel };
