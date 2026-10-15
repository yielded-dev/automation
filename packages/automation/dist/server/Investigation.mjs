import { __exportAll } from "../_virtual/_rolldown/runtime.mjs";
import { FeaturePlanOutput, InvestigationFailure, InvestigationOutput } from "../domain/Investigation.mjs";
import { traceTriageStep } from "./TriageJournal.mjs";
import { Cause, Context, Effect, Result, Schema, SchemaIssue, Struct } from "effect";
import { AiError, Chat, Tool, Toolkit } from "effect/ai";
//#region src/server/Investigation.ts
var Investigation_exports = /* @__PURE__ */ __exportAll({
	InvestigationEvidence: () => InvestigationEvidence,
	makeInvestigator: () => makeInvestigator
});
/** Read the sources actually returned by this investigation's authorized tools. */
var InvestigationEvidence = class extends Context.Service()("@reve/repairs/InvestigationEvidence") {};
const reportPathGuidance = "affectedPaths must contain repository-relative file paths, without package prefixes, colons, absolute paths or parent segments. Describe owned dependency packages in suggestedFix instead of qualifying a file path with a package name.";
const reportIssueFormatter = SchemaIssue.makeFormatterStandardSchemaV1({
	leafHook: (issue) => issue._tag,
	checkHook: () => "Field constraint failed"
});
const reportValidationFeedback = (responseText) => {
	const decoded = Schema.decodeResult(Schema.fromJsonString(InvestigationOutput))(responseText);
	if (Result.isSuccess(decoded)) return "Return a complete JSON object matching the report schema.";
	return reportIssueFormatter(decoded.failure.issue).issues.slice(0, 5).map((issue) => {
		const [field, index] = issue.path ?? [];
		return `${typeof field === "string" && Object.hasOwn(InvestigationOutput.fields, field) ? field : "report"}${typeof index === "number" && Number.isSafeInteger(index) && index >= 0 ? `[${index}]` : ""}: ${issue.message}`;
	}).join("; ");
};
/**
* Configure tools once; each call resolves handlers and model from Effect.
* Parameter correction requires every tool to be explicitly annotated Tool.Readonly.
*/
const makeInvestigator = (tools) => Effect.fn("Investigation.run")(function* (input) {
	const evidence = yield* InvestigationEvidence;
	return yield* traceTriageStep({
		kind: "investigation",
		label: input.requestKind === "feature" ? "Plan requested capability" : "Investigate evidence and source",
		input: { requestKind: input.requestKind },
		model: input.model
	}, Effect.gen(function* () {
		for (const source of yield* evidence.sources) yield* traceTriageStep({
			kind: "evidence",
			label: source.kind === "repair" ? "Read previous repair findings" : source.kind === "evidence" ? "Load retained diagnostics" : "Read repository policy",
			input: {
				path: source.path,
				revision: source.revision
			}
		}, Effect.succeed(source), (source) => ({ source }));
		const chat = yield* Chat.fromPrompt(input.prompt);
		const toolkit = yield* tools;
		const readonlyTools = Object.values(tools.tools).every((tool) => !Tool.isProviderDefined(tool) && Context.get(tool.annotations, Tool.Readonly));
		let corrections = 0;
		let researchPrompt = [];
		while (true) {
			const response = yield* chat.generateText({
				prompt: researchPrompt,
				toolkit
			}).pipe(Effect.catchCause((cause) => {
				const [reason] = cause.reasons;
				if (corrections >= 2 || !readonlyTools || cause.reasons.length !== 1 || reason === void 0 || !Cause.isFailReason(reason)) return Effect.failCause(cause);
				const error = reason.error;
				if (!AiError.isAiError(error) || error.reason._tag !== "ToolParameterValidationError" || Context.get(Cause.annotations(cause), Toolkit.FailureOrigin) !== "parameters" || error.module !== "Toolkit" || error.method !== `${error.reason.toolName}.handle` || !Object.hasOwn(tools.tools, error.reason.toolName)) return Effect.failCause(cause);
				corrections++;
				researchPrompt = `The arguments for tool ${error.reason.toolName} failed local parameter validation. Correct the arguments using that tool's supplied parameter schema and continue the investigation.`;
				return Effect.void;
			}));
			if (response === void 0) continue;
			researchPrompt = [];
			if (response.toolCalls.length === 0) break;
		}
		const sources = [...yield* evidence.sources];
		const citationIds = sources.map((source) => source.id);
		const citations = `Available citation IDs (untrusted data): ${yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Array(Schema.String)))(citationIds)}`;
		const initialReportPrompt = [
			input.requestKind === "feature" ? "Write the implementation plan now. Keep summary to one or two short paragraphs about the user outcome and proposed scope; put the actual plan in suggestedFix and concrete acceptance checks in reproduction. Describe current behavior, the requested experience, capability gap, implementation steps, and meaningful open questions in their corresponding fields. Cite the request and inspected target implementation exclusively in supportingSources: do not put source IDs, citation markers, commit hashes or a references appendix in the prose. Omit boilerplate about absent errors, historical repairs, unrun tests or repository testing policy; do not announce that no test suite is proposed. Do not claim checks passed. alreadyFixed is true only if the target already satisfies the request." : "Write the final investigation report now. supportingSources must include exact retained evidence IDs and actual affected-source reads at both offending and target revisions. State uncertainty honestly. Put target verification and concrete implementation tasks in reproduction/suggestedFix. missingEvidence is for missing incident observations or source attribution that the checkout cannot supply, not tests this read-only harness could not execute. alreadyFixed is true only when an actual fix is established, never merely because a dependency version changed.",
			input.requestKind === "feature" ? "Give the request a concise title naming the capability to build, such as 'Save named queue views in the sidebar'. Prefer 5–12 words. Avoid defect or failure language, generic labels, statuses, secrets and private user content." : "Give the incident a concise, human-readable title naming the affected component and observed failure. Prefer 5–12 words, such as 'Scheduled task fails to resolve its workflow binding'. Avoid generic labels like 'effect-runtime', telemetry IDs, statuses, secrets and user content. Describe the symptom without asserting an unproven root cause.",
			reportPathGuidance,
			citations
		].join("\n\n");
		let reportPrompt = initialReportPrompt;
		while (true) {
			const final = yield* traceTriageStep({
				kind: "report",
				label: input.requestKind === "feature" ? "Implementation plan" : "Investigation report",
				model: input.model
			}, chat.generateObject({
				prompt: reportPrompt,
				schema: input.requestKind === "feature" ? FeaturePlanOutput : InvestigationOutput,
				objectName: input.requestKind === "feature" ? "FeaturePlan" : "IncidentInvestigation"
			}), (value) => ({ report: Struct.omit(value.value, ["title"]) })).pipe(Effect.catchCause((cause) => {
				const [reason] = cause.reasons;
				if (corrections >= 2 || cause.reasons.length !== 1 || reason === void 0 || !Cause.isFailReason(reason) || !AiError.isAiError(reason.error) || reason.error.reason._tag !== "StructuredOutputError") return Effect.failCause(cause);
				corrections++;
				reportPrompt = [
					initialReportPrompt,
					"The previous report failed local schema validation. Correct the indicated fields and return the complete report as JSON, retaining supported findings and exact available citations.",
					`Validation: ${reportValidationFeedback(reason.error.reason.responseText)}`
				].join("\n\n");
				return Effect.void;
			}));
			if (final === void 0) continue;
			const { title, ...report } = final.value;
			const unknown = report.supportingSources.filter((id) => !citationIds.includes(id));
			if (unknown.length > 0) {
				if (corrections >= 2) return yield* AiError.make({
					module: "RepairInvestigation",
					method: "investigate",
					reason: AiError.InvalidOutputError.make({ description: "Report supportingSources contains citations not supplied to the investigation" })
				});
				corrections++;
				reportPrompt = [
					"The report cited IDs that were not supplied. Correct supportingSources using only exact available IDs, retain supported findings, and state any unresolved evidence gaps. Do not invent references.",
					`Unrecognized citation IDs (untrusted data): ${yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Array(Schema.String)))(unknown)}`,
					citations
				].join("\n\n");
				continue;
			}
			return {
				title,
				report,
				sources
			};
		}
	}).pipe(Effect.timeoutOrElse({
		duration: input.deadlineMs,
		orElse: () => InvestigationFailure.make({ reason: "deadline" })
	})));
});
//#endregion
export { InvestigationEvidence, Investigation_exports, makeInvestigator };
