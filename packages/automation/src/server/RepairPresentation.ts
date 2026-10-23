import { Context, Effect } from "effect";
import type { RunState } from "../domain/RepairExecution";
import type { RepairPublicationRequest } from "../domain/proof";
import {
  repairPublicationMarkdown,
  type RepairPublicationContext,
  type RepairPublicationLinks,
  type RepairReport,
  type RepairSummary,
} from "../domain/RepairReport";
import type { ConflictError } from "../domain/errors";
import type { SessionSandbox } from "@yielded/sandbox/SessionSandbox";
import type { RepairExecutionContext } from "./RepairExecutionContext";
import type { PublicationContent } from "./Publication";
import type { RepairHostError } from "./RepairHostError";

export const repairPublicationContent = (
  summary: RepairSummary,
  context: RepairPublicationContext,
  links: RepairPublicationLinks,
): PublicationContent => ({
  kind: "repair",
  changeKind: context.changeKind,
  title: summary.title,
  body: repairPublicationMarkdown(summary, context, links),
});

/** The host retains reviewed media and supplies authorized metadata and routes to the template. */
export class RepairPresentation extends Context.Service<
  RepairPresentation,
  {
    readonly retain: (
      report: RepairReport,
    ) => Effect.Effect<
      void,
      RepairHostError | ConflictError,
      SessionSandbox | RepairExecutionContext
    >;
    readonly publicationContent: (
      state: RunState,
      request: RepairPublicationRequest,
    ) => Effect.Effect<PublicationContent, RepairHostError | ConflictError, RepairExecutionContext>;
  }
>()("@reve/repairs/RepairPresentation") {}
