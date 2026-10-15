import { Context, Effect } from "effect";
import type { RunState } from "../domain/RepairExecution";
import type { RepairPublicationRequest } from "../domain/proof";
import type { RepairReport } from "../domain/RepairReport";
import type { ConflictError } from "../domain/errors";
import type { SessionSandbox } from "@yielded/sandbox/SessionSandbox";
import type { RepairExecutionContext } from "./RepairExecutionContext";
import type { PublicationContent } from "./Publication";
import type { RepairHostError } from "./RepairHostError";

/** The host retains reviewed media, sanitizes content, and renders its PR presentation. */
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
