import { Cause, Schema } from "effect";
import type { AgentFailedEvent, AgentRecoveringEvent } from "@yielded/sandbox/Contract";
import { RepositorySetupError } from "@yielded/sandbox/model/RepositorySetup";

/** A lost workspace or connection is never evidence that an admitted command failed. */
export const repairSandboxTaskFailure = (
  cause: Cause.Cause<unknown>,
  codex: {
    readonly startDispatched: boolean;
    readonly result: { readonly terminalEventType: string } | null;
  } | null,
): AgentFailedEvent | AgentRecoveringEvent | undefined => {
  if (Cause.hasInterruptsOnly(cause)) return undefined;
  if (codex?.result != null && codex.result.terminalEventType !== "turn.completed")
    return {
      type: "agent.failed",
      reason: "turn-failed",
      message: "The repair agent ended without completing the repair.",
    };
  const failures = cause.reasons.flatMap((reason) =>
    Cause.isFailReason(reason) && Schema.is(RepositorySetupError)(reason.error)
      ? [reason.error]
      : [],
  );
  if (
    codex === null &&
    failures.length > 0 &&
    failures.length === cause.reasons.length &&
    failures.every((error) => error.recovery === "terminal")
  )
    return {
      type: "agent.failed",
      reason: "repository-setup-failed",
      message: failures[0]!.report.message,
    };
  return {
    type: "agent.recovering",
    reason: "runtime-recovery",
    message:
      "The repair runtime could not confirm completion. Recovering the accepted run without restarting uncertain work.",
  };
};
