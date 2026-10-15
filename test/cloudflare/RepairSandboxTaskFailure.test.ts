import { Cause, Schema } from "effect";
import { expect, it } from "vite-plus/test";
import { repairSandboxTaskFailure } from "../../packages/automation/src/cloudflare/RepairSandboxTaskFailure";
import { AgentEvent } from "../../packages/sandbox/src/Contract";
import { RepositorySetupError } from "../../packages/sandbox/src/model/RepositorySetup";

const confirmed = RepositorySetupError.make({
  message: "start-1 exited with code 1. Fix the committed recipe or its inputs and retry setup.",
  recovery: "terminal",
  cause: new Error("private provider payload"),
  report: {
    state: "failed",
    sourceHead: "a".repeat(40),
    recipePath: ".setup.json",
    message: "start-1 exited with code 1. Fix the committed recipe or its inputs and retry setup.",
    steps: [{ name: "start-1", exitCode: 1, output: "private stdout" }],
  },
});

it("emits the safe confirmed failure before Codex, never private causes/output", () => {
  const event = repairSandboxTaskFailure(Cause.fail(confirmed), null)!;
  expect(event).toEqual({
    type: "agent.failed",
    reason: "repository-setup-failed",
    message: confirmed.report.message,
  });
  const wire = Schema.encodeSync(Schema.fromJsonString(AgentEvent))(event);
  expect(wire).not.toContain("private");
});

it("keeps lost-workspace and old unclassified setup errors recoverable", () => {
  for (const error of [
    { _tag: "HarnessTurnError", reason: "turnFailed", message: "private lost workspace details" },
    RepositorySetupError.make({ message: confirmed.message, report: confirmed.report }),
    RepositorySetupError.make({
      message: confirmed.message,
      report: confirmed.report,
      cause: confirmed.cause,
      recovery: "retryable",
    }),
  ]) {
    const event = repairSandboxTaskFailure(Cause.fail(error), null)!;
    expect(event.type).toBe("agent.recovering");
    expect(Schema.encodeSync(Schema.fromJsonString(AgentEvent))(event)).not.toContain("private");
  }
});

it("cannot terminalize mixed causes or an existing Codex receipt as a setup failure", () => {
  for (const reason of [
    Cause.makeDieReason("defect"),
    Cause.makeInterruptReason(),
    Cause.makeFailReason("unknown"),
  ]) {
    expect(
      repairSandboxTaskFailure(Cause.fromReasons([Cause.makeFailReason(confirmed), reason]), null)
        ?.type,
    ).toBe("agent.recovering");
  }
  expect(
    repairSandboxTaskFailure(Cause.fail(confirmed), { startDispatched: false, result: null })?.type,
  ).toBe("agent.recovering");
  expect(repairSandboxTaskFailure(Cause.interrupt(), null)).toBeUndefined();
});

it("retains the existing native Codex terminal failure rule", () => {
  expect(
    repairSandboxTaskFailure(Cause.fail("unknown"), {
      startDispatched: true,
      result: { terminalEventType: "turn.failed" },
    })?.type,
  ).toBe("agent.failed");
  expect(
    repairSandboxTaskFailure(Cause.fail(confirmed), {
      startDispatched: true,
      result: { terminalEventType: "turn.completed" },
    })?.type,
  ).toBe("agent.recovering");
});
