import { Cause, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";
import type { WorkspaceBaselinePreparationError } from "../../src/cloudflare/WorkspaceBaselinePreparation";
import { RepositorySetupError, RepositorySetupReport } from "../../src/model/RepositorySetup";
import {
  repositorySetupCommandFailure,
  repositorySetupFailureCause,
} from "../../src/model/RepositorySetupFailure";
import {
  causeFromFailureDiagnostic,
  FailureDiagnostic,
  failureDiagnostic,
} from "../../src/observability/FailureDiagnostic";

const report = RepositorySetupReport.make({
  state: "failed",
  recipePath: ".setup.json",
  sourceHead: "a".repeat(40),
  steps: [{ name: "start-1", exitCode: null, output: "private command output" }],
  message: "The setup command outcome could not be confirmed.",
});

describe("retained setup outcomes", () => {
  it("retains workspace preparation classifications and nested causes without private content", () => {
    const cause = Cause.fromReasons<
      Pick<WorkspaceBaselinePreparationError, "_tag" | "reason" | "cause">
    >([
      Cause.makeFailReason({ _tag: "WorkspaceBaselinePreparationError", reason: "workspace-lost" }),
      Cause.makeFailReason({
        _tag: "WorkspaceBaselinePreparationError",
        reason: "setup-failed",
        cause: Cause.fail(
          RepositorySetupError.make({
            report,
            message: "private setup detail",
            cause: new TypeError("private provider payload"),
          }),
        ),
      }),
      Cause.makeFailReason({
        _tag: "WorkspaceBaselinePreparationError",
        reason: "source-changed",
        cause: Cause.fail({
          _tag: "RepositoryForkIntentError",
          reason: "source-pin",
          message: "private source detail",
        }),
      }),
      Cause.makeDieReason(new RangeError("private concurrent defect")),
      Cause.makeInterruptReason(),
    ]);
    const options = { operation: "RepositoryPreparation.execute" };
    const diagnostic = failureDiagnostic(cause, options);
    const codec = Schema.fromJsonString(FailureDiagnostic);
    const wire = Schema.encodeSync(codec)(diagnostic);
    expect(wire).not.toContain("private");
    expect(Schema.decodeSync(codec)(wire)).toEqual({
      ...options,
      failures: [
        { kind: "defect", type: "RangeError", causes: [] },
        {
          kind: "failure",
          type: "WorkspaceBaselinePreparationError",
          reason: "workspace-lost",
          causes: [],
        },
        {
          kind: "failure",
          type: "WorkspaceBaselinePreparationError",
          reason: "setup-failed",
          causes: [{ type: "UnknownError" }, { type: "TypeError" }],
        },
        {
          kind: "failure",
          type: "WorkspaceBaselinePreparationError",
          reason: "source-changed",
          causes: [{ type: "RepositoryForkIntentError", reason: "source-pin" }],
        },
      ],
      interruptions: 1,
    });
    expect(failureDiagnostic(causeFromFailureDiagnostic(diagnostic), options)).toMatchObject(
      diagnostic,
    );
  });

  it.each([
    [{ exitCode: 1, timedOut: false }, "terminal"],
    [{ exitCode: null, timedOut: true }, "terminal"],
    [{ exitCode: null, timedOut: false }, "retryable"],
    [{ exitCode: -1, timedOut: false }, "retryable"],
  ] as const)("classifies the closed result %j", (result, recovery) => {
    expect(repositorySetupCommandFailure(report, "start-1", result)?.recovery).toBe(recovery);
  });

  it("does not fail a confirmed success", () => {
    expect(
      repositorySetupCommandFailure(report, "install", { exitCode: 0, timedOut: false }),
    ).toBeUndefined();
  });

  it("retains the original typed cause without copying it into the report", () => {
    const original = {
      _tag: "HarnessTurnError",
      reason: "turnFailed",
      message: "private provider payload",
    };
    const mapped = repositorySetupFailureCause(Cause.fail(original), report);
    const reason = mapped.reasons[0]!;
    expect(Cause.isFailReason(reason)).toBe(true);
    if (!Cause.isFailReason(reason)) throw new Error("Expected a typed failure");
    expect(reason.error.cause).toBe(original);
    expect(reason.error.recovery).toBe("retryable");
    expect(reason.error.report).toEqual(report);
    expect(
      Schema.encodeSync(Schema.fromJsonString(RepositorySetupReport))(reason.error.report),
    ).not.toContain(original.message);
  });

  it("preserves confirmation, defects and interruptions through report projection", () => {
    const confirmed = repositorySetupCommandFailure(report, "start-1", {
      exitCode: 1,
      timedOut: false,
    })!;
    const defect = new Error("private defect");
    const cause = Cause.fromReasons([
      Cause.makeFailReason(confirmed),
      Cause.makeDieReason(defect),
      Cause.makeInterruptReason(),
    ]);
    const mapped = repositorySetupFailureCause(cause, report);
    expect(mapped.reasons).toHaveLength(3);
    const failure = mapped.reasons[0]!;
    expect(
      Cause.isFailReason(failure) &&
        Schema.is(RepositorySetupError)(failure.error) &&
        failure.error.recovery,
    ).toBe("terminal");
    expect(mapped.reasons[1]).toEqual(cause.reasons[1]);
    expect(mapped.reasons[2]).toEqual(cause.reasons[2]);
  });

  it("does not lose non-enumerable Error message/cause when enriching a report", () => {
    const original = new Error("private original cause");
    const error = RepositorySetupError.make({
      message: "Setup could not be observed",
      report,
      recovery: "retryable",
      cause: original,
    });
    const mapped = repositorySetupFailureCause(Cause.fail(error), report).reasons[0]!;
    if (!Cause.isFailReason(mapped)) throw new Error("Expected a typed failure");
    expect(mapped.error.message).toBe(error.message);
    expect(mapped.error.cause).toBe(original);
  });
});
