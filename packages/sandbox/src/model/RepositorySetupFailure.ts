import { Cause, Schema } from "effect";
import { RepositorySetupError, type RepositorySetupReport } from "./RepositorySetup";

/** Only a closed native result can confirm a command failure. */
export const repositorySetupCommandFailure = (
  report: RepositorySetupReport,
  step: string,
  result: { readonly exitCode: number | null; readonly timedOut: boolean },
): RepositorySetupError | undefined => {
  if (!result.timedOut && result.exitCode === 0) return undefined;
  const confirmed = result.timedOut || (result.exitCode !== null && result.exitCode > 0);
  const message = result.timedOut
    ? `${step} exceeded the 10-minute limit. Fix the committed recipe and retry setup.`
    : !confirmed
      ? "The setup command has no confirmed exit code."
      : `${step} exited with code ${result.exitCode}. Fix the committed recipe or its inputs and retry setup.`;
  return RepositorySetupError.make({
    message,
    report: { ...report, state: "failed", message },
    recovery: confirmed ? "terminal" : "retryable",
  });
};

/** Preserve every cause; only the bounded report is retained as setup state. */
export const repositorySetupFailureCause = (
  cause: Cause.Cause<unknown>,
  report: RepositorySetupReport,
): Cause.Cause<RepositorySetupError> =>
  Cause.map(cause, (error) =>
    Schema.is(RepositorySetupError)(error)
      ? RepositorySetupError.make({
          message: error.message,
          recovery: error.recovery ?? "retryable",
          cause: error.cause,
          report: {
            ...error.report,
            sourceHead: report.sourceHead,
            steps: error.report.steps.length === 0 ? report.steps : error.report.steps,
          },
        })
      : RepositorySetupError.make({
          message: report.message,
          report,
          recovery: "retryable",
          cause: error,
        }),
  );
