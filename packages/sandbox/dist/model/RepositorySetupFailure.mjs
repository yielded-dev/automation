import { RepositorySetupError } from "./RepositorySetup.mjs";
import { Cause, Schema } from "effect";
//#region src/model/RepositorySetupFailure.ts
/** Only a closed native result can confirm a command failure. */
const repositorySetupCommandFailure = (report, step, result) => {
	if (!result.timedOut && result.exitCode === 0) return void 0;
	const confirmed = result.timedOut || result.exitCode !== null && result.exitCode > 0;
	const message = result.timedOut ? `${step} exceeded the 10-minute limit. Fix the committed recipe and retry setup.` : !confirmed ? "The setup command has no confirmed exit code." : `${step} exited with code ${result.exitCode}. Fix the committed recipe or its inputs and retry setup.`;
	return RepositorySetupError.make({
		message,
		report: {
			...report,
			state: "failed",
			message
		},
		recovery: confirmed ? "terminal" : "retryable"
	});
};
/** Preserve every cause; only the bounded report is retained as setup state. */
const repositorySetupFailureCause = (cause, report) => Cause.map(cause, (error) => Schema.is(RepositorySetupError)(error) ? RepositorySetupError.make({
	message: error.message,
	recovery: error.recovery ?? "retryable",
	cause: error.cause,
	report: {
		...error.report,
		sourceHead: report.sourceHead,
		steps: error.report.steps.length === 0 ? report.steps : error.report.steps
	}
}) : RepositorySetupError.make({
	message: report.message,
	report,
	recovery: "retryable",
	cause: error
}));
//#endregion
export { repositorySetupCommandFailure, repositorySetupFailureCause };
