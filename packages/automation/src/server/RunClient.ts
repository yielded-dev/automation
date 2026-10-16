import { Cause, Context, Effect, Layer, Schema } from "effect";
import { RepairErrorReporter } from "./RepairErrorReporter";
import { ConflictError, ServiceUnavailableError } from "../domain/errors";
import type { RepairPublicationRequest } from "../domain/proof";
import type { RunLaunch } from "../domain/RepairExecution";
import type { SessionSandbox } from "@yielded/sandbox/SessionSandbox";

import {
  abandonPublication,
  publishRepair,
  reconcilePublication,
  PublicationError,
} from "./Publication";
import {
  startRepairExecution,
  inspectRepairExecution,
  reportRepairExecution,
  retireRepairExecution,
  reconcileRepairExecution,
} from "./RepairExecution";
import { RepairExecutionContext } from "./RepairExecutionContext";
import { publicationPushMessage } from "./PublicationPushFailure";
import { readRunEvents } from "./RunOutput";

type ExecutionServices = Exclude<
  | Effect.Services<ReturnType<typeof startRepairExecution>>
  | Effect.Services<typeof inspectRepairExecution>
  | Effect.Services<ReturnType<typeof publishRepair>>
  | Effect.Services<ReturnType<typeof retireRepairExecution>>
  | Effect.Services<ReturnType<typeof abandonPublication>>
  | Effect.Services<ReturnType<typeof readRunEvents>>,
  RepairExecutionContext | SessionSandbox
>;
const make = Effect.gen(function* () {
  const context = yield* Effect.context<ExecutionServices>();
  const reporter = yield* RepairErrorReporter;
  const call = <A, E>(
    runId: string,
    effect: Effect.Effect<A, E, RepairExecutionContext | ExecutionServices>,
  ) =>
    effect.pipe(
      Effect.provideService(RepairExecutionContext, runId),
      Effect.provide(context),
      Effect.catchCause((cause): Effect.Effect<never, ConflictError | ServiceUnavailableError> => {
        if (
          cause.reasons.every(
            (reason) => Cause.isFailReason(reason) && Schema.is(ConflictError)(reason.error),
          )
        )
          return Effect.failCause(
            Cause.map(cause, () =>
              ConflictError.make({ message: "Repair execution rejected the operation" }),
            ),
          );

        return reporter.report(cause, "RepairExecutionClient").pipe(
          Effect.andThen(
            Effect.failCause(
              Cause.map(cause, (error) =>
                ServiceUnavailableError.make({
                  message:
                    Schema.is(PublicationError)(error) && error.reason !== undefined
                      ? publicationPushMessage(error.reason, error.diagnostic)
                      : "Repair execution is unavailable",
                }),
              ),
            ),
          ),
        );
      }),
    );

  return {
    start: (input: RunLaunch) => call(input.runId, startRepairExecution(input)),
    inspect: (runId: string) => call(runId, inspectRepairExecution),
    report: (runId: string) => call(runId, reportRepairExecution),
    events: (runId: string, after = 0, limit = 1000) => call(runId, readRunEvents(after, limit)),
    retire: (launch: RunLaunch, reason: "deadline" | "authority_revoked" | "cancelled" | null) =>
      call(launch.runId, retireRepairExecution(launch, reason)),
    reconcilePublication: (runId: string) => call(runId, reconcilePublication),
    abandonPublication: (runId: string, requestId: string) =>
      call(runId, abandonPublication(requestId).pipe(Effect.as({ accepted: true as const }))),
    publish: (runId: string, input: RepairPublicationRequest) =>
      call(runId, reconcileRepairExecution.pipe(Effect.andThen(publishRepair(input)))),
  };
});
/** Application policy and receipts in PG; execution is exclusively the shared SandboxTask service. */
export class RunClient extends Context.Service<RunClient, Effect.Success<typeof make>>()(
  "repair-tool/RunClient",
) {
  static readonly layer = Layer.effect(this, make);
}
