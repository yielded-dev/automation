import { Effect, Layer } from "effect";
import type { WorkerEnvironment } from "effect-cf";
import { reportSanitizedCause, withErrorReportingScope } from "../observability/RuntimeDiagnostics";
import {
  executeRepositoryPreparation,
  RepositoryPreparationGate,
  RepositoryPreparationObject,
  RepositoryPreparationSources,
  runRepositoryPreparationAlarms,
  scheduleRepositoryPreparation,
  type RepositoryPreparationExecution,
} from "./RepositoryPreparation";
import { makeRepositoryWorkspaceLayer, type SandboxWorkspaceOptions } from "./TurnLayer";

/** Composition shared by a dedicated workspace owner and existing repair-task namespaces. */
export const repositoryPreparationHandlers = <E>(
  sourcesLayer: Layer.Layer<RepositoryPreparationSources, E, WorkerEnvironment>,
) => ({
  rpc: {
    requestRepositoryPreparation: (repository: string) =>
      scheduleRepositoryPreparation(repository).pipe(
        Effect.provide(sourcesLayer),
        Effect.scoped,
        withErrorReportingScope,
      ),
    executeRepositoryPreparation: (input: typeof RepositoryPreparationExecution.Type) =>
      executeRepositoryPreparation(input).pipe(
        Effect.provide(sourcesLayer),
        Effect.scoped,
        Effect.catchCause((cause) =>
          reportSanitizedCause(cause, { operation: "RepositoryPreparation.execute" }).pipe(
            Effect.as({ status: "unavailable" as const }),
          ),
        ),
        withErrorReportingScope,
      ),
  },
  alarms: runRepositoryPreparationAlarms.pipe(
    Effect.provide(sourcesLayer),
    Effect.scoped,
    withErrorReportingScope,
  ),
});

export interface RepositoryPreparationOptions<
  ConfigFailure,
  HostFailure = never,
> extends SandboxWorkspaceOptions<ConfigFailure> {
  readonly coordinatorBinding: string;
  readonly sourcesLayer: Layer.Layer<RepositoryPreparationSources, HostFailure, WorkerEnvironment>;
  readonly diagnosticsLayer: Layer.Layer<never, HostFailure, WorkerEnvironment>;
}

/** Model-free preparation on host-owned resources; no repair records or coding provider required. */
export const makeRepositoryPreparationDurableObject = <ConfigFailure, HostFailure = never>(
  options: RepositoryPreparationOptions<ConfigFailure, HostFailure>,
) => {
  const runtime = Layer.mergeAll(
    makeRepositoryWorkspaceLayer(options),
    RepositoryPreparationGate.layer,
    RepositoryPreparationObject.layer({ binding: options.coordinatorBinding }),
  ).pipe(Layer.provideMerge(options.diagnosticsLayer));
  const handlers = repositoryPreparationHandlers(options.sourcesLayer);
  return RepositoryPreparationObject.make(runtime, handlers);
};
