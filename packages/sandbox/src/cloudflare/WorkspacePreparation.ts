import * as Crypto from "effect/Crypto";
import { Cause, Clock, Effect, Option, Redacted, Schedule } from "effect";

import { sanitizeCause } from "../observability/FailureDiagnostic";

import { HarnessTurnError } from "../harness/Harness";
import { SessionSandbox } from "../SessionSandbox";
import {
  acknowledgeNativeOperation,
  quiesceNativeOperation,
  readNativeCommand,
} from "./NativeOperation";
import { NativeWorkspaceIdentity } from "./NativeWorkspaceIdentity";
import { SessionStateStore, type StoredWorkspacePreparation } from "./SessionStateStore";
import {
  RepositoryInstallAuthorization,
  type RepositoryInstallSource,
} from "./RepositoryInstallAuthorization";

export interface WorkspacePreparationRequest {
  readonly command: StoredWorkspacePreparation["operation"]["command"];
  readonly cwd: string;
  readonly env?: Readonly<Record<string, string>>;
  readonly deadlineAt: number;
  readonly maxOutputBytes: number;
  readonly repositoryToken?: StoredWorkspacePreparation["repositoryToken"];
}

const prepareWorkspaceOperation = Effect.fnUntraced(function* (
  scope: string,
  request?: WorkspacePreparationRequest,
) {
  const store = yield* SessionStateStore;
  const sandbox = yield* SessionSandbox;
  const stored = store.workspacePreparation(scope);
  let receipt = yield* stored.get;

  if (receipt === null) {
    if (request === undefined)
      return yield* HarnessTurnError.make({
        reason: "turnFailed",
        message: "Workspace preparation has no retained admission",
      });
    const workspaceIdentity = yield* NativeWorkspaceIdentity;
    const { repositoryToken, ...configuration } = request;

    receipt = {
      operation: {
        ...configuration,
        id: yield* sandbox.allocateProcess(),
        token: yield* (yield* Crypto.Crypto).randomUUIDv4().pipe(Effect.orDie),
        port: 4502,
        mode: "command",
        ...(workspaceIdentity === null ? {} : { workspaceIdentity }),
      },
      ...(repositoryToken === undefined ? {} : { repositoryToken }),
      result: null,
    };
    yield* stored.put(receipt);
  }
  return receipt;
});

const observeWorkspacePreparation = Effect.fnUntraced(function* (
  scope: string,
  receipt: StoredWorkspacePreparation,
) {
  const store = yield* SessionStateStore;
  const sandbox = yield* SessionSandbox;
  const stored = store.workspacePreparation(scope);
  const operation = receipt.operation;
  const listener = {
    id: operation.id,
    token: operation.token,
    port: operation.port,
    deadlineAt: operation.deadlineAt,
  };

  if (receipt.result !== null) {
    yield* store.preparationListener.put(listener);

    return receipt.result;
  }
  const retryable = (error: { readonly _tag: string; readonly reason?: string }) =>
    error._tag === "SandboxOperationError" ||
    (error._tag === "HarnessTurnError" && error.reason === "connectionLost");
  const retryObservation = <A, E extends { readonly _tag: string; readonly reason?: string }, R>(
    observation: Effect.Effect<A, E, R>,
  ) =>
    observation.pipe(
      Effect.tapError((error) =>
        retryable(error)
          ? Effect.logWarning(
              "Workspace preparation observation disconnected",
              sanitizeCause(Cause.fail(error), { operation: "WorkspacePreparation.observe" }),
            )
          : Effect.void,
      ),
      Effect.retry({
        while: (error) =>
          Effect.map(
            Clock.currentTimeMillis,
            (now) => now < operation.deadlineAt + 1_000 && retryable(error),
          ),
        schedule: Schedule.spaced("250 millis"),
      }),
    );
  const read = retryObservation(readNativeCommand(operation));

  while (true) {
    const output = yield* read;

    if (output.state === "completed") {
      const result = { ...output, state: "completed" as const };

      yield* stored.put({ ...receipt, result });
      yield* store.preparationListener.put(listener);

      return result;
    }
    if (output.state !== "running")
      return yield* HarnessTurnError.make({
        reason: "turnFailed",
        message: "Workspace preparation lost its native owner; it was not replayed",
      });
    if ((yield* Clock.currentTimeMillis) >= operation.deadlineAt + 1_000)
      return yield* HarnessTurnError.make({
        reason: "deadlineExceeded",
        message: "Workspace preparation did not close before its deadline",
      });
    yield* retryObservation(
      Effect.gen(function* () {
        const process = yield* sandbox.getProcess(operation.id);

        if (Option.isSome(process))
          yield* process.value
            .waitForExit({
              timeout: Math.max(1, operation.deadlineAt + 1_000 - (yield* Clock.currentTimeMillis)),
            })
            .pipe(
              Effect.catchIf(
                (error) => error.code === "ETIMEDOUT",
                () => Effect.void,
              ),
            );
        else yield* Effect.sleep("250 millis");
      }),
    );
    // The native long poll owns the wait. Allow its gateway watcher to retain
    // completion before reading that receipt, without polling the DO while running.
    yield* Effect.sleep("100 millis");
  }
});

/** Persist completion before the owning workflow releases authorization and acknowledges it. */
export const runWorkspacePreparation = Effect.fn("WorkspacePreparation.run")(function* (
  scope: string,
  request?: WorkspacePreparationRequest,
) {
  return yield* observeWorkspacePreparation(
    scope,
    yield* prepareWorkspaceOperation(scope, request),
  );
});

/** Dependency credentials exist only while observing the retained install operation. */
export const runRepositoryInstallation = Effect.fn("WorkspacePreparation.install")(function* (
  scope: string,
  request: WorkspacePreparationRequest,
  source: RepositoryInstallSource,
) {
  const receipt = yield* prepareWorkspaceOperation(scope, request);
  const operation = receipt.operation;
  if (receipt.result === null && operation.deadlineAt > (yield* Clock.currentTimeMillis)) {
    const credentials = yield* (yield* RepositoryInstallAuthorization).authorize({
      ...source,
      deadlineAt: operation.deadlineAt,
    });
    const sandbox = yield* SessionSandbox;
    for (const credential of credentials) {
      const identity = yield* sandbox.ensure();
      const grant = yield* Effect.acquireRelease(
        Effect.succeed({
          remote: credential.remote,
          instanceId: identity.instanceId,
          grantId: operation.token,
        }),
        (grant) => sandbox.clearRepositoryAuthorization(grant).pipe(Effect.catchCause(Effect.die)),
      );
      // Refresh only this retained operation's prior grant after a lost acknowledgement.
      yield* sandbox.clearRepositoryAuthorization(grant);
      // Register cleanup before applying authority, including a lost application acknowledgement.
      yield* sandbox.setRepositoryAuthorization(
        credential.remote,
        Redacted.value(credential.authorization),
        operation.deadlineAt,
        operation.token,
      );
    }
  }
  return yield* observeWorkspacePreparation(scope, receipt);
}, Effect.scoped);

/** Called only after the owning workflow releases any authority retained in the receipt. */
export const acknowledgeWorkspacePreparation = Effect.fn("WorkspacePreparation.acknowledge")(
  function* (scope: string, options?: { readonly retainReceipt: boolean }) {
    const store = yield* SessionStateStore;
    const stored = store.workspacePreparation(scope);
    const receipt = yield* stored.get;

    if (receipt === null || receipt.result === null)
      return yield* HarnessTurnError.make({
        reason: "turnFailed",
        message: "Workspace preparation has no retained completion",
      });
    const operation = receipt.operation;

    yield* acknowledgeNativeOperation(operation);
    if (options?.retainReceipt !== true) yield* stored.delete;
  },
);

/** Preparation steps share one listener until the whole filesystem is ready. */
export const quiesceWorkspacePreparation = Effect.gen(function* () {
  const store = yield* SessionStateStore;
  const listener = yield* store.preparationListener.get;

  if (listener === null) return;
  yield* quiesceNativeOperation(listener);
  yield* store.preparationListener.delete;
});
