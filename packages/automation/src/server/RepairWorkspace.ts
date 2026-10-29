import * as SecureRandom from "effect/crypto/SecureRandom";
import { Effect, Schema } from "effect";

import type { SandboxTaskNativeRequest } from "./sandbox-contracts/sandbox-task-native";
import { ConflictError } from "../domain/errors";
import { repairIdentity } from "../domain/launch";
import { RepairSandboxTaskClient } from "./RepairSandboxTaskClient";
import type { EnsureOptions } from "@yielded/sandbox/cloudflare/NativeSandbox";

import { remoteRepairSandbox } from "./RemoteSandbox";
import { readRunState } from "./RepairExecutionAuthority";
import { RepairExecutionContext } from "./RepairExecutionContext";

export const repairExecutionSnapshot = Effect.gen(function* () {
  const runId = yield* RepairExecutionContext;
  return yield* (yield* RepairSandboxTaskClient).snapshot(runId);
});
export const repairHelperSandbox = (
  purpose: SandboxTaskNativeRequest["purpose"],
  options: EnsureOptions = {},
) =>
  Effect.gen(function* () {
    const state = yield* readRunState;

    if (state === undefined)
      return yield* ConflictError.make({ message: "The repair execution is unavailable" });

    return yield* remoteRepairSandbox(state.runId, purpose, state.origin.deadlineAtMs, options);
  });
const repairNativeRequest = (
  purpose: SandboxTaskNativeRequest["purpose"],
  action: SandboxTaskNativeRequest["action"],
) =>
  Effect.gen(function* () {
    return yield* (yield* RepairSandboxTaskClient).native(
      yield* RepairExecutionContext,
      {
        purpose,
        action,
        operationId: yield* (yield* SecureRandom.SecureRandom).uuidV4(),
      },
      Schema.Json,
    );
  });

export const ensureRepairCommit = (head: string) =>
  repairNativeRequest("export", { _tag: "EnsureCommit", head }).pipe(Effect.asVoid);
export const prepareRepairHelperRepository = Effect.fn("RepairExecution.prepareHelperRepository")(
  function* (head: string, scope: string, expectedInstanceId: string) {
    const runId = yield* RepairExecutionContext;

    // Preparation can outlive a controller poll. Recover its native receipt
    // instead of cloning again when the original response was interrupted.
    yield* (yield* RepairSandboxTaskClient).native(
      runId,
      {
        purpose: "export",
        expectedInstanceId,
        operationId: repairIdentity(
          "helper-repository",
          runId,
          "export",
          head,
          scope,
          expectedInstanceId,
        ),
        action: { _tag: "PrepareRepository", head, scope, resetReceipt: false },
      },
      Schema.Json,
    );
  },
);
