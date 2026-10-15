import { Effect } from "effect";
import type { SandboxTaskNativeRequest } from "../server/sandbox-contracts/sandbox-task-native";
import { SandboxAgentConfig } from "@yielded/sandbox/cloudflare/Config";
import { Sandboxes } from "@yielded/sandbox/cloudflare/Bindings";
import { sessionResourceNames } from "@yielded/sandbox/cloudflare/resource-names";
import type { EnsureOptions } from "@yielded/sandbox/cloudflare/NativeSandbox";

export const repairSandboxTaskSessionId = (
  taskId: string,
  purpose: SandboxTaskNativeRequest["purpose"],
): string => {
  switch (purpose) {
    case "turn":
      return taskId;
    case "validation":
      return `${taskId}:repair-validation`;
    case "export":
      return `${taskId}:repository-export`;
    case "source":
      return `${taskId}:source-preparation`;
  }
};

/** One namespace and deterministic helper names, including exact-boot recovery. */
export const repairSandboxTaskSandbox = Effect.fn("repairSandboxTaskSandbox")(function* (
  taskId: string,
  purpose: SandboxTaskNativeRequest["purpose"],
  options: EnsureOptions = {},
) {
  const config = yield* SandboxAgentConfig;
  const names = yield* sessionResourceNames(
    repairSandboxTaskSessionId(taskId, purpose),
    config.sandboxNamespace,
  );

  return yield* Sandboxes.get(names.sandboxName, options);
});
