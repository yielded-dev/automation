import { Effect } from "effect";
import { ConflictError } from "../domain/errors";
import type { SandboxTaskMachineCreate } from "../server/sandbox-contracts/sandbox-task-machine";
import { SandboxAgentArtifacts } from "@yielded/sandbox/cloudflare/Bindings";
import { SessionStateStore } from "@yielded/sandbox/cloudflare/SessionStateStore";
import { RepairSandboxTaskStore } from "./RepairSandboxTaskStore";
import { TurnId } from "../server/sandbox-contracts/ids";

/** A seed grants read access; only the retained private checkpoint grants helper access. */
export const readSandboxTaskPrivateRepository = Effect.fn("RepairSandboxTask.privateRepository")(
  function* (admission: SandboxTaskMachineCreate) {
    if (admission.source._tag === "AttachedRepository") return admission.source;
    if (admission.source._tag !== "SeedRepository")
      return yield* ConflictError.make({
        message: "This execution has no private repository authority",
      });
    const state = yield* (yield* RepairSandboxTaskStore).required;
    const checkpoint = state.savedCheckpoint;
    if (
      state.request.id !== admission.id ||
      state.request.commandId !== admission.commandId ||
      checkpoint === null ||
      checkpoint.baseHead !== admission.source.commitSha ||
      checkpoint.repositoryName === admission.source.repositoryName
    )
      return yield* ConflictError.make({
        message: "The execution has no accepted private repository checkpoint",
      });
    return checkpoint;
  },
);

/** Retain a generation-checked candidate only after the pipeline confirms its Git persistence. */
export const retainRepairSandboxCheckpoint = Effect.fn("RepairSandboxTask.retainCheckpoint")(
  function* (head: string, turnId: string) {
    const records = yield* RepairSandboxTaskStore;
    const state = yield* records.required;
    const source = state.request.source;
    if (source._tag === "None") return;
    const store = yield* SessionStateStore;
    const intent = yield* store.repositoryFork.get;
    const repositoryName =
      source._tag === "AttachedRepository" ? source.repositoryName : yield* store.forkName.get;
    const repositoryId =
      source._tag === "AttachedRepository" ? source.artifactsRepositoryId : intent?.repositoryId;
    const pipeline = yield* store.pipelineTurn.get;
    if (
      repositoryName === null ||
      repositoryId === null ||
      repositoryId === undefined ||
      pipeline?.id !== turnId ||
      pipeline.persisted?.head !== head ||
      (yield* store.verifiedHead.get) !== head ||
      (source._tag === "SeedRepository" &&
        (intent?.initialized !== true ||
          intent.forkName !== repositoryName ||
          repositoryName === source.repositoryName))
    )
      return yield* ConflictError.make({
        message: "The saved candidate has no matching private repository receipt",
      });
    const repository = yield* (yield* SandboxAgentArtifacts).get(repositoryName);
    const info = yield* repository.info;
    if (
      info.name !== repositoryName ||
      info.id !== repositoryId ||
      (yield* repository.readCommit(head)).hash !== head ||
      (yield* repository.info).id !== info.id
    )
      return yield* ConflictError.make({
        message: "The saved candidate repository generation changed",
      });
    const sequence = yield* records.append(turnId, "runtime", "sandbox.checkpoint.saved", { head });
    yield* records.update((current) => ({
      ...current,
      savedCheckpoint: {
        repositoryName,
        artifactsRepositoryId: repositoryId,
        commitSha: head,
        baseHead: source.commitSha,
        turnId: TurnId.make(turnId),
        sequence,
      },
    }));
  },
);
