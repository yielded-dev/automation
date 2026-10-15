//#region src/workspace-paths.ts
/** Git checkout inside the disposable sandbox workspace. */
const REPO_DIR = "/workspace/repo";
/** Disposable root shared by the checkout and agent state. */
const WORKSPACE_DIR = "/workspace";
/** Native protocol receipts belong to the disposable workspace's current boot. */
const NATIVE_RECEIPTS_DIRECTORY = "/workspace/.sandbox-agent/native";
/**
* Kernel-owned marker used to detect container replacement. Everything under
* {@link WORKSPACE_DIR} is disposable; the Artifacts fork is durable state.
*/
const MATERIALIZATION_MARKER = "/workspace/.sandbox-agent-materialization";
//#endregion
export { MATERIALIZATION_MARKER, NATIVE_RECEIPTS_DIRECTORY, REPO_DIR, WORKSPACE_DIR };
