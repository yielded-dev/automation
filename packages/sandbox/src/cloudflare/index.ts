/**
 * Cloudflare host integration for the sandbox-agent kernel. Applications keep
 * authentication and routing at their edge, then export the concrete container
 * and session classes built by these factories.
 *
 * @module
 */
export {
  DirectoryBackupGateway,
  makeSandboxAgentOutbound,
  makeSandboxAgentContainer,
  SANDBOX_PACKAGE_VERSION,
  HOST_OPERATION_HOST,
  OPENAI_HOST,
  type SandboxAgentContainerOptions,
} from "./Container";
export { CodexHarnessLive, cancelCodexTurn } from "../harness/CodexHarness";
export {
  Harness,
  HarnessTurnError,
  type HarnessSteeringOutcome,
  type HarnessTurnSteer,
  type HarnessTurnRequest,
  type HarnessTurnResult,
} from "../harness/Harness";
export {
  defaultSandboxAgentDeadlinePolicy,
  SandboxAgentConfig,
  sandboxAgentDeadlinePolicy,
  type SandboxAgentConfigShape,
  type SandboxAgentDeadlinePolicy,
  type SandboxMcpConfiguration,
} from "./Config";
// Deterministic per-session resource names. Hosts hold the same Artifacts and
// Sandbox bindings they hand the kernel, so these helpers give them side-band
// access to any session's fork and container: pre-created seed forks that
// `ensureFork` adopts, preview proxying, scoped read tokens. Writes still go
// through the session Durable Object, always.
export {
  forkNameFor,
  ResourceHashError,
  resourceHash,
  sandboxNameFor,
  sessionResourceNames,
} from "./resource-names";
// The turn-scoped context a deploy-time harness Layer (or any turn-scoped
// operation) yields instead of receiving parameters.
export { SessionSandbox } from "../SessionSandbox";
export { MATERIALIZATION_MARKER, REPO_DIR } from "../workspace-paths";
export {
  ensureWorkspaceServices,
  TurnWorkspaceServices,
  WorkspaceServiceCoordinator,
} from "./WorkspaceProcesses";
export { Sandboxes, SandboxAgentArtifacts, sandboxFor } from "./Bindings";
export * from "./NativeSandbox";
export * from "./WorkspaceBaselineRepository";
export { refreshWorkspaceBaselines } from "./WorkspaceBaselineRefresh";
export {
  isTerminalWorkspaceBaselinePreparationCause,
  prepareWorkspaceBaseline,
  WorkspaceBaselinePreparationError,
} from "./WorkspaceBaselinePreparation";
export { WorkspaceBaselinePreparationResult } from "./WorkspaceBaselinePreparationState";
export {
  RepositoryPreparationError,
  RepositoryPreparationSources,
  RepositoryPreparationSource,
  RepositoryPreparationRequest,
  RepositoryPreparationExecution,
  RepositoryPreparationReply,
  RepositoryPreparationObject,
  repositoryPreparationName,
  requestRepositoryPreparation,
} from "./RepositoryPreparation";
export {
  makeRepositoryPreparationDurableObject,
  type RepositoryPreparationOptions,
} from "./RepositoryPreparationRuntime";
export {
  artifactsForkPolicyFromDeadlines,
  artifactsMainHead,
  ArtifactsOperationTimeoutError,
  awaitArtifactsForkReady,
  ensureArtifactsFork,
  ForkNotReadyError,
  isForkProvisioning,
  requestArtifactsFork,
  type ArtifactsForkInput,
  type ArtifactsForkPolicy,
  type EnsureArtifactsForkResult,
} from "./ArtifactsFork";
export {
  RepositorySourceCheckout,
  RepositorySourceRestoreHeadMismatchError,
  type RepositorySourceCheckoutService,
} from "./RepositorySourceCheckout";
export {
  ArtifactsCheckout,
  ArtifactsCheckoutDirectory,
  ArtifactsRepositoryGenerationMismatchError,
  NonFastForwardPushError,
  type ArtifactsRepositoryGeneration,
  type ArtifactsRepositorySource,
  type ArtifactsCheckoutService,
  type PersistResult,
} from "./ArtifactsCheckout";
export {
  MAX_RUN_ARCHIVE_BYTES,
  RunArchiveLimitError,
  RunSinkError,
  SessionRunSink,
} from "./RunSink";
export {
  repositoryWorkspaceLayer,
  sandboxWorkspaceLayer,
  sandboxAgentTurnLayer,
  makeRepositoryWorkspaceLayer,
  makeSandboxWorkspaceLayer,
  makeSandboxAgentTurnLayer,
  type SandboxAgentBindingNames,
  type SandboxWorkspaceOptions,
  type SandboxAgentSessionOptions,
} from "./TurnLayer";
// The in-process turn: an Effect a host runs inside its own Durable Object,
// providing the turn layer above and the host's output sink.
export { AgentEventSink, AgentEventSinkError } from "./AgentEventSink";
export {
  RepositoryPersistenceCheckpoint,
  RepositoryPersistenceCheckpointError,
} from "./RepositoryPersistenceCheckpoint";
export {
  describeFailure,
  runTurn,
  runMessagePipeline,
  TurnSteering,
  GenerationSourceCheckpoint,
  TurnLifecycleCapture,
  type TurnRequest,
  type LifecycleCapture,
  type MessageContext,
} from "./MessagePipeline";
export { SessionStateStore, type SessionStateStoreError } from "./SessionStateStore";
export { inMeasure, makeRunSpanCapture } from "../tracing/RunSpanCapture";
export {
  readRepositorySetup,
  executeRepositorySetup,
  prepareRepositorySetup,
  resolveRepositorySetupEnvironment,
  repositorySetupServiceArgv,
  repositorySetupFingerprint,
  type ExecuteRepositorySetupInput,
} from "./RepositorySetup";

export {
  ensureNativeOperation,
  readNativeCommand,
  cancelNativeOperation,
  acknowledgeNativeOperation,
  type NativeOperation,
  type NativeOperationIdentity,
  type NativeCommandStatus,
} from "./NativeOperation";
export { NativeWorkspaceIdentity } from "./NativeWorkspaceIdentity";
export { withSandboxLease } from "./SandboxLease";

export { OutboundAuthority, OutboundAuthorityError } from "./OutboundAuthority";
export {
  RepositoryInstallAuthorization,
  RepositoryInstallAuthorizationError,
  type RepositoryInstallSource,
} from "./RepositoryInstallAuthorization";
