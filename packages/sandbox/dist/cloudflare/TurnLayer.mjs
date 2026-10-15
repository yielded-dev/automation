import { Harness } from "../harness/Harness.mjs";
import { SandboxAgentConfig } from "./Config.mjs";
import { SandboxAgentArtifacts, Sandboxes } from "./Bindings.mjs";
import { WorkspaceBaselineRepository } from "./WorkspaceBaselineRepository.mjs";
import { SessionStateStore } from "./SessionStateStore.mjs";
import { RepositoryInstallAuthorization } from "./RepositoryInstallAuthorization.mjs";
import { WorkspaceServiceCoordinator } from "./WorkspaceProcesses.mjs";
import { ArtifactsCheckout } from "./ArtifactsCheckout.mjs";
import { RepositorySourceCheckout } from "./RepositorySourceCheckout.mjs";
import { CodexHarnessLive } from "../harness/CodexHarness.mjs";
import { SessionRunSink } from "./RunSink.mjs";
import { Layer } from "effect";
import "effect-cf";
//#region src/cloudflare/TurnLayer.ts
/**
* Model-free workspace services. Host capabilities remain exposed; durable
* storage and process coordination are acquired for each owning object.
* Use Layer.fresh for separately configured graphs in the same scope.
*/
const repositoryWorkspaceLayer = Layer.mergeAll(ArtifactsCheckout.layer, SessionStateStore.layer, WorkspaceServiceCoordinator.layer, Layer.effect(SandboxAgentConfig, SandboxAgentConfig), Layer.effect(SandboxAgentArtifacts, SandboxAgentArtifacts), Layer.effect(Sandboxes, Sandboxes), Layer.effect(WorkspaceBaselineRepository, WorkspaceBaselineRepository), Layer.effect(RepositoryInstallAuthorization, RepositoryInstallAuthorization));
/** Generation-bound source support is acquired only by owners that require it. */
const sandboxWorkspaceLayer = RepositorySourceCheckout.layer.pipe(Layer.provideMerge(repositoryWorkspaceLayer));
/** Delegated turns add the host-selected harness to the workspace services. */
const sandboxAgentTurnLayer = Layer.mergeAll(sandboxWorkspaceLayer, Layer.effect(Harness, Harness));
const workspaceBindingsLayer = (options) => Layer.mergeAll(options.configLayer, SandboxAgentArtifacts.layer({ binding: options.bindings.artifacts }), Sandboxes.layer({ binding: options.bindings.sandbox }), options.workspaceBaselineLayer ?? WorkspaceBaselineRepository.noopLayer, options.repositoryInstallAuthorizationLayer ?? RepositoryInstallAuthorization.emptyLayer);
/**
* Composes repository checkout, disposable sandbox and durable session services
* from host bindings. Native workers and delegated harness turns share these
* capabilities without acquiring generation-bound checkout services.
* The suspended boundary shares one isolated graph per factory call; host
* adapters remain outside it so their own Layer sharing is preserved.
*/
const makeRepositoryWorkspaceLayer = (options) => Layer.suspend(() => Layer.fresh(repositoryWorkspaceLayer)).pipe(Layer.provide(workspaceBindingsLayer(options)));
/** Generation-bound source support is acquired only by owners that require it. */
const makeSandboxWorkspaceLayer = (options) => Layer.suspend(() => Layer.fresh(sandboxWorkspaceLayer)).pipe(Layer.provide(workspaceBindingsLayer(options)));
/** Optional delegated turns add a harness to the same model-free workspace capabilities. */
const makeSandboxAgentTurnLayer = (options) => Layer.mergeAll(Layer.suspend(() => Layer.fresh(sandboxAgentTurnLayer)).pipe(Layer.provide(Layer.mergeAll(workspaceBindingsLayer(options), options.harnessLayer ?? CodexHarnessLive))), options.runSinkLayer ?? SessionRunSink.noopLayer);
//#endregion
export { makeRepositoryWorkspaceLayer, makeSandboxAgentTurnLayer, makeSandboxWorkspaceLayer, repositoryWorkspaceLayer, sandboxAgentTurnLayer, sandboxWorkspaceLayer };
