import { Layer } from "effect";
import { WorkerEnvironment } from "effect-cf";

import { CodexHarnessLive } from "../harness/CodexHarness";
import { Harness } from "../harness/Harness";
import { ArtifactsCheckout } from "./ArtifactsCheckout";
import { SandboxAgentArtifacts, Sandboxes, webCryptoLayer } from "./Bindings";
import { SandboxAgentConfig } from "./Config";
import { SessionRunSink } from "./RunSink";
import { SessionStateStore } from "./SessionStateStore";
import { RepositorySourceCheckout } from "./RepositorySourceCheckout";
import { WorkspaceBaselineRepository } from "./WorkspaceBaselineRepository";
import { WorkspaceServiceCoordinator } from "./WorkspaceProcesses";
import { RepositoryInstallAuthorization } from "./RepositoryInstallAuthorization";

/** Host-selected Wrangler binding names mapped onto kernel capabilities. */
export interface SandboxAgentBindingNames {
  /** Cloudflare Artifacts binding containing the seed and session forks. */
  readonly artifacts: string;
  /** Native container namespace; image and size are selected at start. */
  readonly sandbox: string;
}

/** Host adapters for the compatibility workspace factories. */
export interface SandboxWorkspaceOptions<ConfigFailure> {
  /** Maps host binding names to the kernel's workspace capabilities. */
  readonly bindings: SandboxAgentBindingNames;
  /** Layer that decodes and provides host policy at the Worker boundary. */
  readonly configLayer: Layer.Layer<SandboxAgentConfig, ConfigFailure, WorkerEnvironment>;
  readonly workspaceBaselineLayer?: Layer.Layer<
    WorkspaceBaselineRepository,
    never,
    WorkerEnvironment
  >;
  readonly repositoryInstallAuthorizationLayer?: Layer.Layer<
    RepositoryInstallAuthorization,
    ConfigFailure,
    WorkerEnvironment
  >;
}

/** Host capabilities used to compose one in-process kernel turn. */
export interface SandboxAgentSessionOptions<
  ConfigFailure,
  RunSinkFailure = never,
  HarnessFailure = never,
> extends SandboxWorkspaceOptions<ConfigFailure> {
  /** Optional retention adapter; omission keeps complete runs ephemeral. */
  readonly runSinkLayer?: Layer.Layer<SessionRunSink, RunSinkFailure, WorkerEnvironment>;
  /** Deploy-time harness choice; defaults to the Codex CLI harness. The
   * host's container image must carry the chosen harness's binary. */
  readonly harnessLayer?: Layer.Layer<Harness, HarnessFailure, WorkerEnvironment>;
}

/**
 * Model-free workspace services. Host capabilities remain exposed; durable
 * storage and process coordination are acquired for each owning object.
 * Use Layer.fresh for separately configured graphs in the same scope.
 */
export const repositoryWorkspaceLayer = Layer.mergeAll(
  ArtifactsCheckout.layer,
  SessionStateStore.layer,
  WorkspaceServiceCoordinator.layer,
  Layer.effect(SandboxAgentConfig, SandboxAgentConfig),
  Layer.effect(SandboxAgentArtifacts, SandboxAgentArtifacts),
  Layer.effect(Sandboxes, Sandboxes),
  Layer.effect(WorkspaceBaselineRepository, WorkspaceBaselineRepository),
  Layer.effect(RepositoryInstallAuthorization, RepositoryInstallAuthorization),
).pipe(Layer.provideMerge(webCryptoLayer));

/** Generation-bound source support is acquired only by owners that require it. */
export const sandboxWorkspaceLayer = RepositorySourceCheckout.layer.pipe(
  Layer.provideMerge(repositoryWorkspaceLayer),
);

/** Delegated turns add the host-selected harness to the workspace services. */
export const sandboxAgentTurnLayer = Layer.mergeAll(
  sandboxWorkspaceLayer,
  Layer.effect(Harness, Harness),
);

const workspaceBindingsLayer = <ConfigFailure>(options: SandboxWorkspaceOptions<ConfigFailure>) =>
  Layer.mergeAll(
    webCryptoLayer,
    options.configLayer,
    SandboxAgentArtifacts.layer({ binding: options.bindings.artifacts }),
    Sandboxes.layer({ binding: options.bindings.sandbox }),
    options.workspaceBaselineLayer ?? WorkspaceBaselineRepository.noopLayer,
    options.repositoryInstallAuthorizationLayer ?? RepositoryInstallAuthorization.emptyLayer,
  );

/**
 * Composes repository checkout, disposable sandbox and durable session services
 * from host bindings. Native workers and delegated harness turns share these
 * capabilities without acquiring generation-bound checkout services.
 * The suspended boundary shares one isolated graph per factory call; host
 * adapters remain outside it so their own Layer sharing is preserved.
 */
export const makeRepositoryWorkspaceLayer = <
  ConfigFailure,
  RunSinkFailure = never,
  HarnessFailure = never,
>(
  options: SandboxAgentSessionOptions<ConfigFailure, RunSinkFailure, HarnessFailure>,
) =>
  Layer.suspend(() => Layer.fresh(repositoryWorkspaceLayer)).pipe(
    Layer.provide(workspaceBindingsLayer(options)),
  );

/** Generation-bound source support is acquired only by owners that require it. */
export const makeSandboxWorkspaceLayer = <
  ConfigFailure,
  RunSinkFailure = never,
  HarnessFailure = never,
>(
  options: SandboxAgentSessionOptions<ConfigFailure, RunSinkFailure, HarnessFailure>,
) =>
  Layer.suspend(() => Layer.fresh(sandboxWorkspaceLayer)).pipe(
    Layer.provide(workspaceBindingsLayer(options)),
  );

/** Optional delegated turns add a harness to the same model-free workspace capabilities. */
export const makeSandboxAgentTurnLayer = <
  ConfigFailure,
  RunSinkFailure = never,
  HarnessFailure = never,
>(
  options: SandboxAgentSessionOptions<ConfigFailure, RunSinkFailure, HarnessFailure>,
) =>
  Layer.mergeAll(
    Layer.suspend(() => Layer.fresh(sandboxAgentTurnLayer)).pipe(
      Layer.provide(
        Layer.mergeAll(
          workspaceBindingsLayer(options),
          options.harnessLayer ?? CodexHarnessLive.pipe(Layer.provide(webCryptoLayer)),
        ),
      ),
    ),
    options.runSinkLayer ?? SessionRunSink.noopLayer,
  );
