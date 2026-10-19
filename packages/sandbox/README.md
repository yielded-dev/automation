# @yielded/sandbox

Durable agent workspaces with Effect: prepare a repository, run a chosen harness,
retain its result and recover after interruption.

Sandbox can run user-created tasks independently of Automation. Applications own
authentication, repository authorization, credentials, storage bindings and
container deployment. The included execution backend uses Cloudflare Durable
Objects, Containers and Artifacts.

## Entry points

| Import                                              | Purpose                                                                 |
| --------------------------------------------------- | ----------------------------------------------------------------------- |
| `@yielded/sandbox`                                  | Runtime-neutral request, event, repository and result schemas.          |
| `@yielded/sandbox/cloudflare`                       | Workspace layers, harness execution and durable repository preparation. |
| `@yielded/sandbox/harness/Harness`                  | The interface for a host-selected agent harness.                        |
| `@yielded/sandbox/RuntimeVersions`                  | Matching container tooling and compatibility versions.                  |
| `@yielded/sandbox/repository/RepositoryGitTransfer` | Repository transfers and retained transfer recovery.                    |
| `@yielded/sandbox/container/*`                      | Container build assets shipped with this version.                       |

## Compose at the host boundary

`repositoryWorkspaceLayer` provides model-free checkout, storage and process
coordination. `sandboxWorkspaceLayer` adds a generation-bound checkout, and
`sandboxAgentTurnLayer` adds harness execution. Hosts compose their implementations
using ordinary Effect layers:

```ts
import { Layer } from "effect";
import { sandboxWorkspaceLayer } from "@yielded/sandbox/cloudflare";

// HostWorkspaceLive supplies configuration, bindings and repository authority.
const WorkspaceLive = sandboxWorkspaceLayer.pipe(Layer.provideMerge(HostWorkspaceLive));
```

`runTurn(request)` takes accepted task data: the session and turn identity,
instructions, source and original deadline. Its Effect requirements expose the
workspace, `Harness`, `AgentEventSink` and execution state. A host can supply the
included Codex adapter or another harness. Archiving hosts compose `SessionRunSink`
separately. See the repository's native Git fixture for a complete host using
these public entrypoints with a scripted harness.

Native receipts outlive individual Worker invocations. Retries keep the accepted
identity and deadline; source changes cannot silently retarget an accepted turn.
Effect scopes release leases, subscriptions and temporary credentials. A retained
result can be recovered without asking the model to produce it again.

## Filesystem checkpoints

`makeNativeSandbox` provides files, processes and checkpoints without a repository
or coding harness. The host supplies `NativeSandboxNamespace.layer({ binding })`:

```ts
import { Effect } from "effect";
import { makeNativeSandbox } from "@yielded/sandbox/cloudflare";

const save = Effect.gen(function* () {
  const sandbox = yield* makeNativeSandbox("workspace-123");
  const boot = yield* sandbox.ensure();
  yield* sandbox.writeFile("/workspace/hello.txt", "Hello");
  return yield* sandbox.checkpoint({ instanceId: boot.instanceId, sequence: 0 });
});
```

Retain the sequence before capture and increase it for each save in that boot.
An optional `name` is passed to the native snapshot API and must stay unchanged
on retry. Retrying the latest sequence returns its original receipt; older
sequences and uncertain captures are rejected without taking another snapshot.
Keep workspace writes settled until capture returns.

After the container stops, `sandbox.restore(saved)` starts from that checkpoint
and returns the new instance identity. It rejects an unavailable checkpoint or a
different live workspace. Processes start fresh. Checkpoints are image- and
class-specific and have an expiry; keep `backupDirectory` receipts for portable,
long-lived saves and restore those with `restoreDirectory` after an image upgrade.
Repository preparation adds its source checks and sharing policy above this API.
Capture and restore use Cloudflare's native
[`snapshotContainer()` and `start({ containerSnapshot })`](https://developers.cloudflare.com/containers/guides/snapshots/)
with `durable_object` scheduling.

`sandbox.forwardPort(port, request)` forwards a host request to a running workspace.
To authorize browser access, `sandbox.grantPreview` binds the chosen port to a token,
live process, workspace instance, materialization and expiry. Hosts authenticate users
and route authorized requests. Native control ports cannot be previewed; use a separate
port for a host-managed static server so project dev servers can keep their defaults.

## Repository preparation

Supply `RepositoryPreparationSources` to authorize and select the repository's
exact source generation. From an existing durable event consumer, provide
`RepositoryPreparationObject.layer({ binding })` and call
`requestRepositoryPreparation(repository)`.

`makeRepositoryPreparationDurableObject` supplies a model-free preparation owner.
It coalesces requests, completes retained generations before selecting newer
sources and retries uncertain outcomes using native alarms. Use the same request
for missed-event recovery and `refreshWorkspaceBaselines` for expiry maintenance.

Keep the existing namespace, `repository-preparation:<hash>[:<generation>]` names,
`repositoryPreparation` storage key and receipts when migrating a deployment.
Durably enqueue events before acknowledging them so queue retries can bridge a
rolling upgrade in which the old receiver lacks a new method.

## Container image

Build from the assets in the installed package, keeping them paired with its
runtime version:

```sh
docker build --platform linux/amd64 -t agent-workspace:local node_modules/@yielded/sandbox/container
```

The application owns the image registry, deployment, bindings and secrets. This
package does not provision infrastructure or start a central hosted service.

`makeSandboxAgentContainer` starts containers without public internet access by
default. Set `enableInternet: true` in its host configuration for direct outbound
access, including package registries, browser downloads and public repositories.
Configured outbound interceptors still apply; enabling internet access does not
grant host credentials. The setting takes effect when a container starts.
