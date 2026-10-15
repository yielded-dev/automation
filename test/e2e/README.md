# Native readiness

`vp run test:e2e:inspect` exercises the current typed RPC client and Durable
Object with a saved running record, request fingerprint and launch receipt.
It uses local workerd and SQLite only and needs no historical repository.
It does not fetch, run containers or call providers; evidence stays in the
printed `.vite/inspect-*` directory.

The same fixture resumes saved model/candidate receipts through the current task
owner and tests completion delivery with real SQLite transactions, alarms and
reloads. It covers a rejected terminal transaction, rejected and lost host
acknowledgments, duplicate admission, mixed failure causes, cleanup, and hosts
without a notification adapter. Host queue acceptance, Artifacts metadata and
compute cleanup are foreign fixture endpoints. This does not prove live Queue
delivery, model execution, checkout, containers, publication or hosted behavior.

`vp run test:e2e -- <local-image>` bundles the public `@yielded/sandbox` entrypoints
with Vite+ and runs six synthetic cases in the pinned Miniflare/workerd with real Docker containers:
cold installation and snapshot capture, same-boot retry, compatible snapshot
restore after an unrelated source commit, restored retry, changed installation
input rejection, and required-service exit after preparation. Assertions include
boot ownership, install/start counts, real HTTP readiness and boundary spans.

Prerequisites are Node 24, `bun install --frozen-lockfile`, `vp run build`, and a
local Docker daemon with a Unix socket (the current Docker context or
`DOCKER_HOST`). Build the packages before running any of these fixtures.
Prepare images separately:

```sh
docker build --platform linux/amd64 -t sandbox-native-e2e:local packages/sandbox/container
docker pull cloudflare/proxy-everything:3cb1195@sha256:0ef6716c52430096900b150d84a3302057d6cd2319dae7987128c85d0733e3c8
vp run test:e2e -- sandbox-native-e2e:local
```

The task requires both images locally and uses their immutable image IDs. It
does not build, pull, deploy, call providers or use consumer dependencies. Each
run gets fresh Durable Object storage and container names; cleanup removes only
that run's containers, native snapshot images and runtime state. Synthetic JSON
evidence stays in the printed temporary directory. The task is explicit because
the regular `Check` job does not provision Docker images.

The fixture exercises real preparation and restore APIs. It does not prove
Artifacts admission/retention, a coding turn, publication or hosted latency.
Client timings include fixture setup and assertions; nested spans must not be
added together. Prepared receipts and supervisor readiness do not establish app
readiness; the success cases also require a real HTTP response.

Both commands default to the current runtime. To additionally exercise historical
compatibility, explicitly supply a local repository containing the original
compiled releases:

```sh
vp run test:e2e:inspect -- /absolute/path/to/legacy-repository
vp run test:e2e -- sandbox-native-e2e:local /absolute/path/to/legacy-repository
```

Inspect reads exactly `82e08f0f5b819d9e69dc697fc87dda14cb71ea07:dist` for the
pre-fingerprint server; readiness reads exactly
`b30515a45c290d190eb257d63c9d5de7138c06b3:dist` for the initial boot and capture,
then switches to the current runtime for retry and restore. Only the supplied
repository provides historical code; neither command fetches it or falls back
to current code if it is missing. Without that argument, inspect proves current
storage/RPC/reload behavior and readiness proves current boot/retry/restore
behavior, not compatibility with an older release. Evidence identifies which
mode ran. The historical implementations are not vendored in this repository.

`vp run test:e2e:github` exercises public `@yielded/automation` imports with native
workerd HTTP intake, HMAC verification, RSA App authentication and the GitHub
client. The foreign GitHub API is scripted; it needs no sandbox binding, Docker,
database or model provider. It does not prove live GitHub behavior or action
execution. Public imports in these workspace fixtures do not establish independent
tarball consumption.

`vp run test:e2e:git -- <local-image>` imports only public `@yielded/sandbox`
entrypoints and exercises the actual message pipeline and checkout adapter
against local bare repositories served by `git http-backend`.
Only the external Artifacts control plane and model harness are fixture ports;
Worker storage, Docker snapshots, scoped outbound credentials, Git transfers,
and ancestry checks run natively. It covers cold setup, unchanged and changed
heads, three saved-candidate continuations, retry, full and shallow prepared
history, changed-input rejection, source/generation fences, and descendant-only
publication to a local Git remote. It does not contact hosted providers.

Passing `1 fork` after the image selects the retained-fork cases: a pin distinct
from current main, a held provider reply during the synthetic turn, source
replacement during either branch read, pre-model failure, saved-result recovery
without redispatch, and an expired receipt that retries cannot renew. Recovery
also crosses the legacy/current turn APIs after a failed generation handoff,
preserving the candidate and deadline before the first push. The same fixture
checks ordered progress, steering, original mixed callback causes and absent
handoff authority. Recovery proves pipeline/native-store ordering, not Codex
process reattachment.

The changed-head cases require one transfer, followed by generation-checked
local history reads. The optional transfer-count argument is an assertion,
not a selector for historical code. Passing `1 inspect` after the image selects
full-history preparation and restore, native inspection, scoped outbound grants
across reload and publication fences. Evidence records resolved runtime entry
paths and hashes, package versions, transfer counts, and client wall time including fixture setup and
assertions. Timings on a shared Docker engine are diagnostic, not hosted or
isolated speedup evidence. Failed cases remain in the temporary results file.

`vp run test:e2e:launch -- <local-image> prepare-current-baseline` exercises
exact-commit preparation, dependency reuse, full-history demand restore, changed
inputs, capture/publication recovery and competing expiry refreshes against real
Postgres and native containers. It uses synthetic source and no model provider.
`prepare-terminal-baseline` uses the same fixture to prove pre-capture owner loss,
source revocation, cleanup and acknowledged-capture recovery across generations.
`prepare-setup-failure` covers oversized/invalid recipes and failed installation,
terminal cleanup, duplicate safety and a valid next preparation generation.
It observes native output bounds and preserves retries for stderr-only truncation.

`prepare-events` uses the standalone, model-free preparation factory. It exercises
event coalescing, lost execution replies, retained alarm deadlines across a
workerd reload, and source/head changes while a generation is pending. The
source-authority and Artifacts endpoints are scripted; execution, Postgres,
SQLite, alarms, Git and container snapshots are native. It does not prove a
hosted provider or a real Queue binding; the adopting host owns that boundary.

## Native inactivity isolation

`inactivity.mjs` runs the small `inactivity.worker.ts` app in installed workerd
with cached Docker images. It uses native `DurableObject` and `ctx.container`
directly, without Repairs or Effect. The same local image and egress-image
prerequisites above apply. Select one stage per invocation:

```sh
vp node test/e2e/inactivity.mjs <local-image> bare 3
vp node test/e2e/inactivity.mjs <local-image> snapshot 3
vp node test/e2e/inactivity.mjs <local-image> identity 3 inactivity
vp node test/e2e/inactivity.mjs <local-image> sync 3 inactivity
vp node test/e2e/inactivity.mjs <local-image> alarm 3 inactivity
vp node test/e2e/inactivity.mjs <local-image> artifacts 1
vp node test/e2e/inactivity.mjs <local-image> snapshot 2 exec 16
```

The sample count is 1–5 (default 3). The optional order argument selects
`inactivity`, `inspect`, or `exec` as the first awaited control call after start;
the default rotates all three. Successful `/bin/true` execution is the independent
command-readiness boundary. The entrypoint only emits a process marker and idles.

The optional final `payloadMiB` argument generates 1–4096 MiB of random bytes in
the snapshot instead of the tiny marker. It requires a snapshot, identity, sync,
or alarm stage; omission preserves the marker baseline. `16` is a small local
check; `1024` selects 1 GiB and `4096` explicitly opts into 4 GiB. Generation and
SHA-256/byte-count verification run after command readiness. Restores measure the first native
`/bin/true` before reading or hashing the payload; generation, verification,
snapshot capture and readiness retain separate spans. Returned `payload.sizeBytes`
counts generated bytes; `capturedSnapshot.size` is native snapshot metadata with
platform-dependent accounting, not a measured transfer size.

`snapshot` captures a real native snapshot and verifies a restored file in every
fresh actor. `identity` adds the production-shaped identity write between start
and inactivity. `sync` adds an explicit post-write storage sync for diagnosis.
`alarm` runs that operation from a native alarm and verifies a repeated admission
returns the same persisted receipt. `artifacts` checks the real installed local
binding, then runs one container case while retaining a real synthetic repository
handle only if supported. It reads metadata with native `repo.info()` before and
after the container call, and deletes the repository afterward.
It supplies no remote connection or substitute. An unavailable capability retains
its original error and skips repository operations. This stage does not prove a
Git checkout. Initial Artifacts credentials never leave the Worker or enter a
snapshot. An ambiguous creation retains the intended repository name for manual
reconciliation; it is not adopted or silently retried.

The printed evidence directory retains call endpoints, snapshot size, failures,
client elapsed times, Docker process markers, and cleanup receipts. Worker clocks
can round short calls to zero. Docker timestamps use a separate clock domain;
do not subtract them from Worker spans or sum overlapping spans. A cached local
snapshot is not proof of hosted restore latency. Cleanup targets only this run's
actors, containers, snapshots, and native storage. This runner binds to localhost
and injects a fresh synthetic token in memory. Every app route requires POST and
a matching `Authorization: Bearer <token>` header; missing or malformed
`ISOLATION_TEST_TOKEN` configuration fails closed before accessing any binding.

`inactivity.wrangler.jsonc` is an optional host-owned deployment configuration
using the managed `cloudflare/debian-trixie` base, with no image build/publication.
It includes a dedicated DO and actual Artifacts binding; supply a disposable
64-character lowercase hex `ISOLATION_TEST_TOKEN` secret separately. An offline
configuration/bundle check is:

```sh
vp node <installed-wrangler>/bin/wrangler.js deploy --dry-run --config test/e2e/inactivity.wrangler.jsonc
```

Deployment and hosted driving require separate host authorization. Use a fresh
actor per sample and retain results before POST `/destroy` with a JSON body
`{"name":"<owned-name>"}`. POST `/purge` with the same body additionally removes that actor's storage and
alarm, returning any snapshot receipt first. Neither route deletes hosted
snapshots: this installed native API/CLI has no snapshot-delete operation. Account
for snapshot retention before capture; do not mistake stopping a container for
removing its snapshot. Managed-base hosted results are not the same image baseline
as this runner's cached sandbox image. A private bounded manifest can drive the
same routes without adding another runner to the repository.

Before comparing samples, check `/health` for the expected `probeRevision` and
configured `imageReference` selector, then check the result's actor
`probeRevision` and direct `/run`'s `callerRevision`. The local runner asserts
these revisions and the actual Docker image or snapshot. A health selector does
not identify a restored snapshot's image; keep that provenance with the private
snapshot inputs. Optional `locationHint: "wnam" | "enam" | "weur"` is a native,
best-effort hint on the first actor lookup. Use fresh actors and verify actual DO
and container placement separately; the hint does not guarantee either location.

All scenario inputs use JSON string fields. To capture generated data, POST `/run`
with `{"name":"payload-seed","stage":"snapshot","order":"exec","capture":"true","payloadMiB":"1024"}`.
For each fresh restore, send its new `name`, `stage`, `order`, `snapshot` from
`capturedSnapshot.id`, the same `payloadMiB`, and `expectedPayloadSha256` from
`payload.sha256`. A missing or malformed expected hash is rejected before startup;
a mismatched restored hash returns `passed: false` with the readiness events intact.
The purge receipt also retains payload size and hash. Do not combine generated
payloads with `expectedGitHead`. Each local snapshot invocation captures one
snapshot; the runner removes only its owned snapshot image and actors after all
samples.

An explicitly approved existing snapshot comparison can send `snapshot`,
`expectedGitHead` and `shape: "matched"`
to `/run` alongside `name`, `order` and `stage: "identity"`. Keep actual IDs and
revisions in private inputs. `matched` is the single fixed alternative
`{vcpu: 2, memoryMib: 12288, diskMb: 20000}`; other cases default to `standard-1`.
The result records the selected instance. Supplying a full Git revision changes
restore verification to only `git -C /workspace/repo rev-parse HEAD`; it cannot be
combined with capture. It does not run repository scripts or Git network
operations. Without that field or `payloadMiB`, restored snapshots must contain
the synthetic marker. The restored filesystem always comes from the supplied
native snapshot, with no image fallback. An optional `artifactsRepo: "inactivity-<uuid>"` holds a
real synthetic repo handle around this same call, without exporting Git tokens.
