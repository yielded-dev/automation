# Tests

Each package owns its tests under `packages/<name>/test`. This directory contains
shared fixtures and integration tests that compose both packages.

`vp run check` builds both packages and runs their unit tests. Run one package's
unit tests with `vp run -F @yielded/automation test` or
`vp run -F @yielded/sandbox test`.

## Native fixtures

Build with `vp run build` before running these commands. They use the pinned
local workerd with real storage and alarms. Foreign APIs, host authority and
model harnesses are scripted; these fixtures do not establish live-provider or
hosted behavior.

| Command                                                     | Coverage                                                                                      | Additional prerequisites         |
| ----------------------------------------------------------- | --------------------------------------------------------------------------------------------- | -------------------------------- |
| `vp run -F @yielded/automation test:e2e`                    | Typed RPC inspection, terminal commit, lost/rejected notification, reload and no model replay | None                             |
| `vp run -F @yielded/automation test:e2e:github`             | Bounded webhook intake, signatures, App authentication and GitHub client                      | None                             |
| `vp run -F @yielded/automation test:e2e:publication`        | Publication receipts, exact-head guards and ambiguous outcomes                                | Local Postgres                   |
| `vp run -F @yielded/sandbox test:e2e -- <image>`            | Preparation, snapshot restore, retries, changed setup and service failure                     | Docker images                    |
| `vp run -F @yielded/sandbox test:e2e:git -- <image> 1 fork` | Retained fork intent, original deadlines, source fences and recovery                          | Docker images                    |
| `vp run test:integration -- <image> prepare-events`         | Event coalescing, interrupted preparation and pending source changes                          | Docker images and local Postgres |

CI runs the package unit tests, Automation's native inspection/recovery fixture,
and `vp run check:packages`. The latter installs each tarball into an isolated
consumer, runs native GitHub intake without Sandbox, and bundles the Sandbox
worker without Automation. Pass a local image to `check:packages` to also run
the standalone Sandbox fork fixture in Docker.

The Postgres helper requires `initdb`, `pg_ctl`, `psql` and `postgres` on `PATH`.
It creates and removes a temporary cluster; no existing database is used.

## Docker fixtures

Use Node 24 and a local Docker daemon with a Unix socket. Prepare images before
running the fixtures:

```sh
docker build --platform linux/amd64 -t sandbox-native-e2e:local packages/sandbox/container
docker pull cloudflare/proxy-everything:3cb1195@sha256:0ef6716c52430096900b150d84a3302057d6cd2319dae7987128c85d0733e3c8
```

The runners use immutable local image IDs and do not build, pull or deploy.
Every run owns fresh storage, containers and snapshots. Cleanup removes only
those resources; the printed evidence directory retains results and failures.
Client timings include fixture setup and assertions and do not establish hosted
latency or an isolated performance improvement.

The Git fixture accepts `all`, `warm`, `inspect` or `fork` after the expected
restore-transfer count (`1` normally). `all` includes changed heads, continuations,
retry, full/shallow history and descendant-only publication to local Git.
`inspect` checks scoped outbound grants across reload and publication fences.
`fork` checks source replacement, a lost fork reply, retained candidate recovery,
mixed callback failures and an expiry that retries cannot renew.

The integration fixture also accepts `prepare-current-baseline`,
`prepare-terminal-baseline` and `prepare-setup-failure` for checkpoint reuse,
competing refreshes, cleanup, revocation and failed installation. Its default
suite covers accepted launches, retries, cancellation and saved continuations.

## Historical compatibility

Current-runtime fixtures need no historical checkout. To exercise compatibility
with the original Repairs releases, explicitly supply a local repository:

```sh
vp run -F @yielded/automation test:e2e -- /absolute/path/to/legacy-repository
vp run -F @yielded/sandbox test:e2e -- sandbox-native-e2e:local /absolute/path/to/legacy-repository
```

Inspection reads exactly `82e08f0f5b819d9e69dc697fc87dda14cb71ea07:dist`;
readiness reads `b30515a45c290d190eb257d63c9d5de7138c06b3:dist` for its initial
capture, then uses the current runtime for retry and restore. Neither command
fetches historical code or substitutes the current code when it is missing.
