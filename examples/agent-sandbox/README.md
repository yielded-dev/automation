# Agent with a coding tool

A standalone `@yielded/agent` assistant calls `start_coding` to fix a disposable
repository through `@yielded/sandbox/cloudflare`. It does not import Automation.
Use Automation when an event needs investigation, classification, host policy
and coordinated publication; an explicitly requested coding task can use Sandbox
directly.

```text
Yielded Agent → start_coding → CodingSession Durable Object → Sandbox → Codex
                                      ↓                         ↓
                               accepted request           saved Git commit
```

[agent.ts](agent.ts) defines the tool and agent. [worker.ts](worker.ts) binds
Sandbox to a native Durable Object, storage and host-selected repository. The
tool accepts instructions, never a repository, credential or authority grant.
The host retains the first accepted source and deadline. Sandbox owns execution
receipts, workspace preparation, persistence and completed-turn recovery.

[run.mjs](run.mjs) supplies a local host and checks the whole path. It starts
native workerd and Docker, asks the real outer model to use its tool, and runs the
real Codex harness. It independently checks out the saved commit and runs the
repository's tests, then stops the container and restarts workerd to check that
replaying the accepted task returns the same result without another model call.

## Run

Install and build from the repository root. Use Node 24 and prepare the two
local Docker images described in the [native fixture guide](../../test/README.md).
Then supply your provider key through your usual secret manager and select a
model available to that account:

```sh
vp install
vp run build
# OPENAI_API_KEY and OPENAI_MODEL must be set in this process.
vp run -F @yielded/example-agent-sandbox live -- sandbox-native-e2e:local
```

This makes billed provider calls. The printed evidence directory retains the
synthetic repository, model events, immutable result and failures. The runner
removes its own containers, checkpoints and Durable Object storage.

This is a local integration example, not a hosted application. Workerd, Durable
Object storage, containers, Git and both model invocations are real; Artifacts
metadata uses the existing local fixture. The outer agent uses in-memory state.
The local HTTP entrypoint has no authentication and must not be exposed publicly.
A deployed host must supply authentication, tenant authorization, an Artifacts
binding and lifecycle scheduling. No pull request, merge or deployment is made.
