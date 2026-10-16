# @yielded/automation

Investigate events and coordinate authorized responses with Effect.

This package currently supports software investigation, retained decisions and
GitHub repair workflows. A response can be an answer, no action or a proposed
code change. Applications supply their own authority, records and resources.

## Entry points

| Import                           | Purpose                                                                           |
| -------------------------------- | --------------------------------------------------------------------------------- |
| `@yielded/automation`            | Investigation, decision reuse, evidence delivery and GitHub namespaces.           |
| `@yielded/automation/github`     | GitHub App manifests, registration exchange and bounded, verified webhook intake. |
| `@yielded/automation/domain`     | Repair request, result and publication schemas.                                   |
| `@yielded/automation/server`     | Host-composed repair execution and publication services.                          |
| `@yielded/automation/client`     | Typed HTTP client and composable Effect atoms.                                    |
| `@yielded/automation/cloudflare` | Durable execution adapter for coding actions.                                     |
| `@yielded/automation/postgres`   | Drizzle table definitions for host-owned PostgreSQL migrations.                   |

The root and GitHub intake need Effect, not an execution environment.
Coding services under `/server` and `/cloudflare` additionally require
`@yielded/sandbox` and `effect-cf`, declared as optional peers. Import individual
server modules when using an operation independently of coding execution.

## Host-authorized intake

```ts
import type { Redacted } from "effect";
import { readGithubWebhook } from "@yielded/automation/github";

const readEvent = (request: Request, webhookSecret: Redacted.Redacted<string>) =>
  readGithubWebhook(request, webhookSecret);
```

The returned event has passed bounded decoding and signature verification.
The application then maps the installation/repository to a saved tenant,
checks its policy and durably accepts the event before acknowledging it.
An authentic webhook does not itself grant tenant or write authority.

`Investigation.makeInvestigator(Tools)` binds a static Effect Toolkit. Each
investigation resolves the model, tool handlers, retained evidence and trace
sink from Effect services. `DecisionLifecycle` provides pure transitions to use
inside the host's existing acceptance transaction. They preserve the original
accepted request and deadline when an event is replayed.

## Resumable investigations

`@yielded/automation/domain/InvestigationWorkflow` exports the
`InvestigationCheckpoint` and `InvestigationPlan` schemas,
`InvestigationWorkflowError`, and `maxDiagnosticRounds` (three).
`@yielded/automation/server/InvestigationWorkflow` exports the
`InvestigationWorkflow` service, `investigationMachine`, and `runInvestigation`.
These modules need no Sandbox, database driver, or host application imports.

Provide the service's collection, planning, read, synthesis, authorization and
checkpoint operations, then call `runInvestigation(savedCheckpoint)`; pass `null`
for a new investigation. Each operation returns the complete retained `data`
object, not a patch. Query objects remain opaque to the workflow. Search and
lookup plans may contain any number of queries; hosts validate their query kinds,
permissions and per-plan limits. Diagnostic planning receives rounds one through
three. An empty diagnostic plan advances to lookup unless `retry: true` requests
the next available round. Exhausting the third round always advances to lookup.

The machine authorizes every delivery, then resumes the saved phase. It saves
each plan before reading and each read before starting the next query. Lookup
has its own `planning_lookup` phase and per-query checkpoints. Synthesis starts
only after a `synthesizing` checkpoint, and its output is returned only after a
`completed` checkpoint succeeds. A completed redelivery reauthorizes and returns
the saved output.

The host must await durable storage in `checkpoint`, serialize deliveries or
reject stale writes, and reload the last committed checkpoint on redelivery.
An operation whose following checkpoint fails can run again; successfully saved
reads are not repeated. Persist only logical checkpoints with safe retained
evidence, never actor snapshots or credentials. Host operations return
`InvestigationWorkflowError` with `{ code, cause }`; translate foreign failures
once and retain their complete Effect `Cause`. The runner preserves typed
failures, defects, interruption and cleanup failures without squashing them.

Use `runInvestigation(checkpoint, { inspect })` for XState inspection events, or
inspect the exported machine. `RunInvestigationOptions` types the optional
callback. Inspection contains the actual actor transitions and retained data.
Execute through the runner to join in-flight operations and their cleanup when
the caller interrupts or times out.

## Coding follow-ups

For coding follow-ups, `prepareGithubFollowUp` from
`@yielded/automation/server/GithubFollowUps` reads live PR feedback, checks and
conflicts, then asks `GithubFollowUpHost` to atomically accept work under current
policy. Dispatch its accepted launch through the existing `RunClient` path.
Publication retains its intent and receipt, reconciles uncertain writes and
checks the exact PR head before changing it.

For PostgreSQL storage, re-export the tables from `@yielded/automation/postgres`
in your Drizzle schema, then generate, review and apply migrations through your
application's migration system. Only this entrypoint requires the optional
`drizzle-orm` peer. The [PostgreSQL example](../../examples/postgres) provides the
Drizzle Kit configuration and commands. Existing deployments keep their own
migration history and application-specific constraints.

The application's existing records own product state; the library does not
introduce a second application database or event queue. Cloudflare hosts can provide
`RepairSandboxTaskNotifications` to durably accept a task-settled wakeup. Duplicate
delivery rereads the same accepted task and never replays a completed model turn.

See [GitHub App setup](docs/github-app.md). Registered Apps, credentials,
installations and grants remain explicitly host-owned resources. This package
never automatically merges or deploys a generated PR.
