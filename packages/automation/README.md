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
