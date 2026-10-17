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

`DecisionLifecycle` provides pure transitions for the host's existing acceptance
transaction. They preserve the accepted request and deadline on redelivery.

## Investigation as a capability

`makeInvestigator` owns research turns, read-only tool execution, argument
correction, retained evidence, citation validation and structured reporting.
Supply a Toolkit, a finding schema and product instructions. It does not need a
Sandbox, database driver or application imports.

```ts
import { Exit, Schema } from "effect";
import { Tool, Toolkit } from "effect/ai";
import { InvestigationObservation } from "@yielded/automation/domain/Investigator";
import { makeInvestigator } from "@yielded/automation/server/Investigation";

const tools = Toolkit.make(
  Tool.make("read_trace", {
    parameters: Schema.Struct({ requestId: Schema.String }),
    success: InvestigationObservation,
  }).annotate(Tool.Readonly, true),
);
const investigator = makeInvestigator({
  tools,
  result: Schema.Struct({ summary: Schema.String }),
  version: "incident-v1",
  instructions: "Find the first failed request boundary. State what remains unknown.",
});
```

Provide the model through `LanguageModel`, readers through `tools.toLayer`, and
storage through `InvestigationStore`. Readers return `{ items, limitations }`;
each evidence item has a stable ID and provenance. Treat unavailable evidence as
a limitation when that is an expected product outcome. Typed failures, defects
and interruption otherwise stop the attempt with their full Effect `Cause`.
Only parameter-validation errors from Toolkit's argument decoder are sent back
to the model for correction; failures originating inside a reader are not.

Call `investigator.run({ id, prompt, evidence })` as an Effect, or invoke
`investigator.actor` in a consumer-owned XState 6 machine. The returned value is
`{ finding, citations }`, with `finding` typed by the supplied schema. The actor returns an Effect `Exit` so a consumer can route on success or retain
the full failure Cause. Consumers choose the surrounding workflow; no library
callback bag defines their states.
For example, a consumer can put human review after investigation:

```ts
import { setupEffect } from "@xstate/effect";

const workflow = setupEffect({
  schemas: {
    context: Schema.Struct({ summary: Schema.String }),
    events: { APPROVE: Schema.Struct({}) },
  },
  actors: { investigate: investigator.actor },
}).createMachine({
  context: { summary: "" },
  initial: "investigating",
  states: {
    investigating: {
      invoke: {
        src: "investigate",
        input: {
          id: "incident-1",
          prompt: "Investigate request 123",
          evidence: { items: [], limitations: [] },
        },
        onDone: ({ event }) =>
          Exit.isSuccess(event.output)
            ? { target: "reviewing", context: { summary: event.output.value.finding.summary } }
            : { target: "failed" },
      },
    },
    reviewing: { on: { APPROVE: { target: "approved" } } },
    approved: { type: "final" },
    failed: { type: "final" },
  },
});
```

## Checkpoints and recovery

`InvestigationStore` has `load` and `save`. Bind these to the invocation's existing
durable record. Authorize every delivery before calling the capability; readers
must also authorize each resource they access. The host must await durable writes,
serialize deliveries or reject stale writes, and load the last committed checkpoint
on retry. Credentials and bindings stay in Effect services, outside the checkpoint.

`InvestigationCheckpoint` from `/domain/Investigator` stores the definition version,
exact conversation, evidence, pending calls, cursor, remaining research time and
completed output. Each research turn and completed tool result is saved before
another operation starts. Reporting retries reuse that history. A completed replay
returns the saved result without another model call. A read whose following save
failed can run again: this is not an exactly-once guarantee for external effects.
Changing the definition version rejects an old checkpoint instead of interpreting
it under a different workflow.

The default is three research rounds, at most three executed reads per round,
180 seconds of active research and 128 KiB of retained tool output before further
reads stop. A completed read is retained in full and can exceed that byte threshold.
Queue delays consume no active budget. Each research model request has a 45-second
limit; reporting has a 120-second limit per attempt and at most two structured-output
corrections. Product-specific limits belong inside the supplied readers.

The capability uses XState 6 with Effect. `investigator.machine` exposes the actual
internal graph for visualization; `run(input, { inspect })` emits its actual actor
transitions. Persist logical checkpoints, not interpreter snapshots. The runner
waits for in-flight work and cleanup on interruption. The outer consumer graph
still needs a host lifecycle: invoking an actor does not automatically make that
graph durable.

## Coding follow-ups

For coding follow-ups, `prepareGithubFollowUp` from
`@yielded/automation/server/GithubFollowUps` reads live PR feedback, checks and
conflicts, then asks `GithubFollowUpHost` to atomically accept work under current
policy. Dispatch its accepted launch through the existing `RunClient` path.
Publication retains its intent and receipt, reconciles uncertain writes and
checks the exact PR head before changing it. Pass `draft: true` to
`runs.publish(id, { candidateSha, summary, draft: true })` when proposals should
remain draft PRs. The retained publication intent includes that choice; retries
reconcile the same intent rather than converting an existing PR.

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
