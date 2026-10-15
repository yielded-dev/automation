# Boundaries and durability

`@yielded/automation` owns event-response workflows. Its coding adapters depend
on `@yielded/sandbox`; the dependency never points back. Sandbox owns execution
and includes its matching container assets. Both packages consume host services,
never application source or product schemas.

| Boundary               | API                                                            | Responsibility                                                                         |
| ---------------------- | -------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| Normalized input       | `readGithubWebhook`                                            | Verify the signature, bound the payload and decode a supported event.                  |
| Investigation          | `Investigation.makeInvestigator`                               | Run host-supplied read tools and return a validated report with source evidence.       |
| Retained decisions     | `DecisionLifecycle`                                            | Apply pure selection and reuse rules inside the host's acceptance transaction.         |
| Authority              | `GithubFollowUpHost`, `RepairAuthorization`                    | Resolve saved tenant/repository authority and atomically accept permitted work.        |
| Actions                | `RunClient`, `prepareGithubFollowUp`                           | Coordinate an accepted launch and reconcile publication.                               |
| Workspace execution    | `runTurn`, workspace layers                                    | Execute a host-selected harness against an authorized source with a retained deadline. |
| Checkpoint preparation | `RepositoryPreparationSources`, `requestRepositoryPreparation` | Coalesce events and complete retained source generations through native alarms.        |

The Automation root and `/github` entrypoints can be installed without Sandbox
or Cloudflare. The `/server` and `/cloudflare` coding adapters require the optional
Sandbox and `effect-cf` peers. Sandbox's runtime-neutral schemas are separate from
its `/cloudflare` implementation. A user-started coding task can use Sandbox
without event classification or Automation.

## State ownership

Applications keep their existing inboxes, account relationships, product records
and acceptance transactions. A webhook or model response proposes data; neither
creates tenant or write authority. The host durably accepts an event before
acknowledging its delivery.

The accepted action retains its request identity, original deadline, source and
publication receipts. Replayed input reuses that action. A changed source does
not retarget work already accepted under an earlier source generation.

Sandbox retains native execution and checkpoint receipts in its existing durable
owner. Worker interruption does not discard an in-flight process or renew its
deadline. Effect scopes own leases, subscriptions and temporary credentials.
Generation-bound turns require `GenerationSourceCheckpoint` before a remote write.

Checkpoint preparation completes retained generations before selecting newer
ones. Its native alarms own retries. Normal work starts from events; maintenance
may recover missed events or refresh expiring checkpoints.

A terminal coding task and its pending settlement notification commit together.
The host accepts repeated notifications by task ID and rereads saved authority
before publishing. Notification failure does not run the model again.
Publication reconciles uncertain branch/PR writes and checks the exact live head.
Neither package automatically merges or deploys a proposed pull request.

## Compatibility

Historical opaque service IDs, process paths, durable object names, wire schemas
and storage keys are unchanged. Keep the same storage and namespace bindings when
adopting the packages. Queue retention bridges a rolling upgrade in which an older
receiver does not yet expose a new method.

The [native fixture guide](../test/e2e/README.md) distinguishes current-runtime,
historical-comparison, scripted-provider and container proofs. Native fixtures
are not evidence of a hosted provider run.
