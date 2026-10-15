# Public libraries

Never merge a pull request or publish a release without explicit user approval
covering this repository and action. Application permissions do not authorize
changes to public dependencies. Prepare reviewable changes before asking.

Use Vite+ for commands and Bun for package management. Run `vp run check` before completion.
Read `node_modules/effect/AGENTS.md` completely before writing Effect code and follow its relevant links.

## Package responsibilities

This repository contains reusable, consumer-agnostic libraries:

- `@yielded/automation` owns event investigation, classification, decision reuse,
  action coordination and publication. Classification proposes an outcome; host
  policy authorizes it. No action, a read-only answer and an explicit change are
  valid outcomes. Input and investigation must work without Sandbox.
- `@yielded/sandbox` owns repository preparation, workspaces, harness execution,
  checkpoints and execution recovery. Keep its container assets and protocol
  versions together. It must not import Automation or a consumer application.

Hosts own authentication, tenant/repository authorization, product records,
configuration, credentials and provisioned resources. Supply these through
services at composition roots. Shared workflow rules and atomic transitions
remain library responsibilities even when the host owns their storage. Registered
GitHub Apps, installations and permissions are explicit host resources; webhooks
and model output cannot create authority.

Keep one implementation and use existing state owners. Events drive normal work;
polling is for recovery or maintenance. Preserve accepted identities, original
deadlines, receipts, source-generation fences, exact-head publication checks and
ambiguous-outcome recovery. Generated PRs are never automatically merged or deployed.
Historical opaque identifiers are compatibility contracts, not application imports.

Use concrete consumers to justify abstractions. Prefer removing machinery to
adding frameworks, adapters, caches or journals. React renders and dispatches;
shared client state belongs in Effect Atom. Default to existing checks and direct
workflow evidence; no new tests without a concrete regression or explicit request.

## Review and verification

Main requires a PR and the `Check` status. After user approval, squash merge and
delete the branch. Build package `dist` for checks and npm tarballs; do not commit it.
Run the applicable native fixtures when changing execution, storage or packaging.

Review changed behavior for correctness, ownership, security and operability.
Findings need a diff anchor and a concrete failure mode or policy violation;
prefer no finding to an invented one. Formatting belongs to the toolchain.

- Keep Effect requirements visible. Yield services or inward ports in operations
  and provide implementations at composition roots. Dependency drilling through
  internal parameters, options bags or effectful callbacks is P1. Request data,
  pure transformations and Layer configuration are legitimate arguments.
- Preserve original failure causes and scoped cleanup. Decode untrusted input at
  boundaries. Keep credentials out of persisted execution state; redact private
  payloads and credentials from logs and public errors.
- Select the operation family before acquiring unrelated services. Do not
  duplicate durable application records merely to simplify reads.
- Require a current regression or explicit human request plus a concrete gap in
  existing proof for new committed tests. Do not request tests merely because code changed.
- Keep documentation to purpose, ownership, non-obvious constraints and necessary
  manual operations. Use generic examples; do not publish private work reports.
- The reviewer reads guidance from the trusted default branch and does not
  execute PR code. Do not switch to `pull_request_target`. Lockfiles are excluded
  from review; CI builds packages and verifies their isolated tarball installs.
