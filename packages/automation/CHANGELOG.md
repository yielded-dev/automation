# @yielded/automation

## 0.9.0

### Minor Changes

- [#35](https://github.com/yielded-dev/automation/pull/35) [`89796c7`](https://github.com/yielded-dev/automation/commit/89796c7e9f12d59632a7239c8fed34c5ddc94e5e) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Require Effect 4.1 and effect-cf 0.59, and provide native Digest and SecureRandom services at composition roots. Upgrade the paired Cloudflare Sandbox SDK and container image to 1.0.3; rebuild host images when adopting this release.

### Patch Changes

- Updated dependencies [[`89796c7`](https://github.com/yielded-dev/automation/commit/89796c7e9f12d59632a7239c8fed34c5ddc94e5e)]:
  - @yielded/sandbox@0.9.0

## 0.8.0

### Minor Changes

- [#34](https://github.com/yielded-dev/automation/pull/34) [`534fbe7`](https://github.com/yielded-dev/automation/commit/534fbe79e0dab8a61fbaee817fd3381b4e78e1d1) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Provide concise investigation reports and paginated private evidence with credential redaction. Expose whether a repair reports before-and-after observations for its claimed fix.

- [#32](https://github.com/yielded-dev/automation/pull/32) [`e39ffa6`](https://github.com/yielded-dev/automation/commit/e39ffa628dbee32068a4647c8720221e775cda86) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Apply the shared Effect checking policy. GitHub App token, installation URL and bot login operations, and container identity reads, are now reusable Effects instead of zero-argument functions. Publication-content adapters use the supplied run state instead of an ambient execution context. Supply Crypto at custom composition roots; Cloudflare factories provide Web Crypto. Reject non-finite persisted deadlines and start times, and preserve selective failure recovery and scoped cleanup.

### Patch Changes

- Updated dependencies [[`e39ffa6`](https://github.com/yielded-dev/automation/commit/e39ffa628dbee32068a4647c8720221e775cda86)]:
  - @yielded/sandbox@0.8.0

## 0.7.0

### Minor Changes

- [#29](https://github.com/yielded-dev/automation/pull/29) [`77a5169`](https://github.com/yielded-dev/automation/commit/77a51696b2c1e3b0678fe201f9fae828ca05c44c) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Render repair PRs with host-configured links to repairs, source reports and evaluation runs. Distinguish bug fixes, eval changes and features in the template and GitHub labels.

### Patch Changes

- Updated dependencies []:
  - @yielded/sandbox@0.7.0

## 0.6.2

### Patch Changes

- [#27](https://github.com/yielded-dev/automation/pull/27) [`ad00968`](https://github.com/yielded-dev/automation/commit/ad0096886040c6b3b24152a3b515b499881f05e0) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Publish a saved continuation without requiring another commit when its changes have not reached the pull request. Keep publication tied to the exact saved snapshot.
- Updated dependencies []:
  - @yielded/sandbox@0.6.2

## 0.6.1

### Patch Changes

- [#25](https://github.com/yielded-dev/automation/pull/25) [`fcbbf4e`](https://github.com/yielded-dev/automation/commit/fcbbf4e4205d5f24679ebfffc695a59a9fee33aa) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Reduce PostgreSQL round trips when accepting source decisions while preserving identity precedence and concurrent deduplication.
- Updated dependencies []:
  - @yielded/sandbox@0.6.1

## 0.6.0

### Minor Changes

- [#23](https://github.com/yielded-dev/automation/pull/23) [`9e66287`](https://github.com/yielded-dev/automation/commit/9e66287dbaae12f8c886206885907aabba18a464) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Compose typed source pipelines with built-in Cloudflare and Sentry sources, optional semantic hooks, and PostgreSQL-owned identity deduplication. Replace host-implemented deduplication stores with scoped services and Layers that retain canonical decisions across retries.

### Patch Changes

- Updated dependencies []:
  - @yielded/sandbox@0.6.0

## 0.5.0

### Minor Changes

- [#21](https://github.com/yielded-dev/automation/pull/21) [`18ed311`](https://github.com/yielded-dev/automation/commit/18ed31115b93a6083e9f5dbc30b74a8c91d4183c) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Deduplicate incoming work by source identity before consulting an optional consumer semantic policy. Retain decisions through host storage so retries reuse accepted work and stale comparisons cannot admit concurrent duplicates.

### Patch Changes

- Updated dependencies []:
  - @yielded/sandbox@0.5.0

## 0.4.0

### Minor Changes

- [#19](https://github.com/yielded-dev/automation/pull/19) [`e1a77c5`](https://github.com/yielded-dev/automation/commit/e1a77c5266bf686a161f4873b262fce46a2ac7c2) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add optional `RepairVerification` to reports and publication requests for host policy, requiring explicit null or data in new provider output while preserving legacy decoding. Preserve the live draft status of explicitly continued or rebased pull requests regardless of the preference for new PRs.

### Patch Changes

- Updated dependencies []:
  - @yielded/sandbox@0.4.0

## 0.3.2

### Patch Changes

- Updated dependencies [[`8abca50`](https://github.com/yielded-dev/automation/commit/8abca50721dd578e24848cdffea990f22fb0d322), [`50faba2`](https://github.com/yielded-dev/automation/commit/50faba21ac2713817ef8056e4808ccd6ed9a6548)]:
  - @yielded/sandbox@0.3.2

## 0.3.1

### Patch Changes

- Updated dependencies [[`ebb045e`](https://github.com/yielded-dev/automation/commit/ebb045e4991a7e7ea45f55b807ead461fcf7b95c)]:
  - @yielded/sandbox@0.3.1

## 0.3.0

### Minor Changes

- [#12](https://github.com/yielded-dev/automation/pull/12) [`e0c851d`](https://github.com/yielded-dev/automation/commit/e0c851dd013610d8f02c975019746fe04e3781db) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Save filesystem checkpoints without repository preparation using `checkpoint({ instanceId, sequence })`, with repeated saves per boot and a retained receipt for safe retries. Restore with `restore(checkpoint)`, which rejects unavailable checkpoints instead of opening an empty workspace; existing repository checkpoint validation and saved receipts remain supported.

### Patch Changes

- Updated dependencies [[`e0c851d`](https://github.com/yielded-dev/automation/commit/e0c851dd013610d8f02c975019746fe04e3781db)]:
  - @yielded/sandbox@0.3.0

## 0.2.2

### Patch Changes

- [#8](https://github.com/yielded-dev/automation/pull/8) [`fbcd741`](https://github.com/yielded-dev/automation/commit/fbcd741926764497cb098f21a9e74369ccbbf6f7) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Fix repair activity pages failing when they contain a truncated-output marker. Runtime event payloads now retain their JSON values, including null, while Codex events still require JSON object payloads.
- Updated dependencies [[`2508932`](https://github.com/yielded-dev/automation/commit/2508932e85040689bef387e608f2202f128632c8), [`6e2d538`](https://github.com/yielded-dev/automation/commit/6e2d53897e5d05c7f08b8ae70531c7b757ad1518)]:
  - @yielded/sandbox@0.2.2

## 0.2.1

### Patch Changes

- Updated dependencies [[`d00f6d6`](https://github.com/yielded-dev/automation/commit/d00f6d652f7cc0b8d86c4888785a2f1e5631179e)]:
  - @yielded/sandbox@0.2.1

## 0.2.0

### Minor Changes

- [#4](https://github.com/yielded-dev/automation/pull/4) [`1bce76e`](https://github.com/yielded-dev/automation/commit/1bce76ef42fdf04f9e7132fe86124e1c316fdd2d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Compose durable investigations from read-only tools and a caller-defined result schema, or invoke them inside a consumer-owned XState workflow. Resume committed research across reporting retries and retain draft publication through recovery.

### Patch Changes

- Updated dependencies []:
  - @yielded/sandbox@0.2.0
