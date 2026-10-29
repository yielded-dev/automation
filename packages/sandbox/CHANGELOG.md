# @yielded/sandbox

## 0.9.0

### Minor Changes

- [#35](https://github.com/yielded-dev/automation/pull/35) [`89796c7`](https://github.com/yielded-dev/automation/commit/89796c7e9f12d59632a7239c8fed34c5ddc94e5e) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Require Effect 4.1 and effect-cf 0.59, and provide native Digest and SecureRandom services at composition roots. Upgrade the paired Cloudflare Sandbox SDK and container image to 1.0.3; rebuild host images when adopting this release.

## 0.8.0

### Minor Changes

- [#32](https://github.com/yielded-dev/automation/pull/32) [`e39ffa6`](https://github.com/yielded-dev/automation/commit/e39ffa628dbee32068a4647c8720221e775cda86) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Apply the shared Effect checking policy. GitHub App token, installation URL and bot login operations, and container identity reads, are now reusable Effects instead of zero-argument functions. Publication-content adapters use the supplied run state instead of an ambient execution context. Supply Crypto at custom composition roots; Cloudflare factories provide Web Crypto. Reject non-finite persisted deadlines and start times, and preserve selective failure recovery and scoped cleanup.

## 0.7.0

No changes in this release.

## 0.6.2

No changes in this release.

## 0.6.1

No changes in this release.

## 0.6.0

No changes in this release.

## 0.5.0

No changes in this release.

## 0.4.0

No changes in this release.

## 0.3.2

### Patch Changes

- [#16](https://github.com/yielded-dev/automation/pull/16) [`8abca50`](https://github.com/yielded-dev/automation/commit/8abca50721dd578e24848cdffea990f22fb0d322) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Allow hosts to grant previews on their application's port while keeping native control ports private.

- [#18](https://github.com/yielded-dev/automation/pull/18) [`50faba2`](https://github.com/yielded-dev/automation/commit/50faba21ac2713817ef8056e4808ccd6ed9a6548) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Reject preview access to the native Codex control port while keeping previously stored grants readable for revocation and cleanup.

## 0.3.1

### Patch Changes

- [#14](https://github.com/yielded-dev/automation/pull/14) [`ebb045e`](https://github.com/yielded-dev/automation/commit/ebb045e4991a7e7ea45f55b807ead461fcf7b95c) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Avoid stalled workspace file reads by streaming through native exec while preserving file errors and cancellation. Update the Cloudflare Sandbox SDK and matching container helper to 1.0.1.

## 0.3.0

### Minor Changes

- [#12](https://github.com/yielded-dev/automation/pull/12) [`e0c851d`](https://github.com/yielded-dev/automation/commit/e0c851dd013610d8f02c975019746fe04e3781db) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Save filesystem checkpoints without repository preparation using `checkpoint({ instanceId, sequence })`, with repeated saves per boot and a retained receipt for safe retries. Restore with `restore(checkpoint)`, which rejects unavailable checkpoints instead of opening an empty workspace; existing repository checkpoint validation and saved receipts remain supported.

## 0.2.2

### Patch Changes

- [#9](https://github.com/yielded-dev/automation/pull/9) [`2508932`](https://github.com/yielded-dev/automation/commit/2508932e85040689bef387e608f2202f128632c8) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Prepare sandbox HTTPS trust immediately and rebuild the full certificate store in the background so container readiness no longer waits for certificate hooks.

- [#11](https://github.com/yielded-dev/automation/pull/11) [`6e2d538`](https://github.com/yielded-dev/automation/commit/6e2d53897e5d05c7f08b8ae70531c7b757ad1518) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Allow hosts to enable direct public internet access when starting containers.

## 0.2.1

### Patch Changes

- [#6](https://github.com/yielded-dev/automation/pull/6) [`d00f6d6`](https://github.com/yielded-dev/automation/commit/d00f6d652f7cc0b8d86c4888785a2f1e5631179e) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Bound native process-supervisor requests, including response bodies, so an unresponsive container cannot leave workspace operations waiting indefinitely. Preserve the existing process receipts and deadlines for recovery.

## 0.2.0

No changes in this release.
