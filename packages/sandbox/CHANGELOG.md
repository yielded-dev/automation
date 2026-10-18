# @yielded/sandbox

## 0.2.2

### Patch Changes

- [#9](https://github.com/yielded-dev/automation/pull/9) [`2508932`](https://github.com/yielded-dev/automation/commit/2508932e85040689bef387e608f2202f128632c8) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Prepare sandbox HTTPS trust immediately and rebuild the full certificate store in the background so container readiness no longer waits for certificate hooks.

- [#11](https://github.com/yielded-dev/automation/pull/11) [`6e2d538`](https://github.com/yielded-dev/automation/commit/6e2d53897e5d05c7f08b8ae70531c7b757ad1518) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Allow hosts to enable direct public internet access when starting containers.

## 0.2.1

### Patch Changes

- [#6](https://github.com/yielded-dev/automation/pull/6) [`d00f6d6`](https://github.com/yielded-dev/automation/commit/d00f6d652f7cc0b8d86c4888785a2f1e5631179e) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Bound native process-supervisor requests, including response bodies, so an unresponsive container cannot leave workspace operations waiting indefinitely. Preserve the existing process receipts and deadlines for recovery.

## 0.2.0

No changes in this release.
