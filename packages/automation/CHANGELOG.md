# @yielded/automation

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
