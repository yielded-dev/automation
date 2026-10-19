---
"@yielded/automation": minor
"@yielded/sandbox": minor
---

Save filesystem checkpoints without repository preparation using `checkpoint({ instanceId, sequence })`, with repeated saves per boot and a retained receipt for safe retries. Restore with `restore(checkpoint)`, which rejects unavailable checkpoints instead of opening an empty workspace; existing repository checkpoint validation and saved receipts remain supported.
