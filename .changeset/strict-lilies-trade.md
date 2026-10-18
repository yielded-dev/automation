---
"@yielded/automation": patch
---

Fix repair activity pages failing when they contain a truncated-output marker. Runtime event payloads now retain their JSON values, including null, while Codex events still require JSON object payloads.
