import { Context } from "effect";

import type { NativeSandbox } from "./cloudflare/NativeSandbox";

/** One resolved native container capability for the current workspace operation. */
export class SessionSandbox extends Context.Service<SessionSandbox, NativeSandbox>()(
  "sandbox-agent/SessionSandbox",
) {}
