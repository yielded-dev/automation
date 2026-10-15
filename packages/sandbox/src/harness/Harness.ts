import { Context, Schema, type Effect, type JsonSchema } from "effect";

import { CodexErrorCode, HarnessTerminalStatus } from "../observability/FailureDiagnostic";

import type { SandboxAgentDeadlinePolicy, SandboxMcpConfiguration } from "../cloudflare/Config";
import type * as Sandbox from "../cloudflare/NativeSandbox";
import type { SessionStateStore, SessionStateStoreError } from "../cloudflare/SessionStateStore";
import type { HarnessDialectEvent, HarnessName, ReasoningEffort } from "../Contract";
import type { SessionSandbox } from "../SessionSandbox";

/**
 * The reasoning-agent harness seam. A harness is the CLI agent (Codex today;
 * other coding agents later) that executes one turn inside the session's
 * sandbox. The kernel is harness-neutral: it prepares the workspace, yields
 * this service for the turn itself, and persists the result. Each harness
 * tags its own dialect events; the kernel forwards them untranslated.
 */

/** A harness process completed without satisfying the kernel's turn protocol. */
export class HarnessTurnError extends Schema.TaggedError<HarnessTurnError>()("HarnessTurnError", {
  reason: Schema.Literals([
    "malformedEvent",
    "oversizedEvent",
    "missingTerminalEvent",
    "missingResumeToken",
    "nonZeroExit",
    "deadlineExceeded",
    "turnFailed",
    "freshSessionRequired",
    "connectionLost",
    "workspaceLost",
  ]),
  message: Schema.String,
  terminalStatus: Schema.optionalKey(HarnessTerminalStatus),
  code: Schema.optionalKey(CodexErrorCode),
}) {}

export type HarnessSteeringOutcome =
  | { readonly _tag: "Accepted"; readonly turnId: string }
  | { readonly _tag: "Inactive" }
  | { readonly _tag: "Rejected"; readonly reason: string }
  | { readonly _tag: "Unknown"; readonly reason: string };

/** Adds input to this execution without interrupting its active tools. */
export type HarnessTurnSteer = (input: {
  readonly id: string;
  readonly text: string;
}) => Effect.Effect<HarnessSteeringOutcome>;

/** Inputs for one fresh or resumed non-interactive harness turn. */
export interface HarnessTurnRequest<EventError = never> {
  /** Stable identity of this execution, distinct from the native thread's history. */
  readonly turnId: string;
  /** Recover this accepted logical execution through its native dispatch owner. */
  readonly recover?: boolean;
  readonly prompt: string;
  /** Opaque continuity token from a previous turn; `null` starts fresh. */
  readonly resumeToken: string | null;
  /** The model this turn runs on. */
  readonly model: string;
  readonly reasoningEffort: ReasoningEffort;
  readonly fast: boolean;
  /** Host-selected processing tier; retained in the native turn checkpoint. */
  readonly serviceTier?: "fast" | "ultrafast" | null;
  readonly deadlinePolicy: SandboxAgentDeadlinePolicy;
  /** Optional absolute host deadline, retained unchanged through process recovery. */
  readonly deadlineAtMs?: number;
  readonly restrictModelRequests?: boolean;
  /** Host-authorized read-only Sentry MCP access for a repair turn. */
  readonly sentryMcp?: SandboxMcpConfiguration;
  readonly commitCoauthorTrailer?: string;
  /** Host-selected JSON Schema for this turn's final assistant response. */
  readonly outputSchema?: JsonSchema.JsonSchema;
  /** Called for every validated dialect event, in order, already enveloped. */
  readonly onEvent: (event: HarnessDialectEvent) => Effect.Effect<void, EventError>;
  /** Available only after the harness identifies its active native turn. */
  readonly onSteeringReady?: (steer: HarnessTurnSteer) => Effect.Effect<void, EventError>;
}

/** Validated process result after a terminal event and continuity token were observed. */
export interface HarnessTurnResult {
  /** Send this back as `resumeToken` to continue the harness session. */
  readonly resumeToken: string;
  readonly terminalEventType: string;
  readonly lastAgentMessage: string | null;
  /** Harness-reported token usage, `null` when the turn reported none. */
  readonly usage: unknown;
  readonly stderrTail: string;
}

/**
 * One reasoning harness, provided per deployment as a Layer. The container
 * image must carry the harness's binary; swapping harnesses is a deploy-time
 * decision, never a per-request one.
 */
export class Harness extends Context.Service<
  Harness,
  {
    /** Discriminant copied into `agent.ready` and the run record. */
    readonly name: HarnessName;
    /** Executable name of the harness process, used to recognize (and kill)
     * stray harness processes at turn start. May differ from `name`. */
    readonly binary: string;
    /** Version pinned in the container image, available without starting a process. */
    readonly version: string;
    readonly runTurn: <EventError>(
      request: HarnessTurnRequest<EventError>,
    ) => Effect.Effect<
      HarnessTurnResult,
      EventError | HarnessTurnError | Sandbox.SandboxOperationError | SessionStateStoreError,
      SessionSandbox | SessionStateStore
    >;
  }
>()("sandbox-agent/Harness") {}
