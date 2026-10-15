import { CodexErrorCode, HarnessTerminalStatus } from "../observability/FailureDiagnostic.mjs";
import { Context, Schema } from "effect";
//#region src/harness/Harness.ts
/**
* The reasoning-agent harness seam. A harness is the CLI agent (Codex today;
* other coding agents later) that executes one turn inside the session's
* sandbox. The kernel is harness-neutral: it prepares the workspace, yields
* this service for the turn itself, and persists the result. Each harness
* tags its own dialect events; the kernel forwards them untranslated.
*/
/** A harness process completed without satisfying the kernel's turn protocol. */
var HarnessTurnError = class extends Schema.TaggedError()("HarnessTurnError", {
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
		"workspaceLost"
	]),
	message: Schema.String,
	terminalStatus: Schema.optionalKey(HarnessTerminalStatus),
	code: Schema.optionalKey(CodexErrorCode)
}) {};
/**
* One reasoning harness, provided per deployment as a Layer. The container
* image must carry the harness's binary; swapping harnesses is a deploy-time
* decision, never a per-request one.
*/
var Harness = class extends Context.Service()("sandbox-agent/Harness") {};
//#endregion
export { Harness, HarnessTurnError };
