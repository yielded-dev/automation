import { RepairSandboxTaskSettled } from "./RepairSandboxTaskNotifications.mjs";
import { Schema } from "effect";
import { DurableObjectAlarm } from "effect-cf";
//#region src/cloudflare/RepairSandboxAlarms.ts
var RepairSandboxAlarms = class extends DurableObjectAlarm.Tag()("@reve/repairs/RepairSandboxAlarms", {
	run: Schema.Null,
	cleanup: Schema.Null,
	settled: RepairSandboxTaskSettled
}) {};
//#endregion
export { RepairSandboxAlarms };
