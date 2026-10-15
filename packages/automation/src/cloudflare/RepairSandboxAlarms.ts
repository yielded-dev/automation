import { Schema } from "effect";
import { DurableObjectAlarm } from "effect-cf";
import { RepairSandboxTaskSettled } from "./RepairSandboxTaskNotifications";

export class RepairSandboxAlarms extends DurableObjectAlarm.Tag<RepairSandboxAlarms>()(
  "@reve/repairs/RepairSandboxAlarms",
  { run: Schema.Null, cleanup: Schema.Null, settled: RepairSandboxTaskSettled },
) {}
