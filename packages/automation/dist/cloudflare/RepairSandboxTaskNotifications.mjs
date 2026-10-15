import { Context, Schema } from "effect";
//#region src/cloudflare/RepairSandboxTaskNotifications.ts
/** A wake-up for an accepted repair run, not authority to publish it. */
const RepairSandboxTaskSettled = Schema.Struct({ 
/** The native task identity is the accepted Repairs runId. */
taskId: Schema.String.check(Schema.isUUID()) });
var RepairSandboxTaskNotificationError = class extends Schema.TaggedError()("RepairSandboxTaskNotificationError", { cause: Schema.Defect() }) {};
/** Hosts durably accept this identity, then re-read their authority through RunClient. */
var RepairSandboxTaskNotifications = class extends Context.Service()("@reve/repairs/RepairSandboxTaskNotifications") {};
//#endregion
export { RepairSandboxTaskNotificationError, RepairSandboxTaskNotifications, RepairSandboxTaskSettled };
