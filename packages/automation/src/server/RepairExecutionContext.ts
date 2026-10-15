import { Context } from "effect";
/** The authenticated run scope supplied once at an application operation boundary. */
export class RepairExecutionContext extends Context.Service<RepairExecutionContext, string>()(
  "kommunikasie/RepairExecutionContext",
) {}
