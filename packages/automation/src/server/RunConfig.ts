import { Context } from "effect";

/** The host selects its resource namespace and GitHub installation. */
export class RunConfig extends Context.Service<
  RunConfig,
  {
    readonly artifactsNamespace: string;
    readonly accountId: string;
    readonly githubInstallationId: string;
  }
>()("@reve/repairs/RunConfig") {}
