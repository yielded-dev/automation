import { Context } from "effect";

/** The host selects the installation; repository authority is supplied per operation. */
export class RepairGithubConfig extends Context.Service<
  RepairGithubConfig,
  { readonly githubInstallationId: string }
>()("@reve/repairs/RepairGithubConfig") {}
