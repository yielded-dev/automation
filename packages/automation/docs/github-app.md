# GitHub App setup

Generate the registration payload with `githubAppManifest` from
`@yielded/automation/github`, supplying HTTPS URLs, App name, visibility and the
`publishChanges` choice. The discussion profile requests Contents read, Issues
write, Pull requests read, Checks read and Commit statuses read. Publication adds
Contents/Pull requests write. Workflow-file editing is a separate opt-in; Actions,
Deployments and Administration write are not requested.

The authenticated host setup route starts GitHub's
[manifest registration flow](https://docs.github.com/en/apps/sharing-github-apps/registering-a-github-app-from-a-manifest).
Bind `state` to the initiating administrator/account and verify it before calling
`exchangeGithubAppManifest`. Save the returned registration and credentials in
the host's secret store. Do not automatically retry an ambiguous one-use exchange.

GitHub returns an RSA PEM key; `GithubAppAuth.layer` takes base64 PKCS#8 DER.
Normalize it in the host's protected setup environment, import it into the secret
store, then remove temporary key files:

```sh
umask 077
credential_dir="$(mktemp -d)"
openssl pkcs8 -topk8 -nocrypt -in app-private-key.pem -outform DER |
  openssl base64 -A > "$credential_dir/private-key"
```

An installation callback cannot grant application authority. The authenticated
host administrator selects the registered App, installation and repositories.
Verify those resources through `GithubAppAuth.repositoryInstallation` before
saving the host's account association. Configure App auth and the existing
`RepairGithubClient` at the operation's composition root. Tokens are restricted
to the selected repository and operation. Credentials remain outside accepted
launches, evidence and public errors.

`GithubAppAuth.installationToken`, `installationUrl` and `botLogin` are lazy,
reusable Effects. Yield the property directly, for example
`yield* auth.installationToken`, without calling it as a function.

`readGithubWebhook(request, secret)` verifies the bounded raw body and decodes
PR/comment, push, check and status events. Unsupported events return null.
The host filters the saved repository and installation, then durably enqueues a
wake before responding. Intake makes no GitHub API calls and grants no authority.
Installation changes do not provision tenants; the host manages its registered
resources and rechecks current authority before actions.

Use `prepareGithubFollowUp` from `@yielded/automation/server/GithubFollowUps` inside that
existing queue. Provide
`GithubFollowUpHost` for saved publication/current records, observations and
atomic acceptance, plus `GithubFollowUpConfig` for instructions, model and the
existing identity/policy names. Healthy or unrelated events only update observed
status. Eligible feedback/checks/conflicts produce a continuation only after
exact-head checks and current host acceptance. Dispatch the returned original
launch with the existing `RunClient` path. Acceptance deduplicates by attempt ID
and retains the first deadline; publication reconciles uncertain writes against
its existing intent and receipt. No additional webhook, action or reply journal
is required. Standalone issue-answer orchestration is not supplied.

Installing this package does not register or repoint a production App, alter its
grants or migrate installations. Provision those resources explicitly. Preserve
old credentials/routes while retained work still needs them. A webhook or model
response cannot choose a different tenant, grant write authority, merge or deploy.
