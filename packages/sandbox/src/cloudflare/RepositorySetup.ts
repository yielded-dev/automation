import { Cause, Clock, Effect, Exit, Option, Predicate, Redacted, Schema } from "effect";

import {
  REPOSITORY_SETUP_MAX_BYTES,
  RepositorySetup,
  RepositorySetupError,
  type RepositorySetupCheckpoint,
  type RepositorySetupReport,
  type RepositorySetupService,
  type RepositorySetupStep,
} from "../model/RepositorySetup";
import {
  repositorySetupCommandFailure,
  repositorySetupFailureCause,
} from "../model/RepositorySetupFailure";
import { reportSanitizedCause } from "../observability/RuntimeDiagnostics";

import { SessionSandbox } from "../SessionSandbox";
import { inMeasure } from "../tracing/RunSpanCapture";
import { REPO_DIR } from "../workspace-paths";
import { SandboxAgentConfig } from "./Config";
import type { ContainerIdentity } from "./NativeSandbox";
import { captureRepositoryCheckpoint, sameRepositoryCheckpointInput } from "./RepositoryCheckpoint";
import { resourceHash } from "./resource-names";
import { SessionStateStore } from "./SessionStateStore";
import type { RepositoryInstallSource } from "./RepositoryInstallAuthorization";
import {
  WorkspaceBaselineRepository,
  WORKSPACE_PREPARATION_VERSION,
  type WorkspaceBaseline,
} from "./WorkspaceBaselineRepository";
import {
  acknowledgeWorkspacePreparation,
  quiesceWorkspacePreparation,
  runRepositoryInstallation,
  runWorkspacePreparation,
} from "./WorkspacePreparation";
import {
  ensureWorkspaceServices,
  RequiredWorkspaceServices,
  TurnWorkspaceServiceEnvironment,
  TurnWorkspaceServicePorts,
  TurnWorkspaceServices,
} from "./WorkspaceProcesses";

const reportSetupFailure =
  (operation: string) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    effect.pipe(
      Effect.tapCause((cause) =>
        cause.reasons.every(
          (reason) => Cause.isFailReason(reason) && Schema.is(RepositorySetupError)(reason.error),
        )
          ? Effect.void
          : reportSanitizedCause(cause, { operation }),
      ),
    );

const COMMAND_TIMEOUT = 10 * 60_000;
const quote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`;
const reportFor = (
  recipePath: string,
  sourceHead: string,
  state: RepositorySetupReport["state"],
  message: string,
  steps: ReadonlyArray<RepositorySetupStep> = [],
): RepositorySetupReport => ({
  state,
  sourceHead,
  recipePath,
  steps,
  message,
});
const failure = (
  recipePath: string,
  sourceHead: string,
  message: string,
  steps: ReadonlyArray<RepositorySetupStep> = [],
  recovery: "terminal" | "retryable" = "retryable",
  cause?: unknown,
) =>
  RepositorySetupError.make({
    message,
    recovery,
    ...(cause === undefined ? {} : { cause }),
    report: reportFor(recipePath, sourceHead, "failed", message, steps),
  });

/** Reads a committed recipe; warm workspaces retain their preparation commit. */
export const readRepositorySetup = Effect.fn("RepositorySetup.read")(
  function* (sourceHead: string) {
    const recipePath = (yield* SandboxAgentConfig).repositorySetupPath;
    const output = yield* (yield* SessionSandbox).inspect(
      [
        "bash",
        "-euo",
        "pipefail",
        "-c",
        'entry=$(git ls-tree --name-only "$1" -- "$2"); [ -n "$entry" ] || exit 3; git show "$1:$2"',
        "repository-setup-read",
        sourceHead,
        recipePath,
      ],
      { cwd: REPO_DIR, timeout: 10_000, maxBytes: REPOSITORY_SETUP_MAX_BYTES + 1 },
    );

    // Only a successfully inspected tree with no matching path means "no recipe".
    if (output.exitCode === 3 && !output.timedOut && !output.truncated) return null;

    // Truncation also covers stderr; only the confirmed stdout prefix proves excess recipe size.
    if (
      output.exitCode === 0 &&
      !output.timedOut &&
      new TextEncoder().encode(output.stdout).length > REPOSITORY_SETUP_MAX_BYTES
    )
      return yield* failure(
        recipePath,
        "",
        "The committed setup recipe exceeds 16 KiB.",
        [],
        "terminal",
      );

    if (output.exitCode !== 0 || output.timedOut || output.truncated) {
      return yield* failure(
        recipePath,
        "",
        "The committed setup recipe could not be read completely.",
      );
    }

    return yield* Schema.decodeEffect(Schema.fromJsonString(RepositorySetup))(output.stdout, {
      onExcessProperty: "error",
    }).pipe(
      Effect.mapError((cause) =>
        failure(
          recipePath,
          "",
          "Invalid repository setup recipe: check command bounds, unique services and ports, reserved ports, and allowed fields. Secret values and env fields are not allowed.",
          [],
          "terminal",
          cause,
        ),
      ),
    );
  },
  reportSetupFailure("RepositorySetup.read"),
  Effect.catchTag("SandboxOperationError", (cause) =>
    Effect.flatMap(SandboxAgentConfig, (config) =>
      Effect.fail(
        failure(
          config.repositorySetupPath,
          "",
          "Cannot read the setup recipe; retry when the sandbox is available.",
          [],
          "retryable",
          cause,
        ),
      ),
    ),
  ),
);

export const resolveRepositorySetupEnvironment = Effect.fn("RepositorySetup.resolveEnvironment")(
  function* (sourceKey: string, recipe: RepositorySetup) {
    const config = yield* SandboxAgentConfig;
    const recipePath = config.repositorySetupPath;
    const stored = config.repositorySetupSecrets?.[sourceKey];
    const available = stored === undefined ? {} : Redacted.value(stored);
    const environment: Record<string, string> = {};

    for (const name of recipe.requiredSecrets) {
      const value = available[name];

      if (value === undefined || value.length === 0)
        return yield* failure(
          recipePath,
          "",
          `Required secret ${name} is unavailable for this repository. Configure it and retry setup.`,
          [],
          "terminal",
        );
      environment[name] = value;
    }

    return Redacted.make(environment);
  },
);

/** Secret-bearing child output is discarded before the sandbox logging boundary. */
export const repositorySetupServiceArgv = (
  service: RepositorySetupService,
  identity: string,
  secretsInjected: boolean,
): [string, ...Array<string>] => [
  "sh",
  "-c",
  `${secretsInjected ? "exec >/dev/null 2>&1; " : ""}exec sh -c ${quote(service.command)}`,
  `repository-service:${identity}:${service.name}`,
];

/** Refuses an automatic checkpoint containing configured repository secret values. */
export const assertRepositorySetupSecretsNotPersisted = Effect.fn(
  "RepositorySetup.assertSecretsNotPersisted",
)(
  function* (sourceKey: string, baseHead: string, checkpointHead = "HEAD") {
    const config = yield* SandboxAgentConfig;
    const recipePath = config.repositorySetupPath;
    const configured = config.repositorySetupSecrets?.[sourceKey];

    if (configured === undefined) return;
    const values = Object.values(Redacted.value(configured)).filter((value) => value.length > 0);

    if (values.length === 0) return;
    const environment = Object.fromEntries(
      values.map((value, index) => [`KOMMUNIKASIE_SETUP_SCAN_${index}`, value]),
    );
    const names = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Array(Schema.String)))(
      Object.keys(environment),
    );
    const sandbox = yield* SessionSandbox;
    const process = yield* sandbox
      .exec(
        [
          "sh",
          "-c",
          'exec >/dev/null 2>&1; exec node /opt/sandbox-agent/repository-secret-scan.cjs "$1" "$2" "$3"',
          "repository-secret-scan",
          names,
          baseHead,
          checkpointHead,
        ],
        { cwd: REPO_DIR, env: environment, timeout: 60_000 },
      )
      .pipe(
        reportSetupFailure("RepositorySetup.startSecretScan"),
        Effect.mapError((cause) =>
          failure(
            recipePath,
            "",
            "The repository secret scan could not start. The checkpoint was blocked; restore sandbox tools and retry.",
            [],
            "retryable",
            cause,
          ),
        ),
      );
    const exit = yield* process.outputText({ timeout: 60_000, maxBytes: 0 }).pipe(
      reportSetupFailure("RepositorySetup.waitForSecretScan"),
      Effect.catchTag("SandboxOperationError", (cause) =>
        process
          .kill()
          .pipe(
            Effect.ignore,
            Effect.andThen(
              Effect.fail(
                failure(
                  recipePath,
                  "",
                  "The repository secret scan could not finish within one minute. The checkpoint was blocked; reduce generated files and retry.",
                  [],
                  "retryable",
                  cause,
                ),
              ),
            ),
          ),
      ),
    );

    if (exit.exitCode === 1) {
      return yield* failure(
        recipePath,
        "",
        "A configured repository secret was found in repository files or outgoing Git history. Remove it from files and unpublished commits, keep runtime credentials outside Git, and retry.",
        [],
        "terminal",
      );
    }
    if (exit.exitCode !== 0) {
      return yield* failure(
        recipePath,
        "",
        "The repository secret scan could not inspect every eligible file and outgoing Git object within its bounds. The checkpoint was blocked; inspect generated files and unpublished history, then retry.",
      );
    }
  },
  Effect.catchCause((cause) =>
    Effect.flatMap(SandboxAgentConfig, (config) =>
      Effect.failCause(
        Cause.map(cause, (error) =>
          error._tag === "RepositorySetupError"
            ? error
            : failure(
                config.repositorySetupPath,
                "",
                "The repository secret scan could not encode its inputs. The checkpoint was blocked.",
                [],
                "retryable",
                error,
              ),
        ),
      ),
    ),
  ),
);

export interface RepositorySetupInputs {
  readonly sourceHead: string;
  readonly recipe: RepositorySetup | null;
  readonly fingerprint: string;
}

export interface ExecuteRepositorySetupInput {
  readonly recipe: RepositorySetup | null;
  readonly sourceKey: string;
  readonly sourceHead: string;
  readonly execution?: RepositoryInstallSource["execution"];
  readonly materializationId: string;
  readonly fingerprint: string;
  /** Clean validation starts, probes, and stops every declared service. */
  readonly services?: boolean;
  readonly installed?: RepositorySetupReport;
  /** Task preparation captures installed files before per-boot commands start writers. */
  readonly phase?: "installation" | "start";
}

export const repositorySetupFingerprint = Effect.fn("RepositorySetup.fingerprint")(function* (
  sourceHead: string,
  recipe: RepositorySetup | null,
) {
  const recipePath = (yield* SandboxAgentConfig).repositorySetupPath;
  let inputs = sourceHead;

  if (recipe?.installationInputs !== undefined) {
    // Git supplies pathspec matching and a stable record of paths, modes and blob IDs.
    // Hash in the container so a large input closure never crosses the RPC boundary.
    const result = yield* (yield* SessionSandbox).inspect(
      [
        "bash",
        "-euo",
        "pipefail",
        "-c",
        'empty_tree=$(git hash-object -w -t tree --stdin </dev/null); git diff --raw --no-abbrev --no-renames --no-ext-diff --no-textconv --ignore-submodules=none -z "$empty_tree" "$@" | git hash-object --stdin',
        "repository-setup-inputs",
        sourceHead,
        "--",
        ...recipe.installationInputs.map((path) => `:(top,glob)${path}`),
      ],
      { cwd: REPO_DIR, timeout: 30_000, maxBytes: 128 },
    );

    if (result.exitCode !== 0 || result.timedOut || result.truncated)
      return yield* failure(
        recipePath,
        sourceHead,
        "Cannot inspect the committed installation inputs.",
      );
    inputs = result.stdout.trim();
  }
  yield* Effect.annotateCurrentSpan(
    "repository.setup.inputPolicy",
    recipe?.installationInputs === undefined ? "commit" : "declared-inputs",
  );

  return yield* resourceHash(
    // oxlint-disable-next-line no-restricted-properties -- committed object identities and decoded recipe semantics form the preparation fingerprint.
    `${WORKSPACE_PREPARATION_VERSION}:${inputs}:${JSON.stringify(recipe)}`,
  );
});

export const repositorySetupCheckpointMatches = (
  baseline: WorkspaceBaseline,
  recipe: RepositorySetup | null,
  fingerprint: string,
): boolean =>
  baseline.checkpoint.setupFingerprint === fingerprint &&
  baseline.report.state !== "failed" &&
  (recipe?.requiredSecrets.length ?? 0) === 0;

/** Preparation receipts belong to the session, including isolated validation checkouts. */
export const executeRepositorySetup = Effect.fn("RepositorySetup.execute")(function* (
  input: ExecuteRepositorySetupInput,
) {
  const recipePath = (yield* SandboxAgentConfig).repositorySetupPath;
  const { recipe, sourceHead } = input;

  if (recipe === null)
    return {
      ...reportFor(recipePath, sourceHead, "absent", "No repository setup recipe is committed."),
      installation: "absent" as const,
    };
  const steps: Array<RepositorySetupStep> = [];
  const execution = Effect.gen(function* () {
    const sandbox = yield* SessionSandbox;
    const environment = yield* resolveRepositorySetupEnvironment(input.sourceKey, recipe);
    const secretsInjected = recipe.requiredSecrets.length > 0;
    const identity = yield* resourceHash(`${input.materializationId}:${input.fingerprint}`);

    for (const tool of input.phase === "start" || input.installed !== undefined
      ? []
      : recipe.tools) {
      const check = yield* sandbox.exec(["sh", "-c", `command -v ${quote(tool)} >/dev/null 2>&1`], {
        cwd: REPO_DIR,
      });
      const exit = yield* check.outputText({ timeout: 10_000, maxBytes: 0 });

      if (exit.exitCode !== 0)
        return yield* failure(
          recipePath,
          sourceHead,
          exit.exitCode > 0
            ? `Required tool ${tool} is not installed in the sandbox image. Update the image and retry.`
            : `The sandbox could not confirm whether required tool ${tool} is available. Retry setup.`,
          steps,
          exit.exitCode > 0 ? "terminal" : "retryable",
        );
    }
    const commands = [
      ...(input.phase !== "start" && input.installed === undefined
        ? [{ name: "install", command: recipe.install }]
        : []),
      ...(input.phase === "installation"
        ? []
        : recipe.start.map((command, index) => ({ name: `start-${index + 1}`, command }))),
    ];

    if (input.installed !== undefined)
      steps.push(
        ...input.installed.steps
          .filter((step) => step.name === "install")
          .map((step) =>
            input.phase === "installation"
              ? {
                  ...step,
                  output:
                    input.installed?.installation === "in-place"
                      ? "Installation already completed in this workspace."
                      : "Reused a prepared checkpoint; installation was not run.",
                }
              : step,
          ),
      );

    for (const step of commands) {
      const argv: [string, ...Array<string>] = [
        "sh",
        "-c",
        `${secretsInjected ? "exec >/dev/null 2>&1; " : ""}exec sh -c ${quote(step.command)}`,
        `repository-setup:${identity}:${step.name}`,
      ];
      const scope = `repository-setup:${identity}:${step.name}`;
      const request = {
        deadlineAt: (yield* Clock.currentTimeMillis) + COMMAND_TIMEOUT,
        command: argv,
        cwd: REPO_DIR,
        env: Redacted.value(environment),
        maxOutputBytes: 4_096,
      };
      const output = yield* (
        step.name === "install"
          ? runRepositoryInstallation(scope, request, {
              sourceKey: input.sourceKey,
              sourceHead,
              ...(input.execution === undefined ? {} : { execution: input.execution }),
            })
          : runWorkspacePreparation(scope, request)
      ).pipe(inMeasure(step.name === "install" ? "repository-install" : "repository-start"));

      // Retain successes and failures so a resumed setup cannot replay either.
      yield* acknowledgeWorkspacePreparation(`repository-setup:${identity}:${step.name}`, {
        retainReceipt: true,
      });
      steps.push({
        name: step.name,
        exitCode: output.exitCode,
        output: secretsInjected
          ? "Output withheld because repository secrets were injected."
          : `${output.stdout}${output.stderr}`.slice(-4_096),
      });
      const commandFailure = repositorySetupCommandFailure(
        reportFor(recipePath, sourceHead, "failed", "", steps),
        step.name,
        output,
      );
      if (commandFailure !== undefined) return yield* commandFailure;
    }
    if (input.services === true) {
      const handles: Array<import("./NativeSandbox").SandboxProcessHandle> = [];

      yield* Effect.gen(function* () {
        for (const service of recipe.services) {
          const handle = yield* sandbox.exec(
            repositorySetupServiceArgv(service, identity, secretsInjected),
            { cwd: REPO_DIR, env: Redacted.value(environment) },
          );

          handles.push(handle);
          const startup = yield* Effect.gen(function* () {
            if (service.port !== undefined)
              yield* handle.waitForPort(service.port, { timeout: 120_000 });
            else yield* Effect.sleep("100 millis");
            const status = yield* handle.status;

            if (status.state !== "running")
              return yield* failure(
                recipePath,
                sourceHead,
                `Service ${service.name} exited during setup validation. Fix its command and retry.`,
                steps,
              );
          }).pipe(reportSetupFailure("RepositorySetup.validateService"), Effect.result);

          if (startup._tag === "Failure") {
            yield* handle.kill().pipe(Effect.ignore);
            const output = yield* handle
              .outputText({ timeout: 5_000, maxBytes: 4_096 })
              .pipe(Effect.option);

            steps.push({
              name: `service:${service.name}`,
              exitCode: Option.isSome(output) ? output.value.exitCode : null,
              output: secretsInjected
                ? "Output withheld because repository secrets were injected."
                : Option.isSome(output)
                  ? `${output.value.stdout}${output.value.stderr}`.slice(-4_096)
                  : "Service output could not be collected.",
            });

            return yield* failure(
              recipePath,
              sourceHead,
              `Service ${service.name} failed to stay running or open its declared port. Fix its command and retry setup.`,
              steps,
              "retryable",
              startup.failure,
            );
          }
          steps.push({
            name: `service:${service.name}`,
            exitCode: null,
            output:
              service.port === undefined ? "Process is running." : `Port ${service.port} is ready.`,
          });
        }
      }).pipe(
        Effect.ensuring(
          Effect.forEach(handles, (handle) => handle.kill().pipe(Effect.catch(() => Effect.void)), {
            discard: true,
          }),
        ),
      );
    }

    yield* assertRepositorySetupSecretsNotPersisted(input.sourceKey, sourceHead);

    return {
      ...reportFor(recipePath, sourceHead, "ready", "Repository setup completed.", steps),
      installation:
        input.phase === "start"
          ? (input.installed?.installation ?? "executed")
          : input.installed === undefined
            ? "executed"
            : input.installed.installation === "in-place"
              ? "in-place"
              : "checkpoint",
    } satisfies RepositorySetupReport;
  });

  return yield* execution.pipe(
    reportSetupFailure("RepositorySetup.execute"),
    Effect.catchCause((cause) =>
      Effect.failCause(
        repositorySetupFailureCause(
          cause,
          reportFor(
            recipePath,
            sourceHead,
            "failed",
            "The setup command outcome could not be confirmed. Retry to recover its retained receipt; it will not be restarted.",
            steps,
          ),
        ),
      ),
    ),
  );
});

/** Installation/start receipts survive turn retries without touching accepted source receipts. */
export const prepareRepositorySetup = Effect.fn("RepositorySetup.prepare")(function* (input: {
  readonly sourceKey: string;
  readonly sourceHead: string;
  readonly execution?: RepositoryInstallSource["execution"];
  readonly inputs?: RepositorySetupInputs;
  readonly startPreview?: boolean;
  /** Publish pristine installation evidence without starting runtime services. */
  readonly phase?: "installation";
  readonly installed?: WorkspaceBaseline;
  readonly snapshot?: {
    readonly repository: string;
    readonly identity: ContainerIdentity;
    readonly share: boolean;
  };
}) {
  const recipePath = (yield* SandboxAgentConfig).repositorySetupPath;
  const store = yield* SessionStateStore;
  const materializationId = yield* store.materializationId.get;

  if (materializationId === null)
    return yield* failure(
      recipePath,
      input.sourceHead,
      "The repository checkout has not been materialized.",
    );
  const previous = yield* store.repositorySetupReceipt.get;
  const reuse = previous?.materializationId === materializationId && previous.prepared;
  const resumingInstallation =
    !reuse &&
    previous?.materializationId === materializationId &&
    previous.installationComplete === true;
  const sourceHead = reuse || resumingInstallation ? previous.sourceHead : input.sourceHead;
  const inputs = input.inputs?.sourceHead === sourceHead ? input.inputs : undefined;

  return yield* Effect.gen(function* () {
    const recipeResult = yield* (
      inputs === undefined ? readRepositorySetup(sourceHead) : Effect.succeed(inputs.recipe)
    ).pipe(Effect.result);

    if (recipeResult._tag === "Failure") {
      const report = { ...recipeResult.failure.report, sourceHead };

      yield* store.repositorySetupReceipt.put({
        materializationId,
        fingerprint: reuse || resumingInstallation ? previous.fingerprint : "invalid",
        sourceHead,
        installationComplete: resumingInstallation,
        prepared: reuse,
        report,
      });

      return yield* RepositorySetupError.make({
        message: report.message,
        recovery: recipeResult.failure.recovery ?? "retryable",
        cause: recipeResult.failure.cause,
        report,
      });
    }
    const recipe = recipeResult.success;
    const fingerprint =
      inputs?.fingerprint ?? (yield* repositorySetupFingerprint(sourceHead, recipe));

    if (resumingInstallation && previous.fingerprint !== fingerprint)
      return yield* failure(
        recipePath,
        sourceHead,
        "The retained installation inputs no longer match.",
      );
    const restoredInstallation =
      input.installed !== undefined &&
      repositorySetupCheckpointMatches(input.installed, recipe, fingerprint)
        ? input.installed
        : undefined;

    if (input.installed !== undefined && restoredInstallation === undefined)
      return yield* failure(
        recipePath,
        sourceHead,
        "The restored installation is incompatible; a clean container is required.",
      );
    const installed = resumingInstallation
      ? { ...previous.report, installation: "in-place" as const }
      : restoredInstallation?.report;

    yield* Effect.annotateCurrentSpan({
      "repository.setup.inputs.reused": inputs !== undefined,
      "repository.setup.installation":
        reuse || resumingInstallation
          ? "in-place"
          : recipe === null
            ? "absent"
            : installed === undefined
              ? "executed"
              : "checkpoint",
      "repository.setup.fingerprint": fingerprint,
      "repository.setup.checkpoint":
        input.installed === undefined
          ? "none"
          : restoredInstallation === undefined
            ? "inputs-changed"
            : "matched",
    });
    const snapshotInput =
      input.snapshot === undefined
        ? undefined
        : {
            instanceId: input.snapshot.identity.instanceId,
            setupFingerprint: fingerprint,
            head: sourceHead,
            materializationId,
            snapshotAllowed: (recipe?.requiredSecrets.length ?? 0) === 0,
          };
    let capture: RepositorySetupCheckpoint["capture"] =
      snapshotInput?.snapshotAllowed === false ? "disabled" : "not-requested";
    let publication: RepositorySetupCheckpoint["publication"] = "not-requested";

    if (snapshotInput) {
      const accepted = yield* store.workspaceCheckpointInput.get;

      if (
        accepted?.instanceId === snapshotInput.instanceId &&
        !sameRepositoryCheckpointInput(accepted, snapshotInput)
      )
        return yield* failure(
          recipePath,
          sourceHead,
          "This boot already owns different checkpoint inputs.",
        );
      yield* store.workspaceCheckpointInput.put(snapshotInput);
    }

    // A recipe may write injected values anywhere, beyond the Git secret scan.
    // Disable whole-filesystem capture before the first secret-bearing command.
    if (snapshotInput && !snapshotInput.snapshotAllowed) {
      yield* store.workspaceCheckpoint.delete;
    }

    const retainFailure =
      (installationComplete: boolean) =>
      <A, E, R>(effect: Effect.Effect<A, E, R>) =>
        effect.pipe(
          Effect.tapCause((cause) => {
            const reason = cause.reasons.find(
              (reason) =>
                Cause.isFailReason(reason) && Schema.is(RepositorySetupError)(reason.error),
            );
            if (
              reason === undefined ||
              !Cause.isFailReason(reason) ||
              !Schema.is(RepositorySetupError)(reason.error)
            )
              return Effect.void;
            return store.repositorySetupReceipt.put({
              materializationId,
              fingerprint,
              sourceHead,
              installationComplete,
              prepared: false,
              report: reason.error.report,
            });
          }),
        );
    if (!reuse && !resumingInstallation) yield* store.repositorySetupReceipt.delete;
    let report: RepositorySetupReport = reuse
      ? {
          ...reportFor(
            recipePath,
            sourceHead,
            recipe === null ? "absent" : "ready",
            recipe === null
              ? "No repository setup recipe was used for this workspace."
              : "Prepared workspace reused.",
            previous.report.steps,
          ),
          installation: "in-place",
        }
      : yield* executeRepositorySetup({
          ...input,
          sourceHead,
          recipe,
          materializationId,
          fingerprint,
          installed,
          phase: "installation",
        }).pipe(
          retainFailure(resumingInstallation),
          Effect.onExit((exit) =>
            Exit.isFailure(exit) ? quiesceWorkspacePreparation : Effect.void,
          ),
        );

    yield* quiesceWorkspacePreparation;
    // Installation and start are distinct recovery boundaries. In particular a
    // restored installation has no install-process receipt to consult on retry.
    // Retain it before any start command can produce an outcome that must not replay.
    if (!reuse)
      yield* store.repositorySetupReceipt.put({
        materializationId,
        fingerprint,
        sourceHead,
        installationComplete: true,
        prepared: false,
        report,
      });
    if (!reuse && snapshotInput?.snapshotAllowed) {
      // Capture before long-lived services or Codex can write private runtime data.
      yield* Effect.gen(function* () {
        capture = "failed";
        const reusableCheckpoint =
          restoredInstallation !== undefined &&
          (input.phase !== "installation" || restoredInstallation.checkpoint.head === sourceHead)
            ? restoredInstallation.checkpoint
            : undefined;
        const checkpoint =
          reusableCheckpoint ??
          (yield* captureRepositoryCheckpoint(snapshotInput).pipe(
            inMeasure("workspace-checkpoint-capture"),
          ));

        if (checkpoint === null) return;
        capture = reusableCheckpoint === undefined ? "captured" : "reused";
        yield* store.workspaceCheckpoint.put(checkpoint);
        if (input.snapshot?.share && reusableCheckpoint === undefined) {
          const baselines = yield* WorkspaceBaselineRepository;

          publication = "failed";
          const published = yield* baselines
            .publish({
              repository: input.snapshot.repository,
              checkpoint,
              report: { ...report, checkpoint: { capture, publication: "published" } },
            })
            .pipe(inMeasure("workspace-checkpoint-publish"));

          publication = published ? "published" : "existing";
        }
      }).pipe(
        Effect.catchCause((cause) => {
          if (input.phase === "installation") return Effect.failCause(cause);
          if (
            !cause.reasons.every(
              (reason) =>
                Cause.isFailReason(reason) &&
                (Predicate.isTagged(reason.error, "WorkspaceBaselineError") ||
                  Predicate.isTagged(reason.error, "SandboxOperationError")),
            )
          )
            return Effect.failCause(cause);

          return reportSanitizedCause(cause, { operation: "RepositorySetup.capture" });
        }),
      );
    }
    report = { ...report, checkpoint: { capture, publication } };
    yield* Effect.annotateCurrentSpan({
      "repository.setup.capture": capture,
      "repository.setup.publication": publication,
    });
    if (input.phase === "installation") {
      yield* store.repositorySetupReceipt.put({
        materializationId,
        fingerprint,
        sourceHead,
        installationComplete: true,
        prepared: false,
        report,
      });

      return report;
    }
    if (!reuse && recipe !== null) {
      report = yield* executeRepositorySetup({
        ...input,
        sourceHead,
        recipe,
        materializationId,
        fingerprint,
        installed: report,
        phase: "start",
      }).pipe(
        retainFailure(true),
        Effect.onExit((exit) => (Exit.isFailure(exit) ? quiesceWorkspacePreparation : Effect.void)),
      );
      report = { ...report, checkpoint: { capture, publication } };
      yield* quiesceWorkspacePreparation;
    }
    yield* store.repositorySetupReceipt.put({
      materializationId,
      fingerprint,
      sourceHead,
      prepared: true,
      report,
    });
    const environment =
      recipe === null
        ? Redacted.make({})
        : yield* resolveRepositorySetupEnvironment(input.sourceKey, recipe).pipe(
            Effect.catchTag("RepositorySetupError", (error) => {
              const failed = { ...error.report, sourceHead, steps: report.steps };

              return store.repositorySetupReceipt
                .put({ materializationId, fingerprint, sourceHead, prepared: true, report: failed })
                .pipe(
                  Effect.andThen(
                    Effect.fail(
                      RepositorySetupError.make({
                        message: failed.message,
                        recovery: error.recovery ?? "retryable",
                        cause: error.cause,
                        report: failed,
                      }),
                    ),
                  ),
                );
            }),
          );
    const identity = yield* resourceHash(`${materializationId}:${fingerprint}`);
    // SandboxTasks have a separate preview controller. Other hosts can keep the
    // declared preview in the same process lifecycle as the repository services.
    const services =
      recipe?.services.filter((service) => input.startPreview || service.port !== 5173) ?? [];
    const requested = yield* TurnWorkspaceServices;
    const declared = services.map((service) => ({
      name: service.name,
      argv: repositorySetupServiceArgv(
        service,
        identity,
        (recipe?.requiredSecrets.length ?? 0) > 0,
      ),
    }));

    yield* ensureWorkspaceServices(false).pipe(
      Effect.provideService(TurnWorkspaceServices, [
        ...declared,
        ...requested.filter(
          (service) => !recipe?.services.some((declared) => declared.name === service.name),
        ),
      ]),
      Effect.provideService(TurnWorkspaceServiceEnvironment, environment),
      Effect.provideService(
        RequiredWorkspaceServices,
        services.map((service) => service.name),
      ),
      Effect.provideService(
        TurnWorkspaceServicePorts,
        Object.fromEntries(
          services.flatMap((service) =>
            service.port === undefined ? [] : [[service.name, service.port]],
          ),
        ),
      ),
      inMeasure("repository-services"),
      reportSetupFailure("RepositorySetup.ensureServices"),
      Effect.catchTag("SandboxOperationError", (error) => {
        const failed = {
          ...report,
          ...reportFor(
            recipePath,
            sourceHead,
            "failed",
            "A repository service failed to start or its port did not become ready. Fix its command and retry setup.",
            report.steps,
          ),
        };

        return store.repositorySetupReceipt
          .put({ materializationId, fingerprint, sourceHead, prepared: true, report: failed })
          .pipe(
            Effect.andThen(
              Effect.fail(
                RepositorySetupError.make({
                  message: failed.message,
                  report: failed,
                  recovery: "retryable",
                  cause: error,
                }),
              ),
            ),
          );
      }),
      Effect.catchTag("RequiredWorkspaceServiceExited", (error) => {
        const failed = {
          ...report,
          ...reportFor(
            recipePath,
            sourceHead,
            "failed",
            `Required service ${error.name} exited during startup. Use a long-running service command or move a one-shot command to start, then retry setup.`,
            report.steps,
          ),
        };

        return store.repositorySetupReceipt
          .put({ materializationId, fingerprint, sourceHead, prepared: true, report: failed })
          .pipe(
            Effect.andThen(
              Effect.fail(
                RepositorySetupError.make({
                  message: failed.message,
                  report: failed,
                  recovery: "retryable",
                  cause: error,
                }),
              ),
            ),
          );
      }),
    );
    yield* store.repositorySetupReceipt.put({
      materializationId,
      fingerprint,
      sourceHead,
      prepared: true,
      report,
    });

    return report;
  }).pipe(
    // Once prepared, the agent must remain available to repair changed scripts
    // or service inputs. Keep failed health evidence without reinstalling.
    Effect.catchTag("RepositorySetupError", (error) =>
      Effect.gen(function* () {
        const receipt = yield* store.repositorySetupReceipt.get;

        if (
          receipt?.materializationId === materializationId &&
          receipt.sourceHead === sourceHead &&
          receipt.prepared
        ) {
          return error.report;
        }

        return yield* error;
      }),
    ),
  );
});
