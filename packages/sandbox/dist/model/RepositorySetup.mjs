import { Effect, Schema } from "effect";
//#region src/model/RepositorySetup.ts
const REPOSITORY_SETUP_MAX_BYTES = 16384;
const Command = Schema.NonEmptyString.check(Schema.isMaxLength(2048));
const Name = Schema.String.check(Schema.isPattern(/^[a-z][a-z0-9-]{0,31}$/u));
const EnvironmentName = Schema.String.check(Schema.isPattern(/^[A-Z_][A-Z0-9_]{0,63}$/u));
const Tool = Schema.String.check(Schema.isPattern(/^[a-zA-Z0-9][a-zA-Z0-9._+-]{0,63}$/u));
const InstallationInput = Schema.String.check(Schema.isMaxLength(256), Schema.isPattern(/^[a-zA-Z0-9_.*@+-]+(?:\/[a-zA-Z0-9_.*@+-]+)*$/u), Schema.makeFilter((input) => input.split("/").every((part) => part !== "." && part !== ".." && (part === "**" || !part.includes("**"))), { expected: "a root-relative Git glob without traversal or pathspec magic" }));
const RepositorySetupService = Schema.Struct({
	name: Name,
	command: Command,
	port: Schema.optionalKey(Schema.Int.check(Schema.isBetween({
		minimum: 1,
		maximum: 65535
	}), Schema.makeFilter((port) => port !== 4500 && port !== 8080, { expected: "a non-reserved port" })))
});
const RepositorySetup = Schema.Struct({
	install: Command,
	/** Opt in only with the complete tracked input closure of install and start,
	* including lifecycle scripts, their imports, manifests, locks and configuration.
	* Root-relative Git globs compare blobs, modes and paths; absent metadata stays cold. */
	installationInputs: Schema.optionalKey(Schema.NonEmptyArray(InstallationInput).check(Schema.isMaxLength(64), Schema.makeFilter((inputs) => new Set(inputs).size === inputs.length, { expected: "unique installation input globs" }))),
	start: Schema.Array(Command).check(Schema.isMaxLength(8)).pipe(Schema.withDecodingDefaultKey(Effect.succeed([]))),
	services: Schema.Array(RepositorySetupService).check(Schema.isMaxLength(4), Schema.makeFilter((services) => new Set(services.map((service) => service.name)).size === services.length, { expected: "unique service names" }), Schema.makeFilter((services) => {
		const ports = services.flatMap((service) => service.port === void 0 ? [] : [service.port]);
		return new Set(ports).size === ports.length;
	}, { expected: "unique service ports" })).pipe(Schema.withDecodingDefaultKey(Effect.succeed([]))),
	requiredSecrets: Schema.Array(EnvironmentName).check(Schema.isMaxLength(16)).pipe(Schema.withDecodingDefaultKey(Effect.succeed([]))),
	tools: Schema.Array(Tool).check(Schema.isMaxLength(16)).pipe(Schema.withDecodingDefaultKey(Effect.succeed([])))
});
const RepositorySetupStep = Schema.Struct({
	name: Schema.String.check(Schema.isMaxLength(80)),
	exitCode: Schema.NullOr(Schema.Int),
	output: Schema.String.check(Schema.isMaxLength(4096))
});
const RepositorySetupCheckpoint = Schema.Struct({
	capture: Schema.Literals([
		"disabled",
		"captured",
		"reused",
		"failed",
		"not-requested"
	]),
	publication: Schema.Literals([
		"published",
		"existing",
		"failed",
		"not-requested"
	])
});
const RepositorySetupReport = Schema.Struct({
	state: Schema.Literals([
		"absent",
		"ready",
		"failed"
	]),
	/** What this preparation actually did; copied command output is not execution evidence. */
	installation: Schema.optionalKey(Schema.Literals([
		"executed",
		"checkpoint",
		"in-place",
		"absent"
	])),
	/** Positive cache outcomes survive detached execution and sampled trace export. */
	checkpoint: Schema.optionalKey(RepositorySetupCheckpoint),
	sourceHead: Schema.String.check(Schema.isMaxLength(64)),
	recipePath: Schema.NonEmptyString.check(Schema.isMaxLength(512)),
	steps: Schema.Array(RepositorySetupStep).check(Schema.isMaxLength(32)),
	message: Schema.String.check(Schema.isMaxLength(1024))
});
const RepositorySetupPullRequest = Schema.Struct({
	sourceHead: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/u)),
	url: Schema.String.check(Schema.isPattern(/^https:\/\/github\.com\/[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+\/pull\/[1-9][0-9]*$/u))
});
const RepositorySetupReceipt = Schema.Struct({
	materializationId: Schema.String,
	fingerprint: Schema.String,
	sourceHead: Schema.String,
	/** Recovery boundary before per-boot start commands can produce retained results. */
	installationComplete: Schema.optionalKey(Schema.Boolean),
	prepared: Schema.Boolean,
	report: RepositorySetupReport
});
/** Message/report are safe diagnostics. The original cause is private and never persisted. */
var RepositorySetupError = class extends Schema.TaggedError()("RepositorySetupError", {
	message: Schema.String,
	report: RepositorySetupReport,
	/** Missing classification is uncertain, including errors from older hosts. */
	recovery: Schema.optionalKey(Schema.Literals(["terminal", "retryable"])),
	cause: Schema.optionalKey(Schema.Defect())
}) {};
//#endregion
export { REPOSITORY_SETUP_MAX_BYTES, RepositorySetup, RepositorySetupCheckpoint, RepositorySetupError, RepositorySetupPullRequest, RepositorySetupReceipt, RepositorySetupReport, RepositorySetupService, RepositorySetupStep };
