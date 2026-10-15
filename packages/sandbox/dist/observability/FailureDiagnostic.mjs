import { Cause, Effect, Predicate, Schema } from "effect";
//#region src/observability/FailureDiagnostic.ts
const FailureType = Schema.Literals([
	"UnknownError",
	"Error",
	"TypeError",
	"RangeError",
	"ReferenceError",
	"SyntaxError",
	"TimeoutError",
	"SchemaError",
	"HttpClientError",
	"PlatformError",
	"SystemError",
	"SocketError",
	"WorkerError",
	"DurableAlarmError",
	"DurableObjectRpcError",
	"DurableObjectFetchError",
	"ServiceBindingRpcError",
	"ServiceBindingFetchError",
	"RpcArgumentEncodeError",
	"RpcArgumentDecodeError",
	"RpcSuccessEncodeError",
	"RpcSuccessDecodeError",
	"BindingNotFoundError",
	"BindingValidationError",
	"StorageOperationError",
	"R2OperationError",
	"SandboxOperationError",
	"ContainerError",
	"ContainerReadinessTimeoutError",
	"ContainerNotConfiguredError",
	"NativeProcessError",
	"ReadinessProcessError",
	"LeaseLostError",
	"ArtifactsOperationError",
	"ArtifactsOperationTimeoutError",
	"ArtifactsRepositoryGenerationMismatchError",
	"ForkNotReadyError",
	"RepositoryForkIntentError",
	"WorkspaceBaselinePreparationError",
	"RepositoryMutationFenceFailure",
	"GitCommandError",
	"HarnessTurnError",
	"RepositoryGitTransferFailure",
	"AbortError",
	"ConfigError"
]);
const FailureReason = Schema.Literals([
	"TransportError",
	"EncodeError",
	"InvalidUrlError",
	"StatusCodeError",
	"DecodeError",
	"EmptyBodyError",
	"SocketReadError",
	"SocketWriteError",
	"SocketOpenError",
	"SocketUpgradeError",
	"SocketCloseError",
	"ConnectionError",
	"AuthenticationError",
	"AuthorizationError",
	"UnknownError",
	"BadArgument",
	"AlreadyExists",
	"BadResource",
	"Busy",
	"InvalidData",
	"NotFound",
	"PermissionDenied",
	"TimedOut",
	"UnexpectedEof",
	"Unknown",
	"WouldBlock",
	"WriteZero",
	"Body",
	"ResponseHeaders",
	"unavailable",
	"timeout",
	"protocol",
	"write-access",
	"branch-conflict",
	"source-pin",
	"request-mismatch",
	"workspace-mismatch",
	"workspace-lost",
	"source-changed",
	"setup-failed",
	"publication-unavailable",
	"publication-conflict",
	"intent-mismatch",
	"target-mismatch",
	"stopped",
	"rejected",
	"delta-depth",
	"pack-size",
	"repack",
	"configuration",
	"authorization",
	"rate-limited",
	"provider",
	"not-configured",
	"not-authorized",
	"invalid-response",
	"response-too-large",
	"invalid",
	"expired",
	"busy",
	"closed",
	"cleanup",
	"malformed",
	"invalid-input",
	"corrupt",
	"incompatible",
	"pending",
	"denied",
	"missing-credential",
	"code-updated",
	"storage-timeout",
	"malformedEvent",
	"oversizedEvent",
	"missingTerminalEvent",
	"missingResumeToken",
	"nonZeroExit",
	"deadlineExceeded",
	"turnFailed",
	"freshSessionRequired",
	"workspaceLost",
	"connectionLost"
]);
const FailureCode = Schema.Literals([
	"ENOTFOUND",
	"EAI_AGAIN",
	"ECONNREFUSED",
	"ECONNRESET",
	"ETIMEDOUT",
	"ECONNABORTED",
	"EPIPE",
	"EHOSTUNREACH",
	"ENETUNREACH",
	"EACCES",
	"EPERM",
	"ENOENT",
	"ENOSPC",
	"EROFS",
	"EBUSY",
	"EMFILE",
	"UND_ERR_CONNECT_TIMEOUT",
	"UND_ERR_HEADERS_TIMEOUT",
	"UND_ERR_BODY_TIMEOUT",
	"UND_ERR_SOCKET",
	"PROCESS_REJECTED",
	"PROCESS_RETIRED",
	"PROCESS_UNKNOWN",
	"PROCESS_EXITED",
	"PROCESS_LOST",
	"LEASE_LOST",
	"RPC_TRANSPORT",
	"23505",
	"42P01",
	"42703",
	"40001",
	"40P01",
	"53300",
	"57014"
]);
const ArtifactsFailureCode = Schema.Literals([
	"ALREADY_EXISTS",
	"NOT_FOUND",
	"IMPORT_IN_PROGRESS",
	"FORK_IN_PROGRESS",
	"INVALID_INPUT",
	"INVALID_REPO_NAME",
	"INVALID_TTL",
	"INVALID_URL",
	"REMOTE_AUTH_REQUIRED",
	"UPSTREAM_UNAVAILABLE",
	"MEMORY_LIMIT",
	"INTERNAL_ERROR"
]);
/** Finite Codex app-server error categories; variant payloads remain private. */
const CodexErrorCode = Schema.Literals([
	"contextWindowExceeded",
	"sessionBudgetExceeded",
	"usageLimitExceeded",
	"rateLimitExceeded",
	"serverOverloaded",
	"cyberPolicy",
	"misalignmentPolicyViolation",
	"httpConnectionFailed",
	"responseStreamConnectionFailed",
	"internalServerError",
	"unauthorized",
	"badRequest",
	"threadRollbackFailed",
	"sandboxError",
	"responseStreamDisconnected",
	"responseTooManyFailedAttempts",
	"activeTurnNotSteerable",
	"other"
]);
const HarnessTerminalStatus = Schema.Literals(["failed", "interrupted"]);
const Operation = Schema.String.check(Schema.isPattern(/^[A-Za-z][A-Za-z0-9_.-]{0,127}$/));
const AuthoredClassification = Schema.Struct({
	type: Operation,
	reason: Schema.optionalKey(Operation)
});
/** Source-owned errors may declare literal names and finite schema reasons; never foreign text. */
const FailureClassification = Symbol.for("@reve/repairs/FailureClassification");
const isAuthoredClassification = Schema.is(AuthoredClassification);
const SchemaProblem = Schema.Struct({
	kind: Schema.Literals([
		"InvalidType",
		"InvalidValue",
		"MissingKey",
		"UnexpectedKey",
		"Forbidden",
		"OneOf",
		"Filter",
		"Encoding",
		"Pointer",
		"Composite",
		"AnyOf"
	]),
	field: Schema.optionalKey(Operation)
});
const HttpStatus = Schema.Int.check(Schema.isBetween({
	minimum: 100,
	maximum: 599
}));
const FailureDetail = Schema.Struct({
	type: Operation,
	operation: Schema.optionalKey(Operation),
	stage: Schema.optionalKey(Operation),
	schemaProblems: Schema.optionalKey(Schema.Array(SchemaProblem).check(Schema.isMaxLength(8))),
	retryable: Schema.optionalKey(Schema.Boolean),
	overloaded: Schema.optionalKey(Schema.Boolean),
	remote: Schema.optionalKey(Schema.Boolean),
	reason: Schema.optionalKey(Operation),
	code: Schema.optionalKey(Schema.Union([
		FailureCode,
		ArtifactsFailureCode,
		CodexErrorCode
	])),
	terminalStatus: Schema.optionalKey(HarnessTerminalStatus),
	status: Schema.optionalKey(HttpStatus),
	dispatch: Schema.optionalKey(Schema.Literals(["not-dispatched", "possibly-dispatched"])),
	cleanup: Schema.optionalKey(Schema.Literals([
		"not-requested",
		"confirmed",
		"unconfirmed"
	])),
	implementation: Schema.optionalKey(Schema.Literals([
		"cloudflare-browser-run-interactive",
		"cloudflare-browser-rest",
		"cloudflare-browser-rest-crawl"
	]))
});
const retainedDetails = /* @__PURE__ */ new WeakMap();
/** Content-free diagnostics may cross private RPC boundaries before public error translation. */
const FailureDiagnostic = Schema.Struct({
	operation: Operation,
	failures: Schema.Array(Schema.Struct({
		kind: Schema.Literals(["failure", "defect"]),
		...FailureDetail.fields,
		causes: Schema.Array(FailureDetail).check(Schema.isMaxLength(4))
	})).check(Schema.isMaxLength(16)),
	interruptions: Schema.Int.check(Schema.isBetween({
		minimum: 0,
		maximum: 16
	}))
});
const isFailureType = Schema.is(FailureType);
const isFailureReason = Schema.is(FailureReason);
const isFailureCode = Schema.is(FailureCode);
const isArtifactsFailureCode = Schema.is(ArtifactsFailureCode);
const isArtifactsOperation = Schema.is(Schema.Literals([
	"createClient",
	"create",
	"get",
	"import",
	"list",
	"delete",
	"createToken",
	"listTokens",
	"getToken",
	"revokeToken",
	"fork",
	"info",
	"log",
	"readCommit",
	"readTree",
	"cli"
]));
const isSandboxOperation = Schema.is(Schema.Literals([
	"NativeOperation.quiesce",
	"acknowledgeProcess",
	"allocateProcess",
	"backupDirectory",
	"capacity.release",
	"capacity.reserve",
	"checkpoint",
	"configureOutboundAuthority",
	"deleteDirectoryBackup",
	"describe",
	"destroy",
	"ensure",
	"exec",
	"exists",
	"forwardPort",
	"forwardPreview",
	"grantPreview",
	"lease",
	"lstat",
	"mkdir",
	"park",
	"pinImage",
	"preview.grant",
	"preview.revoke",
	"previewTokenHash",
	"process",
	"process.wait",
	"processes",
	"readDirectory",
	"readFile",
	"readFileString",
	"remove",
	"resolvePreview",
	"restoreDirectory",
	"revokePreview",
	"setRepositoryAuthorization",
	"signalProcess",
	"waitForHttp",
	"waitForPort",
	"waitProcess",
	"writeFile"
]));
const isSupervisorOperation = Schema.is(Schema.Literals([
	"acknowledgeJob",
	"allocateTicket",
	"launchJob",
	"lease",
	"listJobs",
	"observeJob",
	"park",
	"protect",
	"signalJob",
	"touch",
	"waitForPort",
	"waitJob"
]));
const isHttpStatus = Schema.is(HttpStatus);
const isCodexErrorCode = Schema.is(CodexErrorCode);
const isHarnessTerminalStatus = Schema.is(HarnessTerminalStatus);
const isR2Operation = Schema.is(Schema.Literals([
	"head",
	"get",
	"put",
	"delete",
	"list",
	"createMultipartUpload",
	"resumeMultipartUpload",
	"uploadPart",
	"abortMultipartUpload",
	"completeMultipartUpload",
	"arrayBuffer",
	"bytes",
	"text",
	"json",
	"blob"
]));
const isImplementation = Schema.is(FailureDetail.fields.implementation);
const isDispatch = Schema.is(FailureDetail.fields.dispatch);
const isCleanup = Schema.is(FailureDetail.fields.cleanup);
const isOperation = Schema.is(FailureDiagnostic.fields.operation);
const isStorageOperation = Schema.is(Schema.Literals([
	"get",
	"put",
	"delete",
	"deleteAll",
	"list",
	"transaction",
	"transactionSync",
	"getAlarm",
	"setAlarm",
	"deleteAlarm",
	"alarm.schedule",
	"alarm.transaction",
	"sync",
	"getCurrentBookmark",
	"getBookmarkForTime",
	"onNextSessionRestoreBookmark",
	"kv.get",
	"kv.put",
	"kv.delete",
	"kv.list",
	"sql.exec",
	"sql.toArray",
	"sql.one",
	"sql.next",
	"sql.raw",
	"sql.rowsRead",
	"sql.rowsWritten"
]));
const detail = (error) => {
	const retained = Predicate.isObject(error) ? retainedDetails.get(error) : void 0;
	if (retained !== void 0) return retained;
	if (!Predicate.isObject(error)) return { type: "UnknownError" };
	const annotation = FailureClassification in error ? error[FailureClassification] : void 0;
	const authored = isAuthoredClassification(annotation) ? annotation : void 0;
	const type = authored?.type ?? ("_tag" in error && isFailureType(error._tag) ? error._tag : "name" in error && isFailureType(error.name) ? error.name : "UnknownError");
	const rawReason = "reason" in error ? error.reason : void 0;
	const storageCause = type === "StorageOperationError" && "cause" in error ? error.cause : void 0;
	const storageReason = storageCause instanceof Error ? storageCause.message === "Durable Object reset because its code was updated." ? "code-updated" : storageCause.message === "Durable Object storage operation exceeded timeout which caused object to be reset." ? "storage-timeout" : void 0 : void 0;
	const reason = authored?.reason ?? (isFailureReason(rawReason) ? rawReason : Predicate.isObject(rawReason) && "_tag" in rawReason && isFailureReason(rawReason._tag) ? rawReason._tag : "kind" in error && isFailureReason(error.kind) ? error.kind : storageReason);
	const http = "http" in error ? error.http : Predicate.isObject(rawReason) && "http" in rawReason ? rawReason.http : void 0;
	const response = Predicate.isObject(rawReason) && "response" in rawReason ? rawReason.response : "response" in error ? error.response : Predicate.isObject(http) && "response" in http ? http.response : void 0;
	const data = type === "HttpError" && "data" in error ? error.data : void 0;
	const status = Predicate.isObject(response) && "status" in response && isHttpStatus(response.status) ? response.status : "status" in error && isHttpStatus(error.status) ? error.status : Predicate.hasProperty(data, "statusCode") && isHttpStatus(data.statusCode) ? data.statusCode : void 0;
	const implementation = "implementation" in error && Predicate.isObject(error.implementation) && "identity" in error.implementation && isImplementation(error.implementation.identity) ? error.implementation.identity : void 0;
	const code = "code" in error && (isFailureCode(error.code) || type === "ArtifactsOperationError" && isArtifactsFailureCode(error.code) || type === "HarnessTurnError" && isCodexErrorCode(error.code)) ? error.code : void 0;
	const operation = "operation" in error && (type === "ArtifactsOperationError" && isArtifactsOperation(error.operation) || type === "SandboxOperationError" && isSandboxOperation(error.operation) || type === "NativeProcessError" && isSupervisorOperation(error.operation) || type === "StorageOperationError" && isStorageOperation(error.operation) || type === "R2OperationError" && isR2Operation(error.operation)) ? error.operation : void 0;
	return {
		type,
		...operation === void 0 ? {} : { operation },
		..."retryable" in error && typeof error.retryable === "boolean" ? { retryable: error.retryable } : {},
		..."overloaded" in error && typeof error.overloaded === "boolean" ? { overloaded: error.overloaded } : {},
		..."remote" in error && typeof error.remote === "boolean" ? { remote: error.remote } : {},
		...reason === void 0 ? {} : { reason },
		...status === void 0 ? {} : { status },
		...code === void 0 ? {} : { code },
		...type === "HarnessTurnError" && "terminalStatus" in error && isHarnessTerminalStatus(error.terminalStatus) ? { terminalStatus: error.terminalStatus } : {},
		...implementation === void 0 ? {} : { implementation },
		...type === "BrowserSessionError" && "dispatch" in error && isDispatch(error.dispatch) ? { dispatch: error.dispatch } : {},
		...type === "BrowserSessionError" && "cleanup" in error && isCleanup(error.cleanup) ? { cleanup: error.cleanup } : {}
	};
};
const schemaProblems = (error, fields) => {
	const problems = [];
	const pending = [{ issue: error.issue }];
	for (let visited = 0; pending.length > 0 && problems.length < 8 && visited < 64; visited++) {
		const { issue, field } = pending.shift();
		switch (issue._tag) {
			case "Pointer": {
				const first = issue.path[0];
				const selected = typeof first === "string" && fields.includes(first) && isOperation(first) ? first : null;
				pending.unshift({
					issue: issue.issue,
					field: field === void 0 ? selected : field
				});
				break;
			}
			case "Encoding":
			case "Filter":
				pending.unshift({
					issue: issue.issue,
					field
				});
				break;
			case "Composite":
			case "AnyOf":
				if (issue.issues.length > 0) {
					pending.unshift(...issue.issues.slice(0, 8).map((issue) => ({
						issue,
						field
					})));
					break;
				}
				problems.push({
					kind: issue._tag,
					...typeof field === "string" ? { field } : {}
				});
				break;
			default: problems.push({
				kind: issue._tag,
				...typeof field === "string" ? { field } : {}
			});
		}
	}
	return problems;
};
/** Retain the innermost source-authored stage on the original error; reporting remains the owner's job. */
const withFailureDiagnostic = (self, options) => self.pipe(Effect.tapCause((cause) => Effect.sync(() => {
	for (const reason of cause.reasons) {
		if (Cause.isInterruptReason(reason)) continue;
		const error = Cause.isFailReason(reason) ? reason.error : reason.defect;
		if (!Predicate.isObject(error) || retainedDetails.get(error)?.stage !== void 0) continue;
		retainedDetails.set(error, {
			...detail(error),
			stage: isOperation(options.operation) ? options.operation : "unknown.operation",
			...Schema.isSchemaError(error) ? { schemaProblems: schemaProblems(error, options.schemaFields ?? []) } : {}
		});
	}
})));
const nestedCause = (error) => {
	if (!Predicate.isObject(error)) return void 0;
	if ("cause" in error && error.cause !== void 0) return error.cause;
	return "reason" in error && Predicate.isObject(error.reason) ? error.reason : void 0;
};
const failureDiagnostic = (cause, options) => ({
	operation: isOperation(options.operation) ? options.operation : "unknown.operation",
	failures: [...cause.reasons].sort((a, b) => Number(Cause.isDieReason(b)) - Number(Cause.isDieReason(a))).flatMap((reason) => {
		if (Cause.isInterruptReason(reason)) return [];
		const error = Cause.isFailReason(reason) ? reason.error : reason.defect;
		const causes = [];
		const seen = /* @__PURE__ */ new Set([error]);
		const pending = [nestedCause(error)];
		while (pending.length > 0 && causes.length < 4 && seen.size < 16) {
			const current = pending.shift();
			if (current === void 0 || seen.has(current)) continue;
			seen.add(current);
			if (Cause.isCause(current)) {
				pending.unshift(...[...current.reasons].sort((a, b) => Number(Cause.isDieReason(b)) - Number(Cause.isDieReason(a))).flatMap((reason) => Cause.isInterruptReason(reason) ? [] : [Cause.isFailReason(reason) ? reason.error : reason.defect]).slice(0, 4 - causes.length));
				continue;
			}
			causes.push(detail(current));
			pending.unshift(nestedCause(current));
		}
		return [{
			kind: Cause.isFailReason(reason) ? "failure" : "defect",
			...detail(error),
			causes
		}];
	}).slice(0, 16),
	interruptions: Math.min(cause.reasons.filter(Cause.isInterruptReason).length, 16)
});
const diagnosticError = (operation, detail, nested) => {
	const suffix = [
		...detail.stage !== void 0 && detail.operation !== void 0 ? [`operation=${detail.operation}`] : [],
		detail.reason,
		...detail.terminalStatus === void 0 ? [] : [`terminalStatus=${detail.terminalStatus}`],
		detail.code,
		detail.status,
		detail.implementation,
		detail.dispatch,
		detail.cleanup,
		...[
			"retryable",
			"overloaded",
			"remote"
		].flatMap((key) => {
			const value = detail[key];
			return value === void 0 ? [] : [`${key}=${value}`];
		}),
		...(detail.schemaProblems ?? []).map((problem) => `${problem.kind}${problem.field === void 0 ? "" : `:${problem.field}`}`)
	].filter((value) => value !== void 0).join("; ");
	const error = new Error(`${detail.stage ?? detail.operation ?? operation} failed${suffix.length === 0 ? "" : ` (${suffix})`}`, nested === void 0 ? void 0 : { cause: nested });
	error.name = detail.type;
	error.stack = `${error.name}: ${error.message}`;
	retainedDetails.set(error, {
		...detail,
		operation: detail.operation ?? operation
	});
	return error;
};
/** Reconstructs only approved diagnostic fields, never a remotely supplied Error or stack. */
const causeFromFailureDiagnostic = (diagnostic) => Cause.fromReasons([...diagnostic.failures.slice(0, 16).map((failure) => {
	const nested = failure.causes.slice(0, 4).reduceRight((cause, detail) => diagnosticError(diagnostic.operation, detail, cause), void 0);
	const error = diagnosticError(diagnostic.operation, failure, nested);
	return failure.kind === "failure" ? Cause.makeFailReason(error) : Cause.makeDieReason(error);
}), ...Array.from({ length: Math.min(diagnostic.interruptions, 16) }, () => Cause.makeInterruptReason())]);
/** Drops error values and annotations; bounded diagnostics retain defects before typed failures. */
const sanitizeCause = (cause, options) => causeFromFailureDiagnostic(failureDiagnostic(cause, options));
/** Promotes sanitized failures without dropping concurrent defects or interruptions. */
const sanitizeDefectCause = (cause, options) => Cause.fromReasons(sanitizeCause(cause, options).reasons.map((reason) => Cause.isFailReason(reason) ? Cause.makeDieReason(reason.error) : reason));
//#endregion
export { CodexErrorCode, FailureClassification, FailureDiagnostic, HarnessTerminalStatus, causeFromFailureDiagnostic, failureDiagnostic, sanitizeCause, sanitizeDefectCause, withFailureDiagnostic };
