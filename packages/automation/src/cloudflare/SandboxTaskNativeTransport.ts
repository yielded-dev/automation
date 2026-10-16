import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";

import { Clock, Effect, Match, Schema } from "effect";
import { DurableObjectState } from "effect-cf";
import { Hex } from "effect/encoding";
import { SqlClient } from "effect/sql";

import { ConflictError } from "../domain/errors";
import type { SandboxTaskMachineCreate } from "../server/sandbox-contracts/sandbox-task-machine";
import {
  SandboxTaskExecutionSnapshot,
  SandboxTaskNativeRequest,
} from "../server/sandbox-contracts/sandbox-task-native";
import {
  ArtifactsCheckout,
  SessionSandbox,
  SessionStateStore,
  MATERIALIZATION_MARKER,
  ProcessStatus,
} from "@yielded/sandbox/cloudflare";

import { NativeOperationResultJson } from "./SandboxTaskNativeResults";
import { readSandboxTaskPrivateRepository } from "./RepairSandboxTaskRepository";

const hash = (text: string) =>
  Effect.promise(async () =>
    Hex.encode(
      new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text))),
    ),
  );
const Receipt = Schema.Struct({
  fingerprint: Schema.String,
  processId: Schema.NullOr(Schema.String),
  instanceId: Schema.NullOr(Schema.String),
  completed: Schema.Boolean,
  result: Schema.NullOr(Schema.Json),
});
const ReceiptJson = Schema.toCodecJson(Receipt);
const Forward = Schema.Union([
  Schema.TaggedStruct("File", {
    path: Schema.String,
    access: Schema.Literals(["read", "write"]),
    nativeTokenHash: Schema.String,
    expectedInstanceId: Schema.String,
  }),
  Schema.TaggedStruct("Gateway", {
    port: Schema.Literal(4502),
    nativeOperationId: Schema.String,
    nativeTokenHash: Schema.String,
    expectedInstanceId: Schema.String,
    materializationId: Schema.String,
  }),
]);
const ForwardJson = Schema.toCodecJson(Forward);
const conflict = (message: string) => ConflictError.make({ message });
/** Read-only execution pointers; callers never become a second authority for these facts. */
export const readSandboxTaskExecutionSnapshot = Effect.fnUntraced(function* (
  admission: SandboxTaskMachineCreate,
) {
  const store = yield* SessionStateStore;
  const session = yield* store.snapshot;
  const pipeline = yield* store.pipelineTurn.get;
  const codexTurn = yield* store.codexTurn.get;

  return yield* Schema.encodeEffect(Schema.toCodecJson(SandboxTaskExecutionSnapshot))({
    repositoryName:
      admission.source._tag === "AttachedRepository"
        ? admission.source.repositoryName
        : session.forkName,
    verifiedHead: session.verifiedHead,
    materializationId: session.materializationId,
    containerSize: session.containerSize,
    pipeline:
      pipeline === null
        ? null
        : {
            id: pipeline.id,
            ...(pipeline.instanceId === undefined ? {} : { instanceId: pipeline.instanceId }),
            materializationId: pipeline.materializationId,
            head: pipeline.head,
            baseHead: pipeline.baseHead,
            persisted: pipeline.persisted,
          },
    codexTurn:
      codexTurn === null
        ? null
        : {
            id: codexTurn.id,
            result:
              codexTurn.result === null
                ? null
                : {
                    terminalEventType: codexTurn.result.terminalEventType,
                    lastAgentMessage: codexTurn.result.lastAgentMessage,
                  },
          },
  });
});

/** The owning DO selects the fixed purpose sandbox, holds its permit and reserves capacity first. */
export const runNativeOperation = Effect.fn("SandboxTaskNativeTransport.run")(function* (
  admission: SandboxTaskMachineCreate,
  request: SandboxTaskNativeRequest,
) {
  const action = request.action;
  const isObservation = [
    "Describe",
    "Process",
    "WaitProcess",
    "WaitPort",
    "WaitHttp",
    "Processes",
    "ReadFileString",
    "Exists",
    "Lstat",
    "ReadDirectory",
    "JournalGet",
  ].includes(action._tag);

  if (
    !isObservation &&
    !(action._tag === "PrepareFile" && action.access === "read") &&
    action._tag !== "Destroy" &&
    action._tag !== "Park" &&
    action._tag !== "ClearRepositoryAuthorization" &&
    action._tag !== "AcknowledgeProcess" &&
    (admission.deadlineAtMillis === undefined ||
      (yield* Clock.currentTimeMillis) >= admission.deadlineAtMillis)
  )
    return yield* conflict("Native execution authority expired");
  const object = yield* DurableObjectState.DurableObjectState;
  const sandbox = yield* SessionSandbox;
  const description = yield* sandbox.describe;

  if (
    request.expectedInstanceId !== undefined &&
    description.current?.instanceId !== request.expectedInstanceId
  )
    return yield* conflict("The request belongs to a different native incarnation");
  if (
    ["ReadFileString", "Exists", "Lstat", "ReadDirectory", "PrepareFile"].includes(action._tag) &&
    !description.running
  )
    return yield* conflict("Native filesystem observation requires its running incarnation");

  if (action._tag === "JournalGet")
    return (
      (yield* object.storage.get<Schema.Json>(`repair-helper:${request.purpose}:${action.key}`)) ??
      null
    );
  const fingerprint = yield* hash(
    yield* Schema.encodeEffect(Schema.fromJsonString(SandboxTaskNativeRequest))(request),
  );
  const key = `native-transport:${request.purpose}:${request.operationId}`;
  const stored = yield* object.storage.get<Schema.Json>(key);
  let receipt = stored === undefined ? undefined : yield* Schema.decodeEffect(ReceiptJson)(stored);

  if (receipt !== undefined && receipt.fingerprint !== fingerprint)
    return yield* conflict("The native operation id already has different inputs");
  if (receipt?.completed === true) return receipt.result;
  if (!isObservation) {
    if (receipt === undefined) {
      const owner = yield* sandbox.describe;

      receipt = {
        fingerprint,
        processId: null,
        instanceId: owner.current?.instanceId ?? null,
        completed: false,
        result: null,
      };
      yield* object.storage.put(key, yield* Schema.encodeEffect(ReceiptJson)(receipt));
    } else if (
      receipt.instanceId === null &&
      action._tag !== "Park" &&
      action._tag !== "JournalPut"
    ) {
      return yield* conflict("The previous native dispatch has no recoverable incarnation receipt");
    }
    // Coordination records belong to the task DO and outlive container incarnations.
    if (action._tag !== "JournalPut") {
      const owner = yield* sandbox.describe;

      if (
        owner.current?.instanceId !== receipt.instanceId &&
        !(owner.current === null && receipt.instanceId === null)
      )
        return yield* conflict("The native mutation belongs to an earlier incarnation");
      // Parking a helper that never booted is still valid cleanup.
      if (
        action._tag !== "Ensure" &&
        action._tag !== "Destroy" &&
        action._tag !== "Park" &&
        receipt.instanceId === null
      )
        return yield* conflict("Native workspace mutation requires an admitted incarnation");
    }
  }
  if (action._tag === "Process" || action._tag === "WaitProcess") {
    const maxBytes = Math.min(action.maxBytes, 1_000_000);
    const sql = (yield* SqlClient.SqlClient).withoutTransforms();
    const readTerminal = () => sql<{ metadata: string; stdout: string; stderr: string }>`
      SELECT metadata, stdout, stderr FROM sandbox_task_native_terminal
      WHERE purpose = ${request.purpose} AND process_id = ${action.processId}`;
    const decodeTerminal = (retained: { metadata: string; stdout: string; stderr: string }) =>
      Schema.decodeEffect(Schema.fromJsonString(ProcessStatus))(retained.metadata).pipe(
        Effect.map((metadata) => ({
          ...metadata,
          stdout: retained.stdout,
          stderr: retained.stderr,
        })),
      );
    let retained = (yield* readTerminal())[0];
    let observed =
      retained === undefined
        ? yield* sandbox.observeProcess(action.processId, {
            maxBytes,
            waitMillis: action._tag === "WaitProcess" ? Math.min(action.waitMillis, 20_000) : 0,
          })
        : yield* decodeTerminal(retained);

    // Running observations honor the caller's byte limit. Once terminal, capture
    // the canonical output before retaining the result or permitting acknowledgement.
    if (retained === undefined && observed?.state === "exited" && maxBytes < 1_000_000)
      observed = yield* sandbox.observeProcess(action.processId, {
        maxBytes: 1_000_000,
        waitMillis: 0,
      });

    if (observed === null) {
      // Another observer may have retained and acknowledged the native bytes during this read.
      retained = (yield* readTerminal())[0];
      if (retained === undefined) return null;
      observed = yield* decodeTerminal(retained);
    }
    if (retained === undefined && observed.state === "exited") {
      const metadata = yield* Schema.encodeEffect(Schema.fromJsonString(ProcessStatus))({
        ...observed,
        stdout: "",
        stderr: "",
      });

      // Retain the canonical bounded bytes before any caller can acknowledge the native result.
      // Even a metadata-only observation must not freeze an empty terminal receipt.
      yield* sql`INSERT INTO sandbox_task_native_terminal
        (purpose, process_id, metadata, stdout, stderr)
        VALUES (${request.purpose}, ${action.processId}, ${metadata}, ${observed.stdout}, ${observed.stderr})
        ON CONFLICT (purpose, process_id) DO NOTHING`;
    }
    if (maxBytes === 0)
      return yield* Schema.encodeEffect(NativeOperationResultJson)({
        ...observed,
        stdout: "",
        stderr: "",
        truncated: observed.truncated || observed.stdout !== "" || observed.stderr !== "",
      });
    const stdout = Buffer.from(observed.stdout);
    const stderr = Buffer.from(observed.stderr);

    return yield* Schema.encodeEffect(NativeOperationResultJson)({
      ...observed,
      stdout: stdout.subarray(0, maxBytes).toString(),
      stderr: stderr.subarray(0, maxBytes).toString(),
      truncated: observed.truncated || stdout.length > maxBytes || stderr.length > maxBytes,
    });
  }
  const result = yield* Match.value(action)
    .pipe(
      Match.tag("Describe", () => sandbox.describe),
      Match.tag("JournalPut", (action) =>
        Effect.gen(function* () {
          if (request.purpose === "turn")
            return yield* conflict("Application cannot alter primary execution journals");
          const journalKey = `repair-helper:${request.purpose}:${action.key}`;

          if (action.expectedValue !== undefined) {
            const current = (yield* object.storage.get<Schema.Json>(journalKey)) ?? null;

            if (!Schema.toEquivalence(Schema.Json)(current, action.expectedValue)) return false;
          }
          yield* object.storage.put(journalKey, action.value);

          return action.expectedValue === undefined ? null : true;
        }),
      ),
      Match.tag("PrepareRepository", "EnsureCommit", (action) =>
        Effect.gen(function* () {
          if (
            (admission.source._tag !== "AttachedRepository" &&
              admission.source._tag !== "SeedRepository") ||
            (request.purpose !== "validation" && request.purpose !== "export")
          )
            return yield* conflict("Repository preparation requires private helper authority");
          const savedHead = yield* (yield* SessionStateStore).verifiedHead.get;

          if (
            action.head !== savedHead &&
            action.head !== admission.source.commitSha &&
            !admission.requiredCommits?.some((sha) => sha === action.head)
          )
            return yield* conflict(
              "The helper revision is outside its frozen repository authority",
            );
          const original = yield* SessionStateStore;
          const owner = yield* sandbox.describe;

          if (owner.current === null)
            return yield* conflict("The helper incarnation is unavailable");
          const source = yield* readSandboxTaskPrivateRepository(admission);
          const seeded = admission.source._tag === "SeedRepository";
          const namespace = `repair-helper:${request.purpose}:${owner.current.instanceId}${
            seeded && action._tag === "PrepareRepository" ? `:${action.scope}` : ""
          }`;

          yield* Effect.gen(function* () {
            const checkout = yield* ArtifactsCheckout;

            if (action._tag === "PrepareRepository") {
              const scope = `repair-helper:${request.purpose}:${action.scope}`;

              if (action.resetReceipt)
                yield* checkout.discardCloneReceipt(seeded ? "workspace" : scope);
              const cloned = yield* seeded
                ? checkout.cloneGenerationIntoSandbox({
                    repositoryName: source.repositoryName,
                    repositoryId: source.artifactsRepositoryId,
                  })
                : checkout.cloneIntoSandbox(source.repositoryName, scope);

              yield* checkout.ensureCommit(source.repositoryName, action.head, {
                includeHistory: true,
                repositoryId: source.artifactsRepositoryId,
              });
              yield* checkout.restoreSnapshotCheckout(
                source.repositoryName,
                action.head,
                cloned.head,
              );
            } else {
              yield* checkout.ensureCommit(source.repositoryName, action.head, {
                includeHistory: true,
                repositoryId: source.artifactsRepositoryId,
              });
            }
          }).pipe(
            Effect.provideService(SessionStateStore, {
              ...original,
              workspacePreparation: (scope) =>
                original.workspacePreparation(`${namespace}:${scope}`),
              preparationListener: original.preparationListenerFor(namespace),
            }),
          );

          return null;
        }),
      ),
      Match.tag("AllocateProcess", ({ resource }) => sandbox.allocateProcess(resource)),
      Match.tag("Lstat", ({ path }) =>
        sandbox.lstat(path).pipe(
          Effect.map((stat) => ({
            ...stat,
            size: String(stat.size),
            accessedAt: stat.accessedAt.getTime(),
            modifiedAt: stat.modifiedAt.getTime(),
            changedAt: stat.changedAt.getTime(),
          })),
        ),
      ),
      Match.tag("ReadDirectory", ({ path }) => sandbox.readDirectory(path)),
      Match.tag("Lease", ({ id, active, renewal }) => sandbox.lease(id, active, renewal)),
      Match.tag("Park", () => sandbox.park),
      Match.tag("Ensure", ({ start }) => sandbox.ensure(start)),
      Match.tag("Exec", ({ argv, options }) =>
        Effect.gen(function* () {
          if (
            admission.deadlineAtMillis === undefined ||
            options.deadlineAt > admission.deadlineAtMillis
          )
            return yield* conflict("The command exceeds its admitted deadline");
          if (receipt === undefined) return yield* conflict("The command has no dispatch intent");
          if (receipt.processId === null) {
            const owner = yield* sandbox.ensure();
            const processId = options.processId ?? (yield* sandbox.allocateProcess());

            receipt = { ...receipt, processId, instanceId: owner.instanceId };
            yield* object.storage.put(key, yield* Schema.encodeEffect(ReceiptJson)(receipt));
          }
          const owner = yield* sandbox.describe;

          if (owner.current?.instanceId !== receipt.instanceId)
            return yield* conflict("The accepted command belongs to an earlier native boot");
          const process = yield* sandbox.exec(argv, { ...options, processId: receipt.processId! });

          return yield* process.status;
        }),
      ),
      Match.tag("WaitPort", ({ processId, port, timeout }) =>
        Effect.gen(function* () {
          const process = yield* sandbox.getProcess(processId);

          if (process._tag === "None")
            return yield* conflict("The native listener receipt is missing");
          yield* process.value.waitForPort(port, { timeout });
        }),
      ),
      Match.tag("WaitHttp", ({ processId, port, timeout, path, status }) =>
        Effect.gen(function* () {
          const process = yield* sandbox.getProcess(processId);

          if (process._tag === "None")
            return yield* conflict("The native listener receipt is missing");
          yield* process.value.waitForHttp(port, {
            timeout,
            ...(path === undefined ? {} : { path }),
            ...(status === undefined ? {} : { status }),
          });
        }),
      ),
    )
    .pipe(
      Match.tag("AcknowledgeProcess", ({ processId }) =>
        Effect.gen(function* () {
          const sql = (yield* SqlClient.SqlClient).withoutTransforms();
          const terminal = yield* sql`SELECT 1 FROM sandbox_task_native_terminal
            WHERE purpose = ${request.purpose} AND process_id = ${processId}`;

          if (terminal.length === 0)
            return yield* conflict("The terminal process output has not been retained");

          return yield* sandbox.acknowledgeProcess(processId);
        }),
      ),
      Match.tag("SignalProcess", ({ processId, signal }) =>
        sandbox.signalProcess(processId, signal),
      ),
      Match.tag("Processes", () => sandbox.listProcesses),
      Match.tag("ReadFileString", ({ path }) => sandbox.readFileString(path)),
      Match.tag("Exists", ({ path }) => sandbox.exists(path)),
      Match.tag("WriteFile", ({ path, content }) => sandbox.writeFile(path, content)),
      Match.tag("Mkdir", ({ path, recursive }) => sandbox.mkdir(path, { recursive })),
      Match.tag("Remove", ({ path, recursive, force }) =>
        sandbox.remove(path, { recursive, force }),
      ),
      Match.tag("RepositoryAuthorization", ({ remote, credential, expiresAt, grantId }) =>
        sandbox.setRepositoryAuthorization(
          remote,
          credential,
          Math.min(expiresAt, admission.deadlineAtMillis ?? 0),
          grantId,
        ),
      ),
      Match.tag("ClearRepositoryAuthorization", (grant) =>
        sandbox.clearRepositoryAuthorization(grant),
      ),
      Match.tag("Destroy", () => sandbox.destroy),
      Match.tag("PrepareFile", (file) =>
        Effect.gen(function* () {
          const owner = yield* sandbox.describe;

          if (owner.current?.instanceId !== file.expectedInstanceId)
            return yield* conflict("The file capability belongs to a different incarnation");
          yield* object.storage.put(
            `native-forward:${request.purpose}:${request.operationId}`,
            yield* Schema.encodeEffect(ForwardJson)({
              _tag: "File",
              path: file.path,
              access: file.access,
              expectedInstanceId: file.expectedInstanceId,
              nativeTokenHash: yield* hash(file.nativeToken),
            }),
          );

          return { operationId: request.operationId, instanceId: file.expectedInstanceId };
        }),
      ),
      Match.tag("PrepareForward", (forward) =>
        Effect.gen(function* () {
          const owner = yield* sandbox.describe;

          if (
            owner.current?.instanceId !== forward.expectedInstanceId ||
            (yield* sandbox.readFileString(MATERIALIZATION_MARKER)).trim() !==
              forward.materializationId
          )
            return yield* conflict("The forward capability no longer owns its admitted workspace");
          yield* object.storage.put(
            `native-forward:${request.purpose}:${request.operationId}`,
            yield* Schema.encodeEffect(ForwardJson)({
              ...forward,
              _tag: "Gateway",
              nativeTokenHash: yield* hash(forward.nativeToken),
            }),
          );

          return { operationId: request.operationId, instanceId: forward.expectedInstanceId };
        }),
      ),
      Match.exhaustive,
    );
  const encodedResult = yield* Schema.encodeEffect(NativeOperationResultJson)(
    result === undefined ? null : result,
  );

  if (receipt !== undefined && !isObservation) {
    const instanceId =
      result !== null &&
      typeof result === "object" &&
      "instanceId" in result &&
      typeof result.instanceId === "string"
        ? result.instanceId
        : receipt.instanceId;

    yield* object.storage.put(
      key,
      yield* Schema.encodeEffect(ReceiptJson)({
        ...receipt,
        instanceId,
        completed: true,
        result: encodedResult,
      }),
    );
  }

  return encodedResult;
});

/** Called only after machine authentication and frozen run ownership checks. Native stream stays native. */
export const forwardSandboxTaskNativeRequest = Effect.fn("SandboxTaskNativeTransport.forward")(
  function* (
    admission: SandboxTaskMachineCreate,
    purpose: SandboxTaskNativeRequest["purpose"],
    operationId: string,
    request: Request,
  ) {
    const object = yield* DurableObjectState.DurableObjectState;
    const encoded = yield* object.storage.get<Schema.Json>(
      `native-forward:${purpose}:${operationId}`,
    );

    if (encoded === undefined) return new Response(null, { status: 404 });
    const grant = yield* Schema.decodeEffect(ForwardJson)(encoded);

    if (
      !(grant._tag === "File" && grant.access === "read") &&
      (admission.deadlineAtMillis === undefined ||
        (yield* Clock.currentTimeMillis) >= admission.deadlineAtMillis)
    )
      return new Response(null, { status: 403 });
    const nativeToken = request.headers.get("x-native-authorization");

    if (nativeToken === null || (yield* hash(nativeToken)) !== grant.nativeTokenHash)
      return new Response(null, { status: 403 });
    const sandbox = yield* SessionSandbox;
    const owner = yield* sandbox.describe;

    if (!owner.running || owner.current?.instanceId !== grant.expectedInstanceId)
      return new Response(null, { status: 409 });
    if (grant._tag === "File") {
      if (grant.access === "read" && request.method === "GET")
        return yield* sandbox.readFile(grant.path);
      if (grant.access === "write" && request.method === "PUT" && request.body !== null) {
        const uploadKey = `native-upload:${purpose}:${operationId}`;
        const prior = yield* object.storage.get<Schema.Json>(uploadKey);

        if (prior !== undefined) {
          const state = yield* Schema.decodeEffect(
            Schema.toCodecJson(
              Schema.Struct({
                instanceId: Schema.String,
                complete: Schema.Boolean,
                sha256: Schema.NullOr(Schema.String),
              }),
            ),
          )(prior);

          return new Response(null, {
            status: state.complete && state.instanceId === grant.expectedInstanceId ? 204 : 409,
          });
        }
        // A streaming write is admitted once. An uncertain stream is never redispatched
        // under the same identity; the application retries its enclosing source operation.
        yield* object.storage.put(uploadKey, {
          instanceId: grant.expectedInstanceId,
          complete: false,
          sha256: null,
        });
        const [write, verify] = request.body.tee();
        const digest = createHash("sha256");
        const outcomes = yield* Effect.all(
          [
            sandbox.writeFile(grant.path, write),
            Effect.promise(async () => {
              await verify.pipeTo(
                new WritableStream({
                  write(chunk) {
                    digest.update(chunk);
                  },
                }),
              );

              return digest.digest("hex");
            }),
          ],
          { concurrency: "unbounded" },
        );

        yield* object.storage.put(uploadKey, {
          instanceId: grant.expectedInstanceId,
          complete: true,
          sha256: outcomes[1],
        });

        return new Response(null, { status: 204 });
      }

      return new Response(null, { status: 405 });
    }
    if ((yield* sandbox.readFileString(MATERIALIZATION_MARKER)).trim() !== grant.materializationId)
      return new Response(null, { status: 409 });
    const headers = new Headers({ Authorization: `Bearer ${nativeToken}` });

    if (request.headers.get("upgrade")?.toLowerCase() === "websocket")
      headers.set("upgrade", "websocket");

    return yield* sandbox.forwardPort(
      grant.port,
      new Request(`http://localhost:${grant.port}/${encodeURIComponent(grant.nativeOperationId)}`, {
        method: request.method,
        headers,
        ...(request.method === "GET" || request.method === "HEAD" ? {} : { body: request.body }),
      }),
    );
  },
);
