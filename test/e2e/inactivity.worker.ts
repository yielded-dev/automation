// oxlint-disable no-restricted-syntax -- This probe deliberately exercises native APIs without Effect or Repairs.
import { DurableObject } from "cloudflare:workers";

const PROBE_REVISION = "native-isolation-6";
const ORDERS = ["inactivity", "inspect", "exec"] as const;
const STAGES = ["bare", "snapshot", "identity", "sync", "alarm"] as const;
const SNAPSHOT_MARKER = "native-inactivity-snapshot";
const SNAPSHOT_PATH = "/tmp/inactivity-snapshot-marker";
const PAYLOAD_PATH = "/tmp/inactivity-generated-payload";
const SYNTHETIC_REPO = /^inactivity-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
type Order = (typeof ORDERS)[number];
type Stage = (typeof STAGES)[number];
interface ProbeInput {
  order: Order;
  stage: Stage;
  snapshotId?: string;
  expectedGitHead?: string;
  payloadMiB?: number;
  expectedPayloadSha256?: string;
  shape?: "standard-1" | "matched";
  capture: boolean;
}
interface Env {
  PROBE: DurableObjectNamespace<InactivityProbe>;
  ARTIFACTS?: Artifacts;
  ISOLATION_TEST_TOKEN?: string;
  ISOLATION_IMAGE?: string;
}
interface Acceptance {
  id: string;
  name: string;
  input: ProbeInput;
  acceptedAt: number;
}
interface GeneratedPayload {
  sizeBytes: number;
  sha256: string;
}

const failureDetails = (cause: unknown) =>
  cause instanceof Error
    ? { name: cause.name, message: cause.message, stack: cause.stack }
    : String(cause);

async function repoInfo(repo: ArtifactsRepo) {
  // Installed generated types omit this remote RPC method; properties on the handle are not metadata.
  const native = repo as unknown as { info(): Promise<unknown> };
  const info = await native.info();
  if (!info || typeof info !== "object" || !("id" in info) || typeof info.id !== "string")
    throw new TypeError("Native repo.info() returned invalid metadata");
  return { id: info.id };
}

export class InactivityProbe extends DurableObject<Env> {
  async run({
    order,
    stage,
    snapshotId,
    expectedGitHead,
    payloadMiB,
    expectedPayloadSha256,
    shape = "standard-1",
    capture,
  }: ProbeInput) {
    const persistIdentity = stage === "identity" || stage === "sync" || stage === "alarm";
    const container = this.ctx.container;
    if (!container) throw new Error("Native container binding is required");
    if (container.running) throw new Error("Each sample requires a fresh container");
    const image = this.env.ISOLATION_IMAGE ?? container.images.harness;
    if (!image) throw new Error("Probe image is not configured");
    const instance =
      shape === "matched" ? { vcpu: 2, memoryMib: 12288, diskMb: 20000 } : "standard-1";
    const events: Array<{ operation: string; phase: string; millis: number; epochMillis: number }> =
      [];
    let capturedSnapshot: ContainerSnapshot | undefined;
    let payload: GeneratedPayload | undefined;
    const identity = {
      instanceId: crypto.randomUUID(),
      imageDigest: snapshotId ? null : image,
      restoredSnapshotId: snapshotId ?? null,
      timings: {
        nativeStartRequestedAt: Date.now(),
        nativeStartReturnedAt: null as number | null,
        ownerReadyAt: null,
      },
    };
    const started = performance.now();
    const mark = (operation: string, phase: string) => {
      const event = {
        operation,
        phase,
        millis: performance.now() - started,
        epochMillis: Date.now(),
      };
      events.push(event);
      console.log(
        "INACTIVITY_EVENT " +
          JSON.stringify({ id: this.ctx.id.toString(), instanceId: identity.instanceId, ...event }),
      );
    };
    const measure = async <A>(operation: string, call: () => Promise<A>) => {
      mark(operation, "begin");
      try {
        const value = await call();
        mark(operation, "end");
        return value;
      } catch (cause) {
        mark(operation, "error");
        throw cause;
      }
    };
    const execute = async () => {
      const process = await measure("exec", () => container.exec(["/bin/true"]));
      const output = await measure("exec.output", () => process.output());
      if (output.exitCode !== 0) throw new Error(`Readiness command exited ${output.exitCode}`);
      mark("commandReady", "observed");
    };

    try {
      mark("start", "begin");
      container.start({
        ...(snapshotId ? { containerSnapshot: { id: snapshotId } } : { image }),
        instance,
        labels: { instance: identity.instanceId },
        enableInternet: false,
        entrypoint: [
          "/bin/sh",
          "-c",
          "printf 'INACTIVITY_PROCESS_READY\\n'; exec tail -f /dev/null",
        ],
      });
      mark("start", "end");
      const runningAfterStart = container.running;
      if (persistIdentity) {
        identity.timings.nativeStartReturnedAt = Date.now();
        await measure("identity.put", () => this.ctx.storage.put("container-identity", identity));
      }
      if (stage === "sync" || stage === "alarm")
        await measure("identity.sync", () => this.ctx.storage.sync());
      let inspected: ContainerInfo | null | undefined;
      if (order === "inspect") inspected = await measure("inspect", () => container.inspect());
      if (order === "exec") await execute();
      await measure("setInactivityTimeout", () =>
        container.setInactivityTimeout(6 * 60 * 60 * 1000),
      );
      if (order !== "exec") await execute();
      // A second identical control call measures the already-ready path directly.
      await measure("ready.setInactivityTimeout", () =>
        container.setInactivityTimeout(6 * 60 * 60 * 1000),
      );
      if (payloadMiB !== undefined) {
        const operation = capture ? "generatePayload" : "verifyPayload";
        const command =
          (capture
            ? `dd if=/dev/urandom of=${PAYLOAD_PATH} bs=1048576 count=${payloadMiB} iflag=fullblock status=none && `
            : "") + `sha256sum ${PAYLOAD_PATH} && stat -c %s ${PAYLOAD_PATH}`;
        const process = await measure(`${operation}.exec`, () =>
          container.exec(["/bin/sh", "-c", command]),
        );
        const output = await measure(`${operation}.output`, () => process.output());
        if (output.exitCode !== 0 || output.stdout.byteLength > 256)
          throw new Error(
            `Payload command failed (exit ${output.exitCode}; output limit 256 bytes)`,
          );
        const lines = new TextDecoder().decode(output.stdout).trim().split("\n");
        const sha256 = lines[0]?.split(/\s+/)[0];
        if (
          lines.length !== 2 ||
          !/^[a-f0-9]{64}$/.test(sha256 ?? "") ||
          Number(lines[1]) !== payloadMiB * 1024 * 1024
        )
          throw new Error("Generated payload size or SHA-256 is invalid");
        payload = { sizeBytes: Number(lines[1]), sha256: sha256! };
        if (snapshotId && payload.sha256 !== expectedPayloadSha256)
          throw new Error("Restored payload SHA-256 did not match the captured bytes");
        if (snapshotId) mark("snapshotRestored", "observed");
      }
      if (snapshotId && payloadMiB === undefined) {
        const verification = expectedGitHead ? "verifyGitHead" : "verifyRestore";
        const process = await measure(`${verification}.exec`, () =>
          container.exec(
            expectedGitHead
              ? ["git", "-C", "/workspace/repo", "rev-parse", "HEAD"]
              : ["/bin/cat", SNAPSHOT_PATH],
          ),
        );
        const output = await measure(`${verification}.output`, () => process.output());
        const observed = new TextDecoder().decode(output.stdout);
        if (
          output.exitCode !== 0 ||
          (expectedGitHead ? observed.trim() !== expectedGitHead : observed !== SNAPSHOT_MARKER)
        )
          throw new Error(
            expectedGitHead
              ? "Restored Git HEAD did not match the supplied revision"
              : "Native snapshot did not restore the captured marker",
          );
        mark("snapshotRestored", "observed");
      }
      if (capture) {
        if (payloadMiB === undefined) {
          const process = await measure("writeSnapshotMarker.exec", () =>
            container.exec(["/bin/sh", "-c", `printf '${SNAPSHOT_MARKER}' > ${SNAPSHOT_PATH}`]),
          );
          const output = await measure("writeSnapshotMarker.output", () => process.output());
          if (output.exitCode !== 0) throw new Error("Could not write snapshot marker");
        }
        capturedSnapshot = await measure("snapshotContainer", () =>
          container.snapshotContainer({ name: "native-inactivity" }),
        );
        await this.ctx.storage.put("owned-snapshot", { ...capturedSnapshot, payload });
      }
      if (persistIdentity) {
        const stored = await measure("identity.get", () =>
          this.ctx.storage.get("container-identity"),
        );
        if (JSON.stringify(stored) !== JSON.stringify(identity))
          throw new Error("Stored boot identity changed");
      }
      mark("complete", "end");
      return {
        passed: true,
        probeRevision: PROBE_REVISION,
        id: this.ctx.id.toString(),
        instanceId: identity.instanceId,
        instance,
        imageReference: snapshotId ? undefined : image,
        order,
        stage,
        runningAfterStart,
        inspected,
        restoredSnapshotId: snapshotId,
        verification: snapshotId
          ? payload
            ? "generated-payload"
            : expectedGitHead
              ? "git-head"
              : "synthetic-marker"
          : undefined,
        capturedSnapshot,
        payload,
        events,
      };
    } catch (cause) {
      return {
        passed: false,
        probeRevision: PROBE_REVISION,
        id: this.ctx.id.toString(),
        instanceId: identity.instanceId,
        instance,
        imageReference: snapshotId ? undefined : image,
        order,
        stage,
        capturedSnapshot,
        payload,
        events,
        failure: failureDetails(cause),
      };
    }
  }

  async schedule(name: string, input: ProbeInput) {
    return this.ctx.storage.transaction(async (transaction) => {
      const existing = await transaction.get<Acceptance>("accepted");
      if (existing) {
        if (existing.name !== name || JSON.stringify(existing.input) !== JSON.stringify(input))
          throw new Error("Actor already owns another accepted scenario");
        return existing;
      }
      const accepted = { id: this.ctx.id.toString(), name, input, acceptedAt: Date.now() };
      await transaction.put("accepted", accepted);
      await transaction.setAlarm(Date.now());
      return accepted;
    });
  }

  async alarm(info: AlarmInvocationInfo) {
    const accepted = await this.ctx.storage.get<Acceptance>("accepted");
    if (!accepted) throw new Error("Alarm has no accepted owner");
    try {
      if (!(await this.ctx.storage.get("result"))) {
        const result = await this.run(accepted.input);
        console.log(
          "INACTIVITY_EVENT " +
            JSON.stringify({
              id: this.ctx.id.toString(),
              operation: "alarm.result.put",
              phase: "begin",
            }),
        );
        await this.ctx.storage.put("result", {
          ...result,
          accepted,
          alarm: {
            isRetry: info.isRetry,
            retryCount: info.retryCount,
            scheduledTime: info.scheduledTime,
          },
        });
        console.log(
          "INACTIVITY_EVENT " +
            JSON.stringify({
              id: this.ctx.id.toString(),
              operation: "alarm.result.put",
              phase: "end",
            }),
        );
        await this.ctx.storage.sync();
      }
    } catch (cause) {
      const failure = failureDetails(cause);
      console.log(
        "INACTIVITY_EVENT " +
          JSON.stringify({
            id: this.ctx.id.toString(),
            operation: "alarm",
            phase: "error",
            failure,
          }),
      );
      await this.ctx.storage.put("result", { passed: false, accepted, failure });
    } finally {
      console.log("INACTIVITY_DONE " + accepted.name);
    }
  }

  result() {
    return this.ctx.storage.get("result");
  }

  async destroy(purge = false) {
    await this.ctx.storage.deleteAlarm();
    await this.ctx.container?.destroy();
    const snapshot = await this.ctx.storage.get<ContainerSnapshot & { payload?: GeneratedPayload }>(
      "owned-snapshot",
    );
    if (purge) await this.ctx.storage.deleteAll();
    return {
      running: this.ctx.container?.running ?? false,
      snapshot,
    };
  }
}

export default {
  async fetch(request: Request, env: Env) {
    // This disposable diagnostic has no public operations, including capability checks.
    const token = env.ISOLATION_TEST_TOKEN;
    if (!token || !/^[a-f0-9]{64}$/.test(token))
      return new Response("Probe token is not configured", { status: 503 });
    const expected = new TextEncoder().encode(`Bearer ${token}`);
    const supplied = new TextEncoder().encode(request.headers.get("Authorization") ?? "");
    if (
      supplied.byteLength !== expected.byteLength ||
      !crypto.subtle.timingSafeEqual(supplied, expected)
    )
      return new Response("Unauthorized", { status: 401 });
    if (request.method !== "POST") return new Response("POST required", { status: 405 });
    const url = new URL(request.url);
    if (url.pathname === "/health")
      return Response.json({
        ok: true,
        probeRevision: PROBE_REVISION,
        imageReference: env.ISOLATION_IMAGE ?? "named:harness",
      });
    let params: Record<string, string>;
    try {
      const body = await request.json<unknown>();
      if (
        !body ||
        typeof body !== "object" ||
        Array.isArray(body) ||
        Object.values(body).some((value) => typeof value !== "string")
      )
        return new Response("Expected a JSON object of string parameters", { status: 400 });
      params = body as Record<string, string>;
    } catch {
      return new Response("Expected JSON parameters", { status: 400 });
    }
    if (url.pathname === "/artifacts") {
      if (!env.ARTIFACTS)
        return new Response("Native Artifacts binding is not configured", { status: 400 });
      const started = performance.now();
      try {
        await env.ARTIFACTS.list({ limit: 1 });
        return Response.json({ supported: true, callMillis: performance.now() - started });
      } catch (cause) {
        return Response.json({
          supported: false,
          callMillis: performance.now() - started,
          failure: failureDetails(cause),
        });
      }
    }
    if (!["/run", "/schedule", "/destroy", "/purge", "/result"].includes(url.pathname))
      return new Response("Unknown operation", { status: 404 });
    const {
      name = "",
      order,
      stage = "bare",
      snapshot: snapshotId,
      expectedGitHead,
      expectedPayloadSha256,
      artifactsRepo,
      shape = "standard-1",
      locationHint,
    } = params;
    const capture = params.capture === "true";
    const payloadMiB = params.payloadMiB === undefined ? undefined : Number(params.payloadMiB);
    if (!/^[a-z0-9-]{1,64}$/.test(name)) return new Response("Invalid actor name", { status: 400 });
    if (locationHint !== undefined && !["wnam", "enam", "weur"].includes(locationHint))
      return new Response("Invalid probe location hint", { status: 400 });
    const probe = env.PROBE.getByName(name, {
      locationHint: locationHint as "wnam" | "enam" | "weur" | undefined,
    });
    if (url.pathname === "/destroy") return Response.json(await probe.destroy());
    if (url.pathname === "/purge") return Response.json(await probe.destroy(true));
    if (url.pathname === "/result") return Response.json((await probe.result()) ?? null);
    if (!ORDERS.includes(order as Order) || !STAGES.includes(stage as Stage))
      return new Response("Invalid scenario", { status: 400 });
    if (shape !== "standard-1" && shape !== "matched")
      return new Response("Invalid fixed instance shape", { status: 400 });
    if (snapshotId !== undefined && (!/^[0-9a-f-]{36}$/.test(snapshotId) || capture))
      return new Response("Invalid snapshot", { status: 400 });
    if (
      expectedGitHead !== undefined &&
      (!snapshotId || !/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/.test(expectedGitHead))
    )
      return new Response("Expected Git HEAD requires an explicit snapshot and full revision", {
        status: 400,
      });
    if (
      payloadMiB !== undefined &&
      (!/^[1-9]\d{0,3}$/.test(params.payloadMiB!) ||
        payloadMiB > 4096 ||
        (!capture && !snapshotId) ||
        expectedGitHead !== undefined)
    )
      return new Response("Payload requires 1–4096 MiB and synthetic capture or restore", {
        status: 400,
      });
    if (
      snapshotId && payloadMiB !== undefined
        ? !/^[a-f0-9]{64}$/.test(expectedPayloadSha256 ?? "")
        : expectedPayloadSha256 !== undefined
    )
      return new Response("Payload restore requires the captured SHA-256", { status: 400 });
    if (
      artifactsRepo !== undefined &&
      (!SYNTHETIC_REPO.test(artifactsRepo) || !env.ARTIFACTS || url.pathname !== "/run")
    )
      return new Response("Invalid synthetic Artifacts scenario", { status: 400 });
    const input: ProbeInput = {
      order: order as Order,
      stage: stage as Stage,
      snapshotId,
      expectedGitHead,
      payloadMiB,
      expectedPayloadSha256,
      capture,
      shape,
    };
    if (url.pathname === "/schedule") {
      if (stage !== "alarm" || capture)
        return new Response("Invalid alarm scenario", { status: 400 });
      return Response.json(await probe.schedule(name, input));
    }
    if (!artifactsRepo) {
      const result = await probe.run(input);
      console.log("INACTIVITY_DONE " + name);
      return Response.json({ ...result, callerRevision: PROBE_REVISION });
    }

    // Keep this real remote handle in the caller's scope throughout the container RPC.
    const artifacts = env.ARTIFACTS!;
    const started = performance.now();
    const events: Array<{ operation: string; beginMillis: number; endMillis: number }> = [];
    const measure = async <A>(operation: string, call: () => Promise<A>) => {
      const beginMillis = performance.now() - started;
      try {
        return await call();
      } finally {
        events.push({ operation, beginMillis, endMillis: performance.now() - started });
      }
    };
    let owned = false;
    let repoId: string | undefined;
    let deleted: boolean | undefined;
    let failure: ReturnType<typeof failureDetails> | undefined;
    let cleanupFailure: ReturnType<typeof failureDetails> | undefined;
    let result: Awaited<ReturnType<InactivityProbe["run"]>> | undefined;
    try {
      // The initial credential never leaves this Worker or enters a snapshot.
      const created = await measure("create", () => artifacts.create(artifactsRepo));
      owned = true;
      repoId = created.id;
      const repo = await measure("get", () => artifacts.get(artifactsRepo));
      const before = await measure("info.before", () => repoInfo(repo));
      if (before.id !== repoId) throw new Error("Synthetic repository identity changed");
      result = await measure("container.run", async () => await probe.run(input));
      const after = await measure("info.after", () => repoInfo(repo));
      if (after.id !== before.id) throw new Error("Retained repository identity changed");
    } catch (cause) {
      failure = failureDetails(cause);
    } finally {
      // Failed/ambiguous creation retains its intended name and is never adopted on retry.
      if (owned) {
        try {
          deleted = await measure("delete", () => artifacts.delete(artifactsRepo));
        } catch (cause) {
          cleanupFailure = failureDetails(cause);
        }
      }
      console.log("INACTIVITY_DONE " + name);
    }
    return Response.json({
      ...result,
      callerRevision: PROBE_REVISION,
      passed:
        result?.passed === true &&
        failure === undefined &&
        cleanupFailure === undefined &&
        deleted === true,
      failure: result?.failure ?? failure ?? cleanupFailure,
      artifacts: { repoName: artifactsRepo, repoId, deleted, events, failure, cleanupFailure },
    });
  },
} satisfies ExportedHandler<Env>;
