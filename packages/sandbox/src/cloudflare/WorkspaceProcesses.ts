import { Context, Effect, Layer, Option, Redacted, Schema, Semaphore } from "effect";

import type { WorkspaceService } from "../Contract";
import { Harness } from "../harness/Harness";
import { SessionSandbox } from "../SessionSandbox";
import { inMeasure } from "../tracing/RunSpanCapture";
import { REPO_DIR } from "../workspace-paths";
import type { ProcessStatus } from "./NativeSandbox";
import { SessionStateStore, type ManagedWorkspaceService } from "./SessionStateStore";

/**
 * Process hygiene and long-lived workspace services inside the session's
 * sandbox. Turns are serialized per session, so any harness process found
 * running at turn start is a zombie from a dead isolate; it is killed before
 * the workspace is touched so it cannot dirty files a later persist would
 * silently commit. Services (dev server, database) are caller-declared per
 * message, keep running between turns, and never block or fail a turn.
 */

/** The message's declared workspace services; defaults to none, so hosts
 * that never declare services provide nothing. */
export const TurnWorkspaceServices = Context.Reference<ReadonlyArray<WorkspaceService>>(
  "sandbox-agent/WorkspaceProcesses/TurnWorkspaceServices",
  { defaultValue: () => [] },
);

export const TurnWorkspaceServiceEnvironment = Context.Reference<
  Redacted.Redacted<Readonly<Record<string, string>>>
>("sandbox-agent/WorkspaceProcesses/TurnWorkspaceServiceEnvironment", {
  defaultValue: () => Redacted.make({}),
});

export const TurnWorkspaceServicePorts = Context.Reference<Readonly<Record<string, number>>>(
  "sandbox-agent/WorkspaceProcesses/TurnWorkspaceServicePorts",
  { defaultValue: () => ({}) },
);

export const RequiredWorkspaceServices = Context.Reference<ReadonlyArray<string>>(
  "sandbox-agent/WorkspaceProcesses/RequiredWorkspaceServices",
  { defaultValue: () => [] },
);

export class RequiredWorkspaceServiceExited extends Schema.TaggedError<RequiredWorkspaceServiceExited>()(
  "RequiredWorkspaceServiceExited",
  { name: Schema.String },
) {}

/**
 * Serializes workspace-service reconciliation within one session Durable
 * Object. A turn and an on-demand preview activation can both observe the
 * same process snapshot; one coordinator prevents them from double-launching
 * a declared service.
 */
export class WorkspaceServiceCoordinator extends Context.Service<
  WorkspaceServiceCoordinator,
  {
    readonly run: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
  }
>()("sandbox-agent/WorkspaceProcesses/WorkspaceServiceCoordinator") {
  static readonly layer = Layer.sync(WorkspaceServiceCoordinator, () => {
    const semaphore = Semaphore.makeUnsafe(1);

    return WorkspaceServiceCoordinator.of({ run: (effect) => semaphore.withPermit(effect) });
  });
}

const SIGKILL = 9;

const sameArgv = (command: ReadonlyArray<string>, argv: ReadonlyArray<string>): boolean =>
  command.length === argv.length && command.every((token, index) => token === argv[index]);

const sameManagedServices = (
  left: ReadonlyArray<ManagedWorkspaceService>,
  right: ReadonlyArray<ManagedWorkspaceService>,
): boolean =>
  left.length === right.length &&
  left.every((service, index) => {
    const candidate = right[index];

    return (
      candidate !== undefined &&
      service.processId === candidate.processId &&
      service.service.name === candidate.service.name &&
      sameArgv(service.service.argv, candidate.service.argv)
    );
  });

const killProcess = Effect.fn("WorkspaceProcesses.killProcess")(function* (processId: string) {
  const sandbox = yield* SessionSandbox;
  const handle = yield* sandbox.getProcess(processId);

  if (Option.isSome(handle)) yield* handle.value.kill(SIGKILL);
});

const runningProcesses = Effect.fn("WorkspaceProcesses.runningProcesses")(function* (
  candidates: ReadonlyArray<ProcessStatus>,
) {
  const sandbox = yield* SessionSandbox;
  const running: Array<ProcessStatus> = [];

  for (const candidate of candidates) {
    const handle = yield* sandbox.getProcess(candidate.id);

    if (Option.isNone(handle)) continue;
    const current = yield* handle.value.status;

    if (current.state === "running" && sameArgv(current.command, candidate.command)) {
      running.push(current);
    }
  }

  return running;
});

/** Kills direct harness processes and their native bootstrap wrappers. */
export const killStrayHarnessProcesses = Effect.fn("WorkspaceProcesses.killStrayHarnessProcesses")(
  function* () {
    const sandbox = yield* SessionSandbox;
    const harness = yield* Harness;
    const processes = yield* sandbox.listProcesses;
    const strays = processes.filter(
      (process) =>
        process.state === "running" &&
        (process.command[0] === harness.binary ||
          (process.command[0] === "sh" &&
            process.command[1] === "-c" &&
            process.command[3] === "codex-app-server" &&
            process.command[4] === harness.binary &&
            process.command[5] === "app-server")),
    );

    yield* Effect.forEach(strays, (stray) => killProcess(stray.id), {
      concurrency: "unbounded",
    });
  },
  inMeasure("stray-harness-kill"),
);

/**
 * Ensures every service declared on the turn has a running process. When the
 * workspace was re-materialized this turn, matching processes are killed
 * first — their working tree is gone — and started fresh against the new
 * checkout. Started processes are deliberately not awaited; the sandbox
 * supervises them.
 */
const reconcileWorkspaceServices = Effect.fn("WorkspaceProcesses.reconcileWorkspaceServices")(
  function* (workspaceReplaced: boolean) {
    const services = yield* TurnWorkspaceServices;
    const environment = yield* TurnWorkspaceServiceEnvironment;
    const ports = yield* TurnWorkspaceServicePorts;
    const required = yield* RequiredWorkspaceServices;
    const store = yield* SessionStateStore;
    const previous = (yield* store.managedWorkspaceServices.get) ?? [];

    // This is every ordinary control/warm turn. Preserve the baseline path:
    // no sandbox process-list RPC and no durable snapshot write are needed.
    if (services.length === 0 && previous.length === 0) return;
    const sandbox = yield* SessionSandbox;
    // Read inside the reconciliation permit. An on-demand activation and a
    // turn may arrive with snapshots taken before either acquired the lock;
    // the live list is what makes adoption single-launch under that race.
    const processes = yield* sandbox.listProcesses;
    // Two declarations with the same argv would both observe the pre-start
    // snapshot and double-launch; only the first declaration counts.
    const seen = new Set<string>();
    const unique = services.filter((service) => {
      const key = service.argv.join("\u0000");

      if (seen.has(key)) return false;
      seen.add(key);

      return true;
    });
    const desiredByName = new Map(unique.map((service) => [service.name, service] as const));
    const killedIds = new Set<string>();
    const stale = previous.filter((registered) => {
      const desired = desiredByName.get(registered.service.name);

      return (
        workspaceReplaced ||
        desired === undefined ||
        !sameArgv(desired.argv, registered.service.argv)
      );
    });

    yield* Effect.forEach(
      stale,
      (registered) =>
        killProcess(registered.processId).pipe(
          Effect.tap(() => Effect.sync(() => killedIds.add(registered.processId))),
        ),
      { concurrency: "unbounded" },
    );
    if (workspaceReplaced) {
      const invalidated = processes.filter(
        (process) =>
          process.state === "running" &&
          !killedIds.has(process.id) &&
          unique.some((service) => sameArgv(process.command, service.argv)),
      );

      yield* Effect.forEach(
        invalidated,
        (process) =>
          killProcess(process.id).pipe(
            Effect.tap(() => Effect.sync(() => killedIds.add(process.id))),
          ),
        { concurrency: "unbounded" },
      );
    }
    const claimed = new Set<string>();
    const reconciled: Array<ManagedWorkspaceService> = [];

    for (const service of unique) {
      const registered = previous.find(
        (candidate) =>
          candidate.service.name === service.name &&
          sameArgv(candidate.service.argv, service.argv) &&
          !killedIds.has(candidate.processId),
      );
      const candidates = processes.filter(
        (process) =>
          process.state === "running" &&
          !killedIds.has(process.id) &&
          !claimed.has(process.id) &&
          sameArgv(process.command, service.argv),
      );

      candidates.sort((left, right) => (left.id === right.id ? 0 : left.id < right.id ? -1 : 1));
      const registeredIndex =
        registered === undefined
          ? -1
          : candidates.findIndex(({ id }) => id === registered.processId);

      if (registeredIndex > 0) {
        const registeredCandidate = candidates[registeredIndex];

        if (registeredCandidate !== undefined) {
          candidates.splice(registeredIndex, 1);
          candidates.unshift(registeredCandidate);
        }
      }
      // Clone/warmup may take seconds after the hygiene snapshot. Re-read a
      // candidate's current status immediately before adoption; dead entries
      // are skipped and a fresh process is started when none remain alive.
      const running = yield* runningProcesses(candidates);
      const adopted = running[0];

      if (adopted !== undefined) {
        // Ambiguous launches can leave several live processes with the same
        // declared argv. Keep the preferred verified process and reap every
        // surplus match before recording the sole durable registration. A
        // failed reap remains observable and is retried on the next turn.
        yield* Effect.forEach(
          running.slice(1),
          (surplus) =>
            killProcess(surplus.id).pipe(
              Effect.tap(() => Effect.sync(() => killedIds.add(surplus.id))),
              Effect.catchCause((cause) =>
                Effect.logError(
                  "sandbox-agent duplicate workspace service cleanup failed",
                  cause,
                ).pipe(Effect.annotateLogs({ "workspace.service": service.name })),
              ),
            ),
          { concurrency: "unbounded" },
        );
        claimed.add(adopted.id);
        reconciled.push({ service, processId: adopted.id });
        const port = ports[service.name];

        if (port !== undefined) {
          const handle = yield* sandbox.getProcess(adopted.id);

          if (Option.isSome(handle)) yield* handle.value.waitForPort(port, { timeout: 120_000 });
        }
        if (required.includes(service.name)) {
          const handle = yield* sandbox.getProcess(adopted.id);

          if (Option.isNone(handle) || (yield* handle.value.status).state !== "running") {
            return yield* RequiredWorkspaceServiceExited.make({ name: service.name });
          }
        }
        continue;
      }
      const command: [string, ...Array<string>] = [
        service.argv[0] as string,
        ...service.argv.slice(1),
      ];
      const handle = yield* sandbox
        .exec(command, { cwd: REPO_DIR, env: Redacted.value(environment) })
        .pipe(inMeasure(`service:${service.name}`));

      claimed.add(handle.id);
      reconciled.push({ service, processId: handle.id });
      // Record the launched process before waiting for readiness, so a failed
      // probe cannot orphan it when the next recipe removes that service.
      yield* store.managedWorkspaceServices.put([
        ...reconciled,
        ...previous.filter(
          (registered) =>
            !killedIds.has(registered.processId) &&
            !reconciled.some((current) => current.service.name === registered.service.name),
        ),
      ]);
      const port = ports[service.name];

      if (port !== undefined) yield* handle.waitForPort(port, { timeout: 120_000 });
      if (required.includes(service.name)) {
        if (port === undefined) yield* Effect.sleep("100 millis");
        const status = yield* handle.status;

        if (status.state !== "running") {
          return yield* RequiredWorkspaceServiceExited.make({ name: service.name });
        }
      }
    }

    if (!sameManagedServices(previous, reconciled)) {
      yield* store.managedWorkspaceServices.put(reconciled);
    }
  },
);

export const ensureWorkspaceServices = Effect.fn("WorkspaceProcesses.ensureWorkspaceServices")(
  function* (workspaceReplaced: boolean) {
    const coordinator = yield* WorkspaceServiceCoordinator;

    yield* coordinator.run(reconcileWorkspaceServices(workspaceReplaced));
  },
);
