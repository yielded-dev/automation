import {
  NATIVE_LISTENER_PORTS,
  NATIVE_OPERATION_LIMIT,
  NATIVE_OUTPUT_LIMIT_BYTES,
} from "../NativeExecutionLimits";
import { NATIVE_RECEIPTS_DIRECTORY } from "../workspace-paths";

export const PROCESS_OWNER_PATH = "/run/kommunikasie/process-owner.mjs";
export const PROCESS_INSTANCE_PATH = "/run/kommunikasie/instance";
export const PROCESS_DIRECTORY = "/workspace/.sandbox-agent/processes";
export const PROCESS_SUPERVISOR_PORT = 4503;
/** Advertised by `/health`. Protocol 2 takes `protect` windows instead of Worker timestamps. */
export const SUPERVISOR_PROTOCOL = 2;
export const PROCESS_TOKEN_PATH = "/run/kommunikasie/private/process-token";

/** One authenticated supervisor owns the container boot and every command's lifetime. */
export const PROCESS_SUPERVISOR_SOURCE = String.raw`
import { mkdirSync, readFileSync, writeFileSync, openSync, closeSync, existsSync, statSync, readSync, writeSync, rmSync, chmodSync } from 'node:fs';
import { createHash, timingSafeEqual } from 'node:crypto';
import { spawn } from 'node:child_process';
import { EventEmitter } from 'node:events';
import { constants } from 'node:os';
import { connect } from 'node:net';

export const PORT = ${PROCESS_SUPERVISOR_PORT};
export const TOKEN_PATH = '${PROCESS_TOKEN_PATH}';
const root = '${PROCESS_DIRECTORY}';
const maximumJobs = ${NATIVE_OPERATION_LIMIT};
const maximumOutput = ${NATIVE_OUTPUT_LIMIT_BYTES};
const listenerPorts = [${NATIVE_LISTENER_PORTS.join(",")}];
const supportedSignals = new Set(Object.values(constants.signals));
const instancePath = '${PROCESS_INSTANCE_PATH}';
const digest = value => createHash('sha256').update(value).digest('hex');
const canonical = value => JSON.stringify(value, (_, v) => v && typeof v === 'object' && !Array.isArray(v) ? Object.fromEntries(Object.keys(v).sort().map(k => [k, v[k]])) : v);
const directory = id => root + '/' + digest(id);
const currentInstance = () => readFileSync(instancePath, 'utf8').trim();
const failure = (code, message) => Object.assign(Error(message), { code });
const sequence = (id, instanceId) => {
  const prefix = 'process:' + instanceId + ':';
  if (typeof id !== 'string' || !id.startsWith(prefix) || !/^[1-9][0-9]*$/.test(id.slice(prefix.length))) throw failure('PROCESS_REJECTED', 'Invalid process ticket');
  const value = Number(id.slice(prefix.length));
  if (!Number.isSafeInteger(value)) throw failure('PROCESS_REJECTED', 'Invalid process ticket');
  return value;
};
// An unreaped process retains its PID/start ticks but cannot own live work.
const processStart = pid => { try { const fields = readFileSync('/proc/' + pid + '/stat', 'utf8').split(') ')[1].split(' '); return fields[0] === 'Z' || fields[0] === 'X' ? null : fields[19]; } catch { return null; } };
const sameProcess = job => job.instanceId === currentInstance() && job.pid && processStart(job.pid) === job.processStart;
// Losing the group keeper closes signal ownership, but does not prove child exit.
const ownsJob = job => sameProcess(job) && job.groupPid && job.groupStart && (job.groupRetiring === true || processStart(job.groupPid) === job.groupStart);
const output = (dir, field, maxBytes) => {
  try { const path = dir + '/' + field, size = statSync(path).size; const bytes = Buffer.alloc(Math.min(size, maxBytes)); const fd = openSync(path, 'r'); try { readSync(fd, bytes, 0, bytes.length, 0); } finally { closeSync(fd); } return { text: bytes.toString(), truncated: size > maxBytes }; }
  catch (e) { if (e.code === 'ENOENT') return { text: '', truncated: false }; throw e; }
};

// Keep the group's Linux identity allocated even after its command leader exits.
// This extra process holds no output writer: only the command and its descendants
// can keep the supervisor's pipes open. The supervisor kills the keeper after drain,
// or the entire group at the original deadline, then retains the closed result.
const runGroup = config => {
  if (config.instanceId !== currentInstance()) throw Error('Container instance was replaced');
  for (const [name, signal] of Object.entries(constants.signals)) {
    if (signal !== 9 && signal !== 19 && signal !== 17) process.on(name, () => {});
  }
  // Catchable group signals reach the command without retiring its keeper.
  // STOP cannot be caught, but keeps this PID allocated until CONT or KILL.
  setInterval(() => {}, 1_000_000);
  let reported = false;
  const report = (code, signal) => {
    if (reported) return;
    reported = true;
    writeSync(3, JSON.stringify({ code: code ?? -1, ...(signal ? { signal } : {}) }));
    closeSync(3);
  };
  try {
    const child = spawn(config.argv[0], config.argv.slice(1), { cwd: config.cwd, env: process.env, stdio: ['ignore', 1, 2] });
    child.once('error', () => report(127));
    child.once('close', report);
  } catch { report(127); }
  closeSync(1); closeSync(2);
};

// The daemon holds configuration in memory and sends it to the group keeper through stdin.
const runJob = (config, onExit) => {
  const dir = directory(config.id);
  if (config.instanceId !== currentInstance()) throw Error('Container instance was replaced');
  const stdout = openSync(dir + '/stdout', 'a', 0o600), stderr = openSync(dir + '/stderr', 'a', 0o600);
  let child, deadline, childStart;
  let timedOut = false, truncated = false, finished = false, spawnFailed = false, groupKilled = false;
  let groupResult = null, groupMessage = '', stdoutEnded = false, stderrEnded = false;
  const maximum = Math.min(config.maxOutputBytes ?? maximumOutput, maximumOutput);
  let stdoutBytes = 0, stderrBytes = 0;
  const finish = (code, signal, confirmed = true) => {
    if (finished) return;
    finished = true;
    clearTimeout(deadline);
    closeSync(stdout); closeSync(stderr);
    onExit(confirmed ? { code: code ?? -1, ...(signal ? { signal } : {}), timedOut, truncated } : null);
  };
  if (config.deadlineAt != null && config.deadlineAt <= Date.now()) { timedOut = true; finish(-1); return; }
  try { child = spawn(process.execPath, ['${PROCESS_OWNER_PATH}', '--group'], { env: { ...process.env, ...config.env }, detached: true, stdio: ['pipe', 'pipe', 'pipe', 'pipe'] }); }
  catch { finish(127); return; }
  child.stdin.on('error', () => {});
  child.stdin.end(JSON.stringify(config));
  childStart = child.pid ? processStart(child.pid) : null;
  const append = (fd, bytes, count) => { const remaining = Math.max(0, maximum - count); if (bytes.length > remaining) truncated = true; if (remaining) writeSync(fd, bytes.subarray(0, remaining)); return count + Math.min(bytes.length, remaining); };
  child.stdout.on('data', bytes => { stdoutBytes = append(stdout, bytes, stdoutBytes); });
  child.stderr.on('data', bytes => { stderrBytes = append(stderr, bytes, stderrBytes); });
  const running = { pid: process.pid, processStart: processStart(process.pid), groupPid: child.pid ?? null, groupStart: childStart };
  const kill = signal => {
    if (groupKilled || config.instanceId !== currentInstance() || !child.pid || !childStart || processStart(child.pid) !== childStart) return;
    if (signal === 9) {
      groupKilled = true;
      running.groupRetiring = true;
    }
    try { process.kill(-child.pid, signal); } catch (e) { if (e.code !== 'ESRCH') throw e; }
  };
  const drained = () => { if (groupResult && stdoutEnded && stderrEnded) kill(9); };
  child.stdout.once('end', () => { stdoutEnded = true; drained(); });
  child.stderr.once('end', () => { stderrEnded = true; drained(); });
  child.stdio[3].on('data', bytes => { groupMessage += bytes.toString(); });
  child.stdio[3].once('end', () => { if (groupMessage) groupResult = JSON.parse(groupMessage); drained(); });
  deadline = config.deadlineAt == null ? undefined : setTimeout(() => { timedOut = true; kill(9); }, Math.max(1, config.deadlineAt - Date.now()));
  child.once('error', () => { spawnFailed = true; });
  // A keeper's closed pipes do not prove its command group stopped. Only our
  // confirmed group retirement or failure to spawn can settle this admission.
  child.once('close', (code, signal) => finish(spawnFailed ? 127 : groupResult?.code ?? code, groupResult ? groupResult.signal : signal, spawnFailed || groupKilled));
  return { kill, running };
};

const rpc = async (method, args) => {
  let response;
  try {
    response = await fetch('http://127.0.0.1:' + PORT + '/rpc', {
      method: 'POST', headers: { authorization: 'Bearer ' + readFileSync(TOKEN_PATH, 'utf8').trim(), 'content-type': 'application/json' },
      body: JSON.stringify({ method, args }), signal: AbortSignal.timeout(25_000),
    });
  } catch { throw failure('PROCESS_UNKNOWN', 'Process supervisor connection was lost'); }
  let reply;
  try { reply = await response.json(); } catch { throw failure('PROCESS_UNKNOWN', 'Process supervisor response was lost'); }
  if (!response.ok || reply?.ok !== true) throw failure(reply?.error?.code ?? 'PROCESS_UNKNOWN', reply?.error?.message ?? 'Process supervisor request failed');
  return reply.value;
};
export const launchJob = config => rpc('launchJob', [config]);
export const observeJob = (id, maxBytes = 1048576) => rpc('observeJob', [id, maxBytes]);
export const listJobs = () => rpc('listJobs', []);
export const signalJob = (id, signal = 15) => rpc('signalJob', [id, signal]);
export const waitJob = (id, maxBytes = 1048576, waitMillis = 20_000) => rpc('waitJob', [id, maxBytes, waitMillis]);
export const acknowledgeJob = id => rpc('acknowledgeJob', [id]);

export const serve = async ({ token, instanceId, idleMillis = 300_000 }) => {
  if (typeof token !== 'string' || token.length < 16 || instanceId !== currentInstance()) throw Error('Invalid supervisor boot authority');
  let methods, server;
  server = Bun.serve({ port: PORT, hostname: '0.0.0.0', idleTimeout: 30, maxRequestBodySize: 8 * 1024 * 1024,
    async fetch(request) {
      if (request.method === 'GET' && new URL(request.url).pathname === '/health') return Response.json({ ok: true, protocol: ${SUPERVISOR_PROTOCOL} });
      const supplied = request.headers.get('authorization') ?? '', expected = 'Bearer ' + token;
      if (supplied.length !== expected.length || !timingSafeEqual(Buffer.from(supplied), Buffer.from(expected))) return Response.json({ ok: false, error: { code: 'PROCESS_REJECTED', message: 'Process supervisor authentication failed' } }, { status: 401 });
      if (request.method !== 'POST' || new URL(request.url).pathname !== '/rpc') return new Response('Not found', { status: 404 });
      try {
        const input = await request.json();
        if (!input || !Object.hasOwn(methods, input.method) || !Array.isArray(input.args)) throw failure('PROCESS_REJECTED', 'Invalid process request');
        const value = await methods[input.method](...input.args);
        return Response.json({ ok: true, value: value ?? null });
      } catch (error) {
        const known = ['PROCESS_REJECTED', 'PROCESS_RETIRED', 'PROCESS_UNKNOWN', 'LEASE_LOST', 'PROCESS_EXITED', 'PROCESS_LOST', 'ETIMEDOUT'].includes(error.code);
        return Response.json({ ok: false, error: { code: known ? error.code : 'PROCESS_UNKNOWN', message: known ? error.message : 'Process supervisor operation failed' } });
      }
    },
  });
  try {
  const jobs = new Map(), waiters = new Map(), controllers = new Map(), resources = new Map();
  const leases = new Map(), protections = new Map();
  let idleAt = Date.now() + idleMillis, retiring = false;
  const activeUntil = () => Math.max(0,
    ...[...leases.values()].filter(lease => !lease.completed).map(lease => lease.expiresAt),
    ...protections.values(),
    ...[...jobs.values()].filter(job => job.completionRevision == null && !job.reserved && job.nativeListenerPort === undefined).map(job => (job.deadlineAt ?? 0) + 5000));
  const requireActive = () => { if (retiring) throw failure('LEASE_LOST', 'Container retirement is pending'); };
  mkdirSync(root, { recursive: true, mode: 0o700 });
  let retiredThrough = 0, nextCompletionRevision = 0, nextSequence = 0;
  const lookup = id => {
    const seq = sequence(id, instanceId), entry = jobs.get(id);
    if (entry?.reserved && entry.expiresAt <= Date.now()) { retire(entry); throw failure('PROCESS_RETIRED', 'Process reservation expired'); }
    if (entry) return entry;
    if (seq <= retiredThrough) throw failure('PROCESS_RETIRED', 'Process ticket was retired');
    return null;
  };
  const refresh = entry => {
    if (entry.reserved) { if (entry.expiresAt <= Date.now()) retire(entry); return null; }
    const exit = entry.exit;
    if (!exit) return null;
    const completed = entry.completionRevision == null;
    if (completed) { entry.completionRevision = ++nextCompletionRevision; }
    if (completed) for (const wake of waiters.get(entry.id) ?? []) wake();
    return exit;
  };
  const status = (id, maxBytes = 1048576) => {
    if (!Number.isSafeInteger(maxBytes) || maxBytes < 0 || maxBytes > maximumOutput) throw failure('PROCESS_REJECTED', 'Invalid process output limit');
    const entry = lookup(id);
    if (!entry) throw failure('PROCESS_REJECTED', 'Process ticket was not registered');
    if (entry.reserved) return null;
    const dir = directory(id), exit = refresh(entry), running = controllers.get(id)?.running;
    const stdout = output(dir, 'stdout', maxBytes), stderr = output(dir, 'stderr', maxBytes);
    return { id, command: entry.argv, instanceId,
      state: exit ? 'exited' : ownsJob({ ...entry, ...running }) ? 'running' : 'lost',
      ...(exit ? { exit: { code: exit.code, ...(exit.signal ? { signal: exit.signal } : {}) } } : {}),
      stdout: stdout.text, stderr: stderr.text, exitCode: exit?.code ?? null,
      timedOut: exit?.timedOut ?? false, truncated: stdout.truncated || stderr.truncated || (exit?.truncated ?? false),
      deadlineAt: entry.deadlineAt ?? null, completionRevision: entry.completionRevision ?? null };
  };
  const retire = entry => {
    retiredThrough = Math.max(retiredThrough, entry.sequence);
    rmSync(directory(entry.id), { recursive: true, force: true });
    jobs.delete(entry.id);
  };
  const acknowledge = id => {
    let entry;
    try { entry = lookup(id); } catch (error) { if (error.code === 'PROCESS_RETIRED') return { retiredThrough }; throw error; }
    if (!entry || entry.reserved || !refresh(entry)) throw failure('PROCESS_UNKNOWN', 'Process has no confirmed closed result');
    entry.acknowledged = true;
    const acknowledged = [...jobs.values()].filter(job => job.acknowledged && job.nativeListenerPort === undefined).sort((a, b) => a.completionRevision - b.completionRevision);
    for (const old of acknowledged.slice(0, Math.max(0, acknowledged.length - 32))) retire(old);
    return { retiredThrough };
  };
  const launch = config => {
    requireActive();
    if (!config || typeof config.id !== 'string' || !Array.isArray(config.argv) || !config.argv.length || !config.argv.every(x => typeof x === 'string') ||
        (config.cwd !== undefined && typeof config.cwd !== 'string') ||
        (config.env !== undefined && (!config.env || typeof config.env !== 'object' || Array.isArray(config.env) || !Object.values(config.env).every(x => typeof x === 'string'))) ||
        (config.deadlineAt != null && !Number.isFinite(config.deadlineAt)) ||
        (config.maxOutputBytes !== undefined && (!Number.isSafeInteger(config.maxOutputBytes) || config.maxOutputBytes < 0)) ||
        (config.workspaceIdentity !== undefined && (!config.workspaceIdentity || typeof config.workspaceIdentity.path !== 'string' || typeof config.workspaceIdentity.id !== 'string')))
      throw failure('PROCESS_REJECTED', 'Invalid process input');
    if (config.instanceId !== instanceId || instanceId !== currentInstance()) throw failure('PROCESS_REJECTED', 'Container instance was replaced');
    if (config.nativeListenerPort !== undefined && !listenerPorts.includes(config.nativeListenerPort)) throw failure('PROCESS_REJECTED', 'Invalid native listener port');
    const seq = sequence(config.id, instanceId), fingerprint = digest(canonical(config));
    const existing = lookup(config.id);
    if (!existing) throw failure('PROCESS_REJECTED', 'Process ticket was not registered');
    if (existing.nativeListenerPort !== config.nativeListenerPort) throw failure('PROCESS_REJECTED', 'Process reservation class conflicted');
    if (existing.resource && (canonical(existing.resource) !== canonical(config.resource) || resources.get(existing.resource.key)?.id !== config.id || config.workspaceIdentity?.path !== '/workspace/.sandbox-agent-materialization' || config.workspaceIdentity?.id !== existing.resource.materializationId))
      throw failure('PROCESS_REJECTED', 'Process resource reservation was superseded');
    if (!existing.reserved) {
      if (existing.fingerprint !== fingerprint) throw failure('PROCESS_UNKNOWN', 'Process identity conflicted');
      return status(config.id);
    }
    if (config.workspaceIdentity) {
      let materialization;
      try { materialization = readFileSync(config.workspaceIdentity.path, 'utf8').trim(); } catch (error) { if (error.code !== 'ENOENT') throw error; }
      if (materialization !== config.workspaceIdentity.id) throw failure('PROCESS_REJECTED', 'Process workspace was replaced');
    }
    const dir = directory(config.id);
    if (existsSync(dir)) throw failure('PROCESS_UNKNOWN', 'Process admission outcome is unknown');
    if (config.nativeListenerPort === undefined && [...jobs.values()].filter(job => job.id !== config.id && job.nativeListenerPort === undefined && !job.acknowledged).length >= maximumJobs)
      throw failure('PROCESS_REJECTED', 'Unacknowledged process limit reached');
    const entry = { id: config.id, sequence: seq, argv: config.argv, cwd: config.cwd, instanceId, fingerprint, resource: existing.resource, deadlineAt: config.deadlineAt ?? null, nativeListenerPort: config.nativeListenerPort, completionRevision: null, acknowledged: false };
    // Retain admission before dispatch; only a confirmed group retirement settles it.
    jobs.set(config.id, entry);
    mkdirSync(dir);
    if (config.deadlineAt != null && config.deadlineAt <= Date.now()) {
      entry.exit = { code: -1, timedOut: true, truncated: false };
      return status(config.id);
    }
    const control = runJob(config, exit => {
      entry.exit = exit;
      refresh(entry);
      controllers.delete(config.id);
      for (const wake of waiters.get(config.id) ?? []) wake();
    });
    if (control) controllers.set(config.id, control);
    idleAt = Date.now() + idleMillis;
    return status(config.id);
  };
  methods = {
    touch: (millis) => {
      requireActive();
      if (!Number.isFinite(millis) || millis <= 0 || millis > 6 * 60 * 60_000) throw failure('PROCESS_REJECTED', 'Invalid idle policy');
      idleMillis = millis; idleAt = Date.now() + idleMillis;
    },
    protect: (id, millis) => {
      requireActive();
      if (millis === null) protections.delete(id);
      else {
        // A window, not a timestamp: the owner's clock is not this VM's clock.
        if (typeof id !== 'string' || !Number.isFinite(millis) || millis <= 0 || millis > 20 * 60_000) throw failure('PROCESS_REJECTED', 'Invalid operation deadline');
        if (!protections.has(id) && protections.size >= maximumJobs) throw failure('PROCESS_REJECTED', 'Operation limit reached');
        protections.set(id, Date.now() + millis);
      }
    },
    lease: (id, active, renewal) => {
      requireActive();
      if (typeof id !== 'string' || !id || typeof active !== 'boolean' || typeof renewal !== 'boolean') throw failure('PROCESS_REJECTED', 'Invalid execution lease');
      const saved = leases.get(id);
      if (active && renewal && (!saved || saved.completed)) throw failure('LEASE_LOST', 'Execution lease was retired');
      if (active && (!saved || saved.completed) && [...leases.values()].filter(lease => !lease.completed).length >= maximumJobs) throw failure('PROCESS_REJECTED', 'Execution lease limit reached');
      if (active || saved) leases.set(id, { completed: !active, expiresAt: Date.now() + 15 * 60_000 });
      const closed = [...leases].filter(([key, lease]) => key !== id && lease.completed);
      for (const [key] of closed.slice(0, Math.max(0, closed.length - 31))) leases.delete(key);
      idleAt = Date.now() + idleMillis;
    },
    park: () => {
      if (activeUntil() > Date.now()) throw failure('PROCESS_REJECTED', 'Active execution cannot be parked');
      retiring = true;
    },
    waitForPort: async (id, port, timeout) => {
      if (!Number.isInteger(port) || port < 1 || port > 65535 || !Number.isFinite(timeout) || timeout <= 0 || timeout > 20_000) throw failure('PROCESS_REJECTED', 'Invalid readiness request');
      const deadline = Date.now() + timeout;
      while (true) {
        const job = status(id, 0);
        if (job?.state !== 'running') throw failure(job?.state === 'exited' ? 'PROCESS_EXITED' : 'PROCESS_LOST', 'Process is unavailable');
        const ready = await new Promise(resolve => {
          const socket = connect({ host: '127.0.0.1', port });
          const finish = value => { socket.destroy(); resolve(value); };
          socket.once('connect', () => finish(true)); socket.once('error', () => finish(false));
          socket.setTimeout(Math.min(1000, Math.max(1, deadline - Date.now())), () => finish(false));
        });
        if (ready) {
          const latest = status(id, 0);
          if (latest?.state !== 'running') throw failure(latest?.state === 'exited' ? 'PROCESS_EXITED' : 'PROCESS_LOST', 'Process is unavailable');
          return;
        }
        if (Date.now() >= deadline) throw failure('ETIMEDOUT', 'Process port is not ready');
        await Bun.sleep(100);
      }
    },
    allocateTicket: (resource, listenerPort) => {
      requireActive();
      const nativeListenerPort = listenerPort ?? undefined;
      if (nativeListenerPort !== undefined && (!listenerPorts.includes(nativeListenerPort) || resource !== null)) throw failure('PROCESS_REJECTED', 'Invalid native listener port');
      if (resource !== null) {
        if (!resource || typeof resource.key !== 'string' || !/^[a-z][a-z0-9-]{0,63}$/.test(resource.key) || !Number.isSafeInteger(resource.generation) || resource.generation < 0 || typeof resource.materializationId !== 'string' || resource.materializationId !== readFileSync('/workspace/.sandbox-agent-materialization', 'utf8').trim()) throw failure('PROCESS_REJECTED', 'Invalid process resource');
        const previous = resources.get(resource.key);
        if (previous) {
          if (previous.generation > resource.generation) throw failure('PROCESS_REJECTED', 'Process resource generation retired');
          if (previous.generation === resource.generation) {
            if (previous.materializationId !== resource.materializationId) throw failure('PROCESS_REJECTED', 'Process resource workspace conflicted');
            return previous.id;
          }
        } else if (resources.size >= maximumJobs) throw failure('PROCESS_REJECTED', 'Process resource limit reached');
      }
      if (nativeListenerPort !== undefined) {
        const previous = [...jobs.values()].find(job => job.nativeListenerPort === nativeListenerPort);
        if (previous) {
          if (previous.reserved && previous.expiresAt > Date.now()) return previous.id;
          if (previous.reserved) retire(previous);
          else {
            const observed = status(previous.id, 0);
            if (observed.state === 'running') return previous.id;
            if (observed.state !== 'exited') throw failure('PROCESS_UNKNOWN', 'Native listener dispatch outcome is unknown');
            retire(previous);
          }
        }
      } else if ([...jobs.values()].filter(job => job.nativeListenerPort === undefined && !job.acknowledged).length >= maximumJobs) {
        throw failure('PROCESS_REJECTED', 'Unacknowledged process limit reached');
      }
      const id = 'process:' + instanceId + ':' + ++nextSequence;
      jobs.set(id, { id, instanceId, sequence: nextSequence, reserved: true, expiresAt: Date.now() + 20 * 60_000, nativeListenerPort, resource, acknowledged: false });
      if (resource !== null) resources.set(resource.key, { ...resource, id });
      return id;
    },
    launchJob: launch,
    observeJob: status,
    listJobs: () => [...jobs.values()].filter(entry => !entry.reserved).map(entry => status(entry.id, 0)),
    signalJob: (id, signal = 15) => {
      if (!Number.isInteger(signal) || !supportedSignals.has(signal)) throw failure('PROCESS_REJECTED', 'Unsupported process signal');
      const entry = lookup(id);
      if (!entry) throw failure('PROCESS_REJECTED', 'Process ticket was not registered');
      if (entry.reserved) {
        const cancelled = { id, sequence: sequence(id, instanceId), argv: [], instanceId, fingerprint: null, deadlineAt: null, nativeListenerPort: entry.nativeListenerPort, completionRevision: null, acknowledged: false };
        jobs.set(id, cancelled);
        mkdirSync(directory(id), { recursive: true });
        cancelled.exit = { code: -1, timedOut: false, truncated: false };
        refresh(cancelled);
        return null;
      }
      if (refresh(entry)) return null;
      if (!ownsJob({ ...entry, ...controllers.get(id)?.running })) throw failure('PROCESS_UNKNOWN', 'Process ownership was lost');
      const control = controllers.get(id);
      if (!control) throw failure('PROCESS_UNKNOWN', 'Process ownership was lost');
      control.kill(signal);
      return null;
    },
    acknowledgeJob: acknowledge,
    waitJob: async (id, maxBytes = 1048576, waitMillis = 20_000) => {
      if (!Number.isFinite(waitMillis) || waitMillis < 0 || waitMillis > 20_000) throw failure('PROCESS_REJECTED', 'Invalid process wait');
      const observed = status(id, 0);
      if (observed?.state === 'running' && waitMillis > 0) await new Promise(resolve => {
        const pending = waiters.get(id) ?? new Set();
        const wake = () => { clearTimeout(timer); pending.delete(wake); if (pending.size === 0) waiters.delete(id); resolve(); };
        const timer = setTimeout(wake, waitMillis);
        pending.add(wake); waiters.set(id, pending);
      });
      return status(id, maxBytes);
    },
  };
  mkdirSync('/run/kommunikasie/private', { recursive: true, mode: 0o700 });
  writeFileSync(TOKEN_PATH, token, { mode: 0o600 }); chmodSync(TOKEN_PATH, 0o600);

  const monitor = setInterval(() => {
    for (const [id, until] of protections) if (until <= Date.now()) protections.delete(id);
    if (Date.now() >= idleAt && activeUntil() <= Date.now()) process.exit(0);
    for (const entry of jobs.values()) if (entry.completionRevision == null) {
      const exit = refresh(entry);
      if (!exit && waiters.has(entry.id) && !ownsJob({ ...entry, ...controllers.get(entry.id)?.running })) {
        for (const wake of waiters.get(entry.id) ?? []) wake();
      }
    }
  }, 100);
  return { server, monitor };
  } catch (error) { server.stop(true); throw error; }
};

// The gateway observes authenticated long polls; socket lifetime never owns the child.
export const watchJob = id => {
  const emitter = new EventEmitter(); emitter.stdout = new EventEmitter(); emitter.stderr = new EventEmitter();
  queueMicrotask(async () => {
    let disconnected = false;
    while (true) {
      try {
        const job = await waitJob(id, maximumOutput, 20_000);
        if (!job) throw failure('PROCESS_UNKNOWN', 'Process receipt is missing');
        if (job.state === 'lost') {
          if (disconnected !== 'lost') emitter.emit('error', Object.assign(failure('PROCESS_UNKNOWN', 'Process ownership was lost'), { processState: 'lost' }));
          disconnected = 'lost';
          // Keep observing the same admission without pretending its child closed.
          await Bun.sleep(20_000);
          continue;
        }
        disconnected = false;
        if (job.state !== 'exited') continue;
        emitter.stdout.emit('data', Buffer.from(job.stdout)); emitter.stderr.emit('data', Buffer.from(job.stderr));
        emitter.emit('close', job.exitCode ?? -1, job);
        return;
      } catch (error) {
        if (!disconnected) emitter.emit('error', error);
        disconnected = true;
        if (error.code === 'PROCESS_RETIRED') { emitter.emit('close', -1); return; }
        await Bun.sleep(250);
      }
    }
  });
  return emitter;
};

if (import.meta.main) {
  if (process.argv.includes('--group')) runGroup(JSON.parse(await Bun.stdin.text()));
  else throw Error('Invalid process supervisor mode');
}
`;

/** Install the boot's source once, then serve until the container exits. */
// oxlint-disable-next-line no-restricted-properties -- quote the generated daemon module as JavaScript source, without runtime credentials.
const supervisorModule = JSON.stringify(PROCESS_SUPERVISOR_SOURCE);

export const PROCESS_BOOTSTRAP_SOURCE = String.raw`
import { existsSync, mkdirSync, readFileSync, renameSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { spawn, spawnSync } from 'node:child_process';
const instanceId = process.env.KOMMUNIKASIE_CONTAINER_INSTANCE;
const token = process.env.KOMMUNIKASIE_SUPERVISOR_TOKEN;
delete process.env.KOMMUNIKASIE_SUPERVISOR_TOKEN;
rmSync('/run/kommunikasie', { recursive: true, force: true });
rmSync('${PROCESS_DIRECTORY}', { recursive: true, force: true });
rmSync('${NATIVE_RECEIPTS_DIRECTORY}', { recursive: true, force: true });
mkdirSync('/run/kommunikasie/private', { recursive: true, mode: 0o700 });
writeFileSync('${PROCESS_INSTANCE_PATH}', instanceId);
// Install this boot's CA in both system trust formats before admitting commands.
// Rebuilding every unrelated certificate and running image hooks can happen later.
const certificatePath = '/etc/cloudflare/certs/cloudflare-containers-ca.crt';
if (existsSync(certificatePath)) {
  const installedPath = '/usr/local/share/ca-certificates/cloudflare-containers.crt';
  const pemPath = '/etc/ssl/certs/cloudflare-containers.pem';
  const bundlePath = '/etc/ssl/certs/ca-certificates.crt';
  const certificate = readFileSync(certificatePath, 'utf8').trim();
  const hash = spawnSync('openssl', ['x509', '-in', certificatePath, '-hash', '-noout'], { encoding: 'utf8' });
  if (hash.status !== 0 || !/^[a-f0-9]{8}$/.test(hash.stdout.trim())) throw Error('Container certificate installation failed');
  const previous = existsSync(installedPath) ? readFileSync(installedPath, 'utf8').trim() : '';
  const bundle = readFileSync(bundlePath, 'utf8');
  const retained = previous ? bundle.replace(previous, '') : bundle;
  writeFileSync(installedPath, certificate + '\n', { mode: 0o644 });
  rmSync(pemPath, { force: true });
  symlinkSync(installedPath, pemPath);
  const temporaryBundle = bundlePath + '.sandbox-' + process.pid;
  writeFileSync(temporaryBundle, retained.trimEnd() + '\n' + certificate + '\n', { mode: 0o644 });
  renameSync(temporaryBundle, bundlePath);
  for (let index = 0; ; index++) {
    const link = '/etc/ssl/certs/' + hash.stdout.trim() + '.' + index;
    try { symlinkSync(pemPath, link); break; }
    catch (error) {
      if (error.code !== 'EEXIST') throw error;
      try { if (readFileSync(link, 'utf8').trim() === certificate) break; }
      catch (error) { if (error.code !== 'ENOENT') throw error; }
    }
  }
  const rebuild = spawn('update-ca-certificates', [], { stdio: 'ignore', detached: true });
  const stopRebuild = () => {
    if (rebuild.pid !== undefined) {
      try { process.kill(-rebuild.pid, 'SIGKILL'); }
      catch (error) { if (error.code !== 'ESRCH') throw error; }
    }
  };
  process.once('exit', stopRebuild);
  rebuild.once('error', () => console.error('Container certificate-store rebuild failed to start'));
  rebuild.once('close', (code, signal) => {
    process.removeListener('exit', stopRebuild);
    if (code !== 0) console.error('Container certificate-store rebuild failed', { code, signal });
  });
}
writeFileSync('${PROCESS_OWNER_PATH}', ${supervisorModule}, { mode: 0o600 });
const { serve } = await import('${PROCESS_OWNER_PATH}');
await serve({ token, instanceId, idleMillis: 20 * 60_000 });
`;
