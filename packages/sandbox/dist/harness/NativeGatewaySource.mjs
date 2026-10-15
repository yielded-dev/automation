import { NATIVE_OUTPUT_LIMIT_BYTES } from "../NativeExecutionLimits.mjs";
//#region src/harness/NativeGatewaySource.ts
/** Runs in the sandbox's installed Bun, independently of any Worker connection. */
const NATIVE_GATEWAY_SOURCE = String.raw`
import { mkdirSync, readFileSync, writeFileSync, renameSync, rmSync, readdirSync, existsSync } from 'node:fs';
import { createHash, randomUUID } from 'node:crypto';
import { StringDecoder } from 'node:string_decoder';
import { launchJob, observeJob, watchJob, signalJob, acknowledgeJob } from '/run/kommunikasie/process-owner.mjs';

const port = Number(process.argv.at(-2));
const directory = process.argv.at(-1);
const instanceId = readFileSync('/run/kommunikasie/instance', 'utf8').trim();
const operations = new Map();
let quiescing = false;
const digest = value => createHash('sha256').update(value).digest('hex');
const canonical = value => JSON.stringify(value, (_, v) => v && typeof v === 'object' && !Array.isArray(v) ? Object.fromEntries(Object.keys(v).sort().map(k => [k, v[k]])) : v);
const path = id => directory + '/' + digest(id) + '.json';
const ticketSequence = id => {
  const prefix = 'process:' + instanceId + ':';
  if (typeof id !== 'string' || !id.startsWith(prefix) || !/^[1-9][0-9]*$/.test(id.slice(prefix.length))) throw Error('Invalid native process ticket');
  const sequence = Number(id.slice(prefix.length));
  if (!Number.isSafeInteger(sequence)) throw Error('Invalid native process ticket');
  return sequence;
};
const readFile = file => {
  try { return JSON.parse(readFileSync(file, 'utf8')); }
  catch (error) { if (error.code === 'ENOENT') return null; throw error; }
};
const releaseSlot = (slot, id) => {
  if (!slot || ![directory + '/.slots/' + port + '/', directory + '/.unacknowledged/' + port + '/'].some(prefix => slot.startsWith(prefix))) return;
  const owner = readFile(slot + '/owner.json');
  if (owner?.id === id && owner.instanceId === instanceId) rmSync(slot, { recursive: true, force: true });
};
const claimSlot = (kind, id) => {
  const slots = directory + '/' + kind + '/' + port;
  mkdirSync(slots, { recursive: true });
  for (let index = 0; index < ${128}; index++) {
    const slot = slots + '/' + index;
    try {
      mkdirSync(slot);
      writeFileSync(slot + '/owner.json', JSON.stringify({ id, instanceId }), { mode: 0o600 });
      return slot;
    } catch (error) { if (error.code !== 'EEXIST') throw error; }
  }
  throw Error('Native operation admission limit reached for this container instance');
};
const claimReceipt = id => {
  ticketSequence(id);
  const admissions = directory + '/.admissions';
  mkdirSync(admissions, { recursive: true });
  const historySlot = claimSlot('.unacknowledged', id);
  let slot;
  try {
    slot = claimSlot('.slots', id);
    mkdirSync(admissions + '/' + digest(id));
  } catch (error) {
    releaseSlot(slot, id); releaseSlot(historySlot, id);
    if (error.code !== 'EEXIST') throw error;
    throw Error('Native operation admission outcome is unknown');
  }
  return { slot, historySlot, listenerPort: port };
};
// Each elected listener owns its slot paths. Retain at most 32 acknowledged
// receipts per port (64 across both listeners), plus the receipt being quiesced.
const pruneAcknowledged = async (retiredThrough, keepId) => {
  if (!existsSync(directory)) return;
  for (const file of readdirSync(directory)) {
    if (!/^[a-f0-9]{64}\.json$/.test(file)) continue;
    const receipt = readFile(directory + '/' + file);
    if (!receipt || receipt.id === keepId || receipt.instanceId !== instanceId || receipt.listenerPort !== port) continue;
    if (retiredThrough !== undefined && ticketSequence(receipt.id) > retiredThrough) continue;
    if (!receipt.acknowledged || !receipt.childClosed || retiredThrough === undefined) {
      // A later helper can advance the floor past an earlier, still-retained
      // unread result. Absence at the supervisor proves retirement even when this
      // listener died before saving childClosed or the consumer's acknowledgement.
      try { await observeJob(receipt.id, 0); continue; }
      catch (error) { if (error.code !== 'PROCESS_RETIRED') throw error; }
    }
    releaseSlot(receipt.slot, receipt.id); releaseSlot(receipt.historySlot, receipt.id);
    rmSync(directory + '/.admissions/' + digest(receipt.id), { recursive: true, force: true });
    rmSync(directory + '/' + file, { force: true });
  }
};
const persist = op => {
  mkdirSync(directory, { recursive: true });
  const target = path(op.id);
  writeFileSync(target + '.tmp', JSON.stringify(op.receipt), { mode: 0o600 });
  renameSync(target + '.tmp', target);
};
const read = id => {
  try { const receipt = JSON.parse(readFileSync(path(id), 'utf8')); if (receipt.listenerPort !== port) throw Error('Native operation listener conflicted'); return receipt; }
  catch (error) { if (error.code === 'ENOENT') return null; throw error; }
};
const send = (ws, message) => { try { ws.send(JSON.stringify(message)); } catch {} };
const fail = (ws, id, message) => send(ws, { id, error: { code: -32000, message } });
const active = op => !op.receipt.childClosed;
const prune = () => {
  for (const [id, op] of operations) if (!active(op)) operations.delete(id);
};
const stop = async op => {
  op.backend?.close();
  if (op.child && !op.receipt.childClosed) {
    let signalled = true;
    try { await signalJob(op.id, 15); }
    catch { signalled = false; finish(op, 'quarantined'); }
    op.killTimer ??= setTimeout(() => {
      if (!op.receipt.childClosed) void signalJob(op.id, 9).catch(() => finish(op, 'quarantined'));
    }, 1000).unref();
    return signalled;
  }
  return true;
};
const finish = (op, state) => {
  op.receipt.state = state;
  persist(op);
  if (state === 'quarantined' && !op.receipt.childClosed) op.child?.emit('quarantined');
  if (op.receipt.childClosed) {
    releaseSlot(op.receipt.slot, op.id);
    clearTimeout(op.timer);
  }
};
const watch = op => {
  if (op.child || op.receipt.childClosed) return;
  const maximumOutputBytes = Math.min(op.config?.maxOutputBytes ?? 65536, ${NATIVE_OUTPUT_LIMIT_BYTES});
  op.receipt.stdout = ''; op.receipt.stderr = '';
  op.child = watchJob(op.id);
  const decoders = { stdout: new StringDecoder('utf8'), stderr: new StringDecoder('utf8') };
  const capture = (field, chunk) => {
    if (op.receipt.childClosed) return;
    const maximum = maximumOutputBytes;
    const previous = op.receipt[field];
    const next = previous + decoders[field].write(chunk);
    if (Buffer.byteLength(next) > maximum) { op.receipt.truncated = true; op.receipt[field] = new StringDecoder('utf8').write(Buffer.from(next).subarray(0, maximum)); }
    else op.receipt[field] = next;
  };
  op.child.stdout.on('data', chunk => capture('stdout', chunk));
  op.child.stderr.on('data', chunk => capture('stderr', chunk));
  op.child.on('error', error => {
    if (error.processState === 'lost') { finish(op, 'quarantined'); op.backend?.close(); }
    else console.warn('Native process observation disconnected', { code: 'PROCESS_OBSERVATION_DISCONNECTED' });
  });
  op.child.on('close', (code, exit) => {
    if (op.receipt.childClosed) return;
    op.receipt.truncated ||= exit?.truncated ?? false;
    op.receipt.timedOut ||= exit?.timedOut ?? false;
    op.receipt.childClosed = true;
    clearTimeout(op.killTimer);
    clearTimeout(op.timer);
    op.receipt.exitCode = code ?? -1;
    if (op.receipt.state === 'running') finish(op, 'completed');
    else { persist(op); releaseSlot(op.receipt.slot, op.id); }
    operations.delete(op.id);
  });
  if (Number.isFinite(op.receipt.deadlineAt)) op.timer = setTimeout(() => { op.receipt.timedOut = true; persist(op); stop(op); }, Math.max(1, op.receipt.deadlineAt - Date.now()));
};
const ensure = async (id, tokenHash, config) => {
  if (quiescing) throw Error('Native gateway is quiescing');
  ticketSequence(id);
  if (!config || config.id !== id || !Array.isArray(config.command) || config.command.length === 0 ||
      !config.command.every(x => typeof x === 'string') || typeof config.cwd !== 'string' ||
      (config.workspaceIdentity !== undefined && (!config.workspaceIdentity || typeof config.workspaceIdentity.path !== 'string' || typeof config.workspaceIdentity.id !== 'string')) ||
      (config.maxOutputBytes !== undefined && (!Number.isSafeInteger(config.maxOutputBytes) || config.maxOutputBytes < 0)) ||
      !Number.isFinite(config.deadlineAt) || !['codex', 'command'].includes(config.mode)) throw Error('Invalid native operation');
  if (config.workspaceIdentity !== undefined) {
    let materialization;
    try { materialization = readFileSync(config.workspaceIdentity.path, 'utf8').trim(); }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
    if (materialization !== config.workspaceIdentity.id) throw Error('Native operation workspace was replaced');
  }
  const fingerprint = digest(canonical(config));
  let op = operations.get(id);
  if (op) {
    if (op.receipt.tokenHash !== tokenHash || (op.receipt.fingerprint !== null && op.receipt.fingerprint !== fingerprint)) throw Error('Native operation identity conflict');
    return op;
  }
  const stored = read(id);
  if (stored && (stored.tokenHash !== tokenHash || (stored.fingerprint !== null && stored.fingerprint !== fingerprint))) throw Error('Native operation identity conflict');
  op = { id, config, clients: new Set(), rpcs: new Map(), pending: new Map(), items: new Map(), turns: new Map(), replayBytes: 0, next: 0,
    receipt: stored ?? { id, instanceId, tokenHash, fingerprint, deadlineAt: config.deadlineAt, state: 'running', childClosed: false, stdout: '', stderr: '', exitCode: null, timedOut: false, truncated: false } };
  if (stored) {
    if (stored.state === 'running') finish(op, 'quarantined');
    if (active(op)) {
      prune();
      if (operations.size >= ${128}) throw Error('Native operation limit reached');
      operations.set(id, op);
      watch(op);
    }
    return op;
  }
  // The supervisor's ticket fence rejects retired IDs before they can claim a slot.
  if (await observeJob(id, 0)) throw Error('Native process already has another admission');
  prune();
  if (operations.size >= ${128}) throw Error('Native operation limit reached');
  Object.assign(op.receipt, claimReceipt(id));
  if (Date.now() >= config.deadlineAt) { op.receipt.childClosed = true; op.receipt.timedOut = true; op.receipt.exitCode = -1; finish(op, config.mode === 'command' ? 'completed' : 'cancelled'); return op; }
  operations.set(id, op);
  // Commit ownership before spawn; this listener alone owns all subsequent dispatch.
  persist(op);
  op.nativeToken = randomUUID();
  const command = config.mode === 'codex' ? [...config.command, '--ws-auth', 'capability-token', '--ws-token-sha256', digest(op.nativeToken)] : config.command;
  const maximumOutputBytes = Math.min(config.maxOutputBytes ?? 65536, ${NATIVE_OUTPUT_LIMIT_BYTES});
  // The shared process owner retains output, group termination and the immutable deadline.
  let launchError;
  try {
    await launchJob({ id, argv: command, cwd: config.cwd, env: config.env, resource: config.resource,
      instanceId, deadlineAt: config.deadlineAt,
      workspaceIdentity: config.workspaceIdentity, maxOutputBytes: maximumOutputBytes });
  } catch (error) {
    // Matching retries observe the quarantined admission and never dispatch again.
    if (error.code === 'PROCESS_REJECTED' || error.code === 'PROCESS_RETIRED') op.receipt.childClosed = true;
    finish(op, 'quarantined');
    if (op.receipt.childClosed) { operations.delete(id); throw error; }
    launchError = error;
  }
  watch(op);
  if (launchError) throw launchError;
  return op;
};
// Native history omits items that are still streaming. Retain self-contained item
// snapshots at their native owner so reconnect never begins with an orphan delta.
const retain = (op, message) => {
  const p = message.params;
  if (!p || typeof message.method !== 'string') return;
  if (message.method === 'turn/started' || message.method === 'turn/completed') {
    op.turns.set(p.turn.id, p.turn);
    if (message.method === 'turn/completed') for (const item of p.turn.items ?? []) retain(op, { method: 'item/completed', params: { turnId: p.turn.id, item } });
  }
  if (message.method === 'thread/tokenUsage/updated') op.usage = message;
  if (message.method === 'turn/plan/updated') op.plan = message;
  const key = p.turnId + ':' + (p.item?.id ?? p.itemId);
  const previous = op.items.get(key);
  let item;
  if (message.method === 'item/started' || message.method === 'item/completed') {
    item = { ...p.item, status: message.method === 'item/started' ? 'inProgress' : 'completed' };
  } else if (previous && ['item/agentMessage/delta', 'item/plan/delta', 'item/reasoning/summaryTextDelta', 'item/reasoning/summaryPartAdded'].includes(message.method)) {
    item = { ...previous.item };
    if (message.method.startsWith('item/reasoning/')) {
      item.summary = [...(item.summary ?? [])];
      const index = p.summaryIndex ?? 0;
      if (index > item.summary.length) throw Error('Native summary skipped an item section');
      item.summary[index] = (item.summary[index] ?? '') + (p.delta ?? '');
    } else item.text = (item.text ?? '') + p.delta;
  }
  if (item) {
    const bytes = Buffer.byteLength(JSON.stringify(item));
    op.replayBytes += bytes - (previous?.bytes ?? 0);
    if (op.replayBytes > 8 * 1024 * 1024) throw Error('Native replay exceeded its byte limit');
    op.items.set(key, { turnId: p.turnId, item, bytes });
  }
};
const backend = async op => {
  if (op.connecting) return op.connecting;
  op.connecting = (async () => {
    while (op.receipt.state === 'running' && Date.now() < op.config.deadlineAt) {
      const ws = new WebSocket('ws://127.0.0.1:' + op.config.nativePort + '/', { headers: { Authorization: 'Bearer ' + op.nativeToken } });
      const opened = await new Promise(resolve => { ws.onopen = () => resolve(true); ws.onerror = () => resolve(false); });
      if (!opened) { ws.close(); await new Promise(resolve => setTimeout(resolve, 50)); continue; }
      op.backend = ws;
      ws.onmessage = event => {
        if (typeof event.data !== 'string' || Buffer.byteLength(event.data) > 8 * 1024 * 1024) { finish(op, 'quarantined'); stop(op); return; }
        let message;
        try { message = JSON.parse(event.data); retain(op, message); }
        catch { finish(op, 'quarantined'); stop(op); return; }
        if (message.id !== undefined && message.method === undefined) {
          const pending = op.pending.get(message.id);
          if (pending) { op.pending.delete(message.id); pending.resolve(message); }
        } else {
          for (const client of op.clients) if (client.data.live) send(client, message);
        }
      };
      ws.onclose = () => {
        // Never reconnect and replay an RPC after losing its native owner.
        if (op.receipt.state === 'running') finish(op, 'quarantined');
        for (const pending of op.pending.values()) pending.reject(Error('Native connection ownership was lost'));
        op.pending.clear();
        if (op.receipt.state !== 'cancelled') for (const client of op.clients) client.close(1012, 'Native connection ended');
      };
      return ws;
    }
    throw Error('Native operation is unavailable');
  })();
  return op.connecting;
};
const call = async (op, method, params) => {
  const ws = await backend(op);
  const id = ++op.next;
  const promise = new Promise((resolve, reject) => op.pending.set(id, { resolve, reject }));
  ws.send(JSON.stringify({ id, method, params }));
  return promise;
};
const once = (op, key, method, params) => {
  const fingerprint = digest(canonical({ method, params }));
  const previous = op.rpcs.get(key);
  if (previous) {
    if (previous.fingerprint !== fingerprint) throw Error('Native request identity conflict');
    return previous.promise;
  }
  if (op.rpcs.size >= 128) throw Error('Native request limit reached');
  // Register synchronously before the first await, independently of client lifetime.
  const entry = { fingerprint, promise: null };
  op.rpcs.set(key, entry);
  entry.promise = call(op, method, params);
  return entry.promise;
};
const snapshot = async op => {
  if (!op.threadId) throw Error('Native thread has no receipt');
  const response = await op.threadPromise;
  const started = (await op.rpcs.get('turn/start').promise).result?.turn;
  // Observe the accepted turn at its native owner, not through rollout history,
  // which can lag admission and report a still-running turn as interrupted.
  const turns = [...response.result.thread.turns];
  if (started && !turns.some(turn => turn.id === started.id)) turns.push(started);
  const replay = turns.map(turn => {
    const items = new Map(turn.items.map(item => [item.id, item]));
    for (const entry of op.items.values()) if (entry.turnId === turn.id) items.set(entry.item.id, entry.item);
    return { ...turn, ...(op.turns.get(turn.id) ?? (turn.id === started?.id ? started : {})), items: [...items.values()], replay: {
      ...(op.usage?.params.turnId === turn.id ? { tokenUsage: op.usage.params.tokenUsage.total } : {}),
      ...(op.plan?.params.turnId === turn.id ? { plan: op.plan.params.plan } : {})
    } }; 
  });
  return { ...response, result: { ...response.result, thread: { ...response.result.thread, turns: replay } } };
};
const dispatch = async (ws, message) => {
  const { id, tokenHash } = ws.data;
  let op = operations.get(id);
  if (op && op.receipt.tokenHash !== tokenHash) throw Error('Native operation authentication failed');
  if (message.method === 'gateway/ensure') {
    op = await ensure(id, tokenHash, message.params);
    send(ws, { id: message.id, result: { state: op.receipt.state, timedOut: op.receipt.timedOut } });
    return;
  }
  if (message.method === 'gateway/status') {
    const receipt = op?.receipt ?? read(id);
    if (!receipt) throw Error('Native operation has not been ensured');
    if (receipt.tokenHash !== tokenHash) throw Error('Native operation authentication failed');
    if (!op && receipt.state === 'running') finish({ id, receipt }, 'quarantined');
    send(ws, { id: message.id, result: receipt });
    return;
  }
  if (message.method === 'gateway/cancel') {
    if (!op) {
      const stored = read(id);
      if (stored && stored.tokenHash !== tokenHash) throw Error('Native operation authentication failed');
      if (!stored) {
        ticketSequence(id);
        const prior = await observeJob(id, 0);
        const cancelledBeforeDispatch = prior?.state === 'exited' && prior.command.length === 0;
        if (prior && !cancelledBeforeDispatch) throw Error('Native process already has another admission');
        const claims = claimReceipt(id);
        op = { id, receipt: { id, instanceId, tokenHash, fingerprint: null, childClosed: false, state: 'quarantined', stdout: '', stderr: '', exitCode: null, timedOut: false, truncated: false, ...claims } };
        persist(op);
        try { await signalJob(id, 15); op.receipt.childClosed = true; }
        catch (error) { finish(op, 'quarantined'); throw error; }
      } else op = { id, receipt: stored };
    }
    if (!op.child && active(op)) {
      finish(op, 'quarantined');
      throw Error('Native owner was lost; cancellation awaits its original deadline');
    }
    finish(op, 'cancelled');
    if (!(await stop(op))) throw Error('Native process ownership was lost; child closure is unknown');
    if (op.child && !op.receipt.childClosed) await new Promise((resolve, reject) => {
      const cleanup = () => { op.child.off('close', closed); op.child.off('quarantined', lost); };
      const closed = () => { cleanup(); resolve(); };
      const lost = () => { cleanup(); reject(Error('Native process ownership was lost; child closure is unknown')); };
      op.child.once('close', closed); op.child.once('quarantined', lost);
      if (op.receipt.state === 'quarantined') lost();
    });
    if (!active(op)) operations.delete(id);
    send(ws, { id: message.id, result: {} });
    return;
  }
  if (message.method === 'gateway/acknowledge') {
    const receipt = op?.receipt ?? read(id);
    if (!receipt) {
      try { await observeJob(id, 0); }
      catch (error) { if (error.code === 'PROCESS_RETIRED') { send(ws, { id: message.id, result: {} }); return; } throw error; }
      throw Error('Native operation has no retained result');
    }
    if (receipt.tokenHash !== tokenHash) throw Error('Native operation authentication failed');
    if (receipt.childClosed !== true || receipt.state === 'running') {
      const closed = await observeJob(id, ${NATIVE_OUTPUT_LIMIT_BYTES});
      if (closed?.state !== 'exited') throw Error('Native operation is not confirmed closed');
      // The consumer retained its terminal result, but the listener's watcher may
      // still be awaiting the same close or have died before persisting it.
      receipt.stdout = closed.stdout; receipt.stderr = closed.stderr;
      receipt.exitCode = closed.exitCode; receipt.timedOut ||= closed.timedOut;
      receipt.truncated ||= closed.truncated; receipt.childClosed = true;
      if (receipt.state === 'running') receipt.state = 'completed';
      if (op) { clearTimeout(op.killTimer); clearTimeout(op.timer); operations.delete(id); }
      persist({ id, receipt });
      releaseSlot(receipt.slot, id);
    }
    // A rejected launch may not have created a process record. Fence it before retiring the gateway receipt.
    try { if (!(await observeJob(id, 0))) await signalJob(id, 15); }
    catch (error) { if (error.code !== 'PROCESS_RETIRED') throw error; }
    const { retiredThrough } = await acknowledgeJob(id);
    receipt.acknowledged = true;
    persist({ id, receipt });
    releaseSlot(receipt.slot, id); releaseSlot(receipt.historySlot, id);
    await pruneAcknowledged(retiredThrough, id);
    send(ws, { id: message.id, result: {} });
    return;
  }
  if (message.method === 'gateway/quiesce') {
    const receipt = op?.receipt ?? read(id);
    if (!receipt || receipt.tokenHash !== tokenHash) throw Error('Native operation authentication failed');
    if (receipt.state !== 'completed' || receipt.childClosed !== true) throw Error('Native preparation child is not closed');
    prune();
    const ready = ![...operations.values()].some(active);
    if (ready) quiescing = true;
    send(ws, { id: message.id, result: { quiesced: ready } });
    if (ready) {
      // Preserve every receipt and let the acknowledgement's socket close first.
      void server.stop();
      setTimeout(() => { void server.stop(true); }, 1000).unref();
    }
    return;
  }
  if (!op) throw Error('Native operation has not been ensured');
  if (op.config.mode !== 'codex' || op.receipt.state !== 'running') throw Error('Native operation is unavailable: ' + op.receipt.state);
  op.clients.add(ws);
  if (message.method === 'initialized') return;
  let response;
  if (message.method === 'initialize' || message.method === 'account/login/start') {
    response = await once(op, message.method, message.method, message.params);
    if (message.method === 'initialize' && !op.initialized) { op.initialized = true; op.backend.send(JSON.stringify({ method: 'initialized', params: {} })); }
  } else if (message.method === 'thread/start' || message.method === 'thread/resume') {
    ws.data.live = false;
    if (!op.threadPromise) op.threadPromise = call(op, message.method, message.params).then(result => { op.threadId = result.result?.thread?.id; return result; });
    response = await op.threadPromise;
    if (op.threadId && op.rpcs.has('turn/start')) {
      response = await snapshot(op);
    }
    send(ws, { ...response, id: message.id });
    if (op.usage) send(ws, op.usage);
    if (op.plan) send(ws, op.plan);
    ws.data.live = true;
    return;
  } else if (message.method === 'turn/start' || message.method === 'turn/steer') {
    response = await once(op, message.method === 'turn/start' ? 'turn/start' : String(message.id), message.method, message.params);
    send(ws, { ...response, id: message.id });
    return;
  } else throw Error('Unsupported native request');
  send(ws, { ...response, id: message.id });
};

// Binding is the election. A duplicate bootstrap exits before any child or ledger write.
const server = Bun.serve({ port, hostname: '0.0.0.0', idleTimeout: 0,
  fetch(request, server) {
    const token = request.headers.get('authorization')?.replace(/^Bearer /, '');
    const id = decodeURIComponent(new URL(request.url).pathname.slice(1));
    if (!token || !id || id.length > 512) return new Response('Unauthorized', { status: 401 });
    if (server.upgrade(request, { data: { id, tokenHash: digest(token), live: false } })) return;
    return new Response('Upgrade required', { status: 426 });
  },
  websocket: {
    maxPayloadLength: 8 * 1024 * 1024,
    message(ws, text) {
      let message;
      try { message = JSON.parse(String(text)); }
      catch { ws.close(1003, 'Invalid native request'); return; }
      dispatch(ws, message).catch(error => fail(ws, message.id, error.message));
    },
    close(ws) { operations.get(ws.data.id)?.clients?.delete(ws); }
  }
});
await pruneAcknowledged().catch(error => {
  if (error.code !== 'PROCESS_UNKNOWN') throw error;
  console.warn('Native receipt pruning delayed');
});
`;
//#endregion
export { NATIVE_GATEWAY_SOURCE };
