import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { performance, monitorEventLoopDelay } from 'node:perf_hooks';

const runDir = process.env.IM_BATCH_PERF_TRIAL;
const database = process.env.IM_BATCH_PERF_DB;
const variant = process.env.IM_BATCH_PERF_VARIANT;
const total = Number(process.env.IM_BATCH_PERF_MESSAGES || 600);
assert.ok(runDir && ['baseline', 'current'].includes(variant));
assert.match(database ?? '', /^im_batch_perf_[0-9_]+_(baseline|current)_\d+$/);
const fixture = JSON.parse(readFileSync(join(runDir, 'fixtures.json'), 'utf8'));
const tracked = new Map(), sockets = [];
const output = { variant, database, startedAt: new Date().toISOString(), result: 'RUNNING', cases: [], methodology: {
  messagesPerCase: total, concurrency: [10, 50, 100], conversations: [1, 20], senders: 100, groupMembers: 101,
  messageBytes: 128, model: 'closed loop, exactly one in-flight request per sender, fixed-count bursts',
  warmup: '100 messages per case, excluded from measurements, drain before measurement',
  note: 'Same Windows host and shared local Docker resources. This is a bounded comparison, not sustainable capacity.',
  login: 'Fixtures generated with project JWT/session functions; live WebSocket authentication and permission RPC remain enabled',
} };
const save = () => writeFileSync(join(runDir, 'results.json'), JSON.stringify(output, null, 2));
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
function command(args, input) {
  return new Promise((resolve, reject) => {
    const child = spawn('docker', args, { windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'] });
    let stdout = '', stderr = '';
    const timer = setTimeout(() => { child.kill(); reject(new Error('docker command timed out')); }, 45000);
    child.stdout.on('data', data => { stdout += data; });
    child.stderr.on('data', data => { stderr += data; });
    child.on('error', error => { clearTimeout(timer); reject(error); });
    child.on('close', code => { clearTimeout(timer); code === 0 ? resolve(stdout.trim()) : reject(new Error(`docker failed: ${stderr.slice(-700)}`)); });
    child.stdin.end(input);
  });
}
const sql = query => command(['exec', '-i', 'im-learn-mysql', 'sh', '-c',
  'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot --batch --raw --skip-column-names "$1"', 'sh', database], query);
const rows = async query => { const raw = await sql(query); return raw ? raw.split('\n').map(JSON.parse) : []; };
function distribution(values) {
  if (!values.length) return { n: 0 };
  const sorted = [...values].sort((a, b) => a - b);
  const p = fraction => Number(sorted[Math.max(0, Math.ceil(sorted.length * fraction) - 1)].toFixed(2));
  return { n: sorted.length, p50Ms: p(.5), p95Ms: p(.95), p99Ms: p(.99), maxMs: p(1) };
}
async function pool(concurrency, count, action) {
  let next = 0;
  await Promise.all(Array.from({ length: concurrency }, (_, worker) => (async () => {
    while (next < count) { const index = next++; await action(index, worker); }
  })()));
}
async function waitUntil(test, timeout = 15000) {
  const deadline = performance.now() + timeout;
  while (performance.now() < deadline) { if (await test()) return true; await delay(50); }
  return false;
}
function connect(index) {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(`ws://127.0.0.1:19101/ws/chat?access_token=${encodeURIComponent(fixture.users[index].token)}`);
    sockets.push(socket);
    const pending = new Map();
    const timer = setTimeout(() => { socket.close(); reject(new Error('WebSocket ready timed out')); }, 15000);
    socket.addEventListener('error', () => { clearTimeout(timer); reject(new Error('WebSocket connection failed')); });
    socket.addEventListener('close', () => {
      for (const task of pending.values()) { clearTimeout(task.timer); task.resolve({ code: 'connection_closed' }); }
      pending.clear();
    });
    socket.addEventListener('message', event => {
      const value = JSON.parse(event.data);
      if (value.eventType === 'connection.ready') {
        clearTimeout(timer);
        resolve({ send(group, id) {
          return new Promise(done => {
            if (socket.readyState !== WebSocket.OPEN) { done({ code: 'connection_closed' }); return; }
            const timer = setTimeout(() => { pending.delete(id); done({ code: 'ack_timeout' }); }, 15000);
            pending.set(id, { resolve: done, timer });
            socket.send(JSON.stringify({ eventType: 'message.send', clientMessageId: id, sessionId: group, sessionType: 1,
              messageType: 0, body: { content: `batch-perf-${id}`.padEnd(128, 'x') } }));
          });
        } });
      } else if (value.eventType === 'message.ack' || value.eventType === 'message.error') {
        const task = pending.get(value.clientMessageId);
        if (task) { clearTimeout(task.timer); pending.delete(value.clientMessageId); task.resolve(value); }
      } else if (index === 100 && value.eventType === 'message.new') {
        const item = tracked.get(value.message.clientMessageId);
        if (item && item.pushMs === undefined) { item.pushMs = performance.now() - item.started; item.pushSeq = value.message.seq; }
      }
    });
  });
}
async function statementCounts() {
  return (await rows(`SELECT JSON_OBJECT(
    'counterUpdates',COALESCE(SUM(IF(LOWER(DIGEST_TEXT) LIKE 'update%conversation_sequence%',COUNT_STAR,0)),0),
    'messageInserts',COALESCE(SUM(IF(LOWER(DIGEST_TEXT) LIKE 'insert%into%message%',COUNT_STAR,0)),0),
    'commits',COALESCE(SUM(IF(DIGEST_TEXT='COMMIT',COUNT_STAR,0)),0))
    FROM performance_schema.events_statements_summary_by_digest WHERE SCHEMA_NAME='${database}'`))[0];
}
async function recover(groups) {
  const recovered = new Set();
  for (const group of groups) {
    let cursor = '0';
    for (let pageNumber = 0; pageNumber < 100; pageNumber++) {
      const response = await fetch('http://127.0.0.1:18105/api/offline/sync', { method: 'POST',
        headers: { Authorization: `Bearer ${fixture.users[100].token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ conversations: [{ sessionId: group, afterSeq: cursor }], limit: 200 }),
        signal: AbortSignal.timeout(15000) });
      const value = await response.json();
      assert.equal(value.code, 200, 'sync failed');
      const page = value.data.conversations[0];
      for (const message of page.messages) recovered.add(message.clientMessageId);
      if (!page.hasMore) break;
      assert.ok(BigInt(page.nextSeq) > BigInt(cursor), 'sync cursor stalled');
      cursor = page.nextSeq;
    }
  }
  return recovered;
}

const clients = [];
try {
  save();
  assert.ok(await waitUntil(async () => {
    try { await fetch('http://127.0.0.1:18105/', { signal: AbortSignal.timeout(1000) }); return true; } catch { return false; }
  }, 45000), 'offline service not ready');
  // Wait for RPC registration and Kafka group startup outside measurements.
  await delay(3000);
  for (let attempt = 0; attempt < 30; attempt++) {
    try { clients[0] = await connect(0); break; } catch { await delay(500); }
  }
  assert.ok(clients[0], 'realtime service not ready');
  await pool(10, 100, async index => { clients[index + 1] = await connect(index + 1); });
  await delay(1000);

  const sendTracked = async (client, group, phase) => {
    const id = randomUUID(), started = performance.now();
    const item = { id, group, phase, started };
    tracked.set(id, item);
    const response = await client.send(group, id);
    item.ackMs = performance.now() - started;
    item.ok = response.status === 'stored';
    item.error = item.ok ? undefined : response.code || response.status || 'bad_ack';
    return item;
  };

  const lowAcks = [], lowPushes = [];
  for (let i = 0; i < 25; i++) {
    const item = await sendTracked(clients[0], fixture.groups[64], 'low-load');
    assert.ok(item.ok, `low load failed: ${item.error}`);
    assert.ok(await waitUntil(() => item.pushMs !== undefined), 'low load push lost');
    if (i >= 5) { lowAcks.push(item.ackMs); lowPushes.push(item.pushMs); }
    await delay(100);
  }
  output.lowLoad = { messages: 20, ack: distribution(lowAcks), push: distribution(lowPushes) };
  save();

  let groupOffset = 0;
  for (const concurrency of [10, 50, 100]) {
    for (const groupCount of [1, 20]) {
      const groups = fixture.groups.slice(groupOffset, groupOffset + groupCount); groupOffset += groupCount;
      const name = `${groupCount === 1 ? 'hot' : 'many'}-c${concurrency}`;
      console.log(`START ${variant} ${name}`);
      const warm = [];
      await pool(10, 100, async index => {
        const item = await sendTracked(clients[index], groups[index % groups.length], 'warmup');
        assert.ok(item.ok, `warmup failed: ${item.error}`); warm.push(item);
      });
      assert.ok(await waitUntil(() => warm.every(item => item.pushMs !== undefined), 30000), 'warmup notifications did not drain');
      const before = await statementCounts();
      const items = [], errors = {};
      const histogram = monitorEventLoopDelay({ resolution: 10 }); histogram.enable();
      const cpuBefore = process.cpuUsage(), start = performance.now();
      await pool(concurrency, total, async (index, worker) => {
        const item = await sendTracked(clients[worker], groups[index % groups.length], name);
        items.push(item);
        if (!item.ok) errors[item.error] = (errors[item.error] || 0) + 1;
      });
      const seconds = (performance.now() - start) / 1000, cpu = process.cpuUsage(cpuBefore);
      histogram.disable();
      const after = await statementCounts();
      const successful = items.filter(item => item.ok);
      const metric = { name, concurrency, groupCount, requests: items.length, succeeded: successful.length,
        errors, errorCount: items.length - successful.length, durationSecs: Number(seconds.toFixed(3)),
        ackRPS: Number((successful.length / seconds).toFixed(2)), ack: distribution(successful.map(item => item.ackMs)),
        statementCounts: Object.fromEntries(Object.keys(after).map(key => [key, after[key] - before[key]])),
        driverCPUPercentOneCore: Number(((cpu.user + cpu.system) / (seconds * 10000)).toFixed(2)),
        driverEventLoopP99Ms: Number((histogram.percentile(99) / 1e6).toFixed(2)) };
      metric.statementCountCoverage = after.counterUpdates > 0 && after.messageInserts > 0
        ? 'DML digests available'
        : 'Prepared DML not visible in this digest table; zero DML counts are unavailable, not zero executions. COMMIT observations are approximate.';
      output.cases.push(metric); save();
      const drainStarted = performance.now();
      await waitUntil(() => successful.every(item => item.pushMs !== undefined), 15000);
      metric.drainWaitSecs = Number(((performance.now() - drainStarted) / 1000).toFixed(3));
      metric.pushed = successful.filter(item => item.pushMs !== undefined).length;
      metric.pushMissing = successful.length - metric.pushed;
      metric.push = distribution(successful.filter(item => item.pushMs !== undefined).map(item => item.pushMs));
      const actual = await rows(`SELECT JSON_OBJECT('sessionId',CAST(m.session_id AS CHAR),'count',COUNT(*),'minSeq',MIN(m.seq),
        'maxSeq',MAX(m.seq),'lastSeq',s.last_seq) FROM message m JOIN conversation_sequence s ON m.session_id=s.session_id
        WHERE m.session_id IN (${groups.join(',')}) GROUP BY m.session_id,s.last_seq`);
      assert.ok(actual.every(row => row.minSeq === 1 && row.count === row.maxSeq && row.maxSeq === row.lastSeq), 'Seq has holes');
      metric.databaseMessages = actual.reduce((sum, row) => sum + row.count, 0);
      metric.sequenceChecks = actual;
      metric.committedWithoutStoredAck = metric.databaseMessages - warm.length - successful.length;
      const recovered = await recover(groups);
      metric.syncMissing = successful.filter(item => !recovered.has(item.id)).length;
      assert.equal(metric.syncMissing, 0, 'stored message missing from sync');
      assert.ok(metric.committedWithoutStoredAck >= 0, 'stored ACK without persisted message');
      save();
      console.log(`END ${variant} ${name}: ${metric.ackRPS}/s ACK p95=${metric.ack.p95Ms}ms push p95=${metric.push.p95Ms}ms errors=${metric.errorCount} missedPush=${metric.pushMissing} missedSync=${metric.syncMissing}`);
    }
  }
  output.result = output.cases.some(item => item.errorCount || item.pushMissing || item.committedWithoutStoredAck) ? 'COMPLETED_WITH_DELIVERY_OR_REQUEST_ERRORS' : 'PASS';
  output.finishedAt = new Date().toISOString(); save();
  console.log(`RESULT ${variant}: ${output.result}`);
} catch (error) {
  output.result = 'FAIL'; output.failure = error.stack; save();
  console.error(error.message); process.exitCode = 1;
} finally {
  for (const socket of sockets) socket.close();
  save();
}
