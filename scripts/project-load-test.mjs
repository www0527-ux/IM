import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { performance, monitorEventLoopDelay } from 'node:perf_hooks';

const database = process.env.IM_LOAD_DB;
const runDir = process.env.IM_LOAD_RUN_DIR;
const runID = process.env.IM_LOAD_RUN_ID;
assert.match(database ?? '', /^im_acceptance_[0-9_]+$/);
assert.ok(runDir && runID, 'Run project-load-test.ps1');
const seconds = Number(process.env.IM_LOAD_STAGE_SECONDS || 12);
const messageLimit = Number(process.env.IM_LOAD_MESSAGE_LIMIT || 600);
const claimRounds = Number(process.env.IM_LOAD_CLAIM_ROUNDS || 6);
const isolateStages = process.env.IM_LOAD_ISOLATE_STAGES === '1';
assert.ok(seconds > 0 && seconds <= 120 && messageLimit >= 50 && messageLimit <= 10000 && claimRounds > 0 && claimRounds <= 30);
const base = 'http://127.0.0.1:10010';
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const quote = value => `'${String(value).replaceAll("'", "''")}'`;
const levels = [10, 50, 100];
const cases = [], telemetry = [], packetIDs = [], trackedMessages = new Map(), sockets = [];
const observedIDs = new Set(), observedSeq = new Map(), outOfOrderPushes = [];
let currentPhase = 'setup', monitor, sampleTimer, sampling = false;
const output = { runID, database, result: 'RUNNING', methodology: {
  concurrency: levels, maxStageSeconds: seconds, maxMessagesPerStage: messageLimit, claimsRoundsPerLevel: claimRounds,
  claimsMaxStageSeconds: 120, maxSyncRequestsPerStage: 3000, warmupMessagesPerStage: 10,
  model: 'closed loop, one in-flight operation per worker; message stages end on time or request limit',
  messageBytes: 128, onlineConnections: 101, groupMembers: 121, manyConversationCount: 20,
  gateway: base, latency: 'client monotonic elapsed, no automatic retry in measurement',
  note: 'Bounded local load; short capped stages are bursts, not sustainable capacity. Driver shares the host.',
  claimLatencyIncludesRoundBarrier: false,
  drainOutboxBetweenStages: isolateStages,
}, cases, telemetry };
function save() { writeFileSync(join(runDir, 'results.json'), JSON.stringify(output, null, 2)); }
function command(executable, args, input, timeout = 45000) {
  return new Promise((resolve, reject) => {
    const child = spawn(executable, args, { windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'] });
    let stdout = '', stderr = '';
    const timer = setTimeout(() => { child.kill(); reject(new Error(`${executable} command timeout`)); }, timeout);
    child.stdout.on('data', value => { stdout += value; }); child.stderr.on('data', value => { stderr += value; });
    child.on('error', error => { clearTimeout(timer); reject(error); });
    child.on('close', code => { clearTimeout(timer); code === 0 ? resolve(stdout.trim()) : reject(new Error(`${executable} exit ${code}: ${stderr.slice(-600)}`)); });
    child.stdin.end(input);
  });
}
const docker = (...args) => command('docker', args);
const sql = query => command('docker', ['exec', '-i', 'im-learn-mysql', 'sh', '-c',
  'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot --batch --raw --skip-column-names "$1"', 'sh', database], query);
const rows = async query => { const raw = await sql(query); return raw ? raw.split('\n').map(line => JSON.parse(line)) : []; };
async function request(path, token, body) {
  const response = await fetch(base + path, { method: body === undefined ? 'GET' : 'POST',
    headers: { ...(token ? { Authorization: `Bearer ${token}` } : {}), ...(body ? { 'Content-Type': 'application/json' } : {}) },
    body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(15000),
  });
  const value = await response.json();
  if (value.code !== 200) {
    output.failureSamples ??= [];
    if (output.failureSamples.length < 20) output.failureSamples.push({ phase: currentPhase, path, code: value.code,
      httpStatus: response.status, message: value.message });
  }
  return { ...value, httpStatus: response.status };
}
function ok(value) { assert.equal(value.code, 200, `API error code ${value.code}, HTTP ${value.httpStatus}`); return value.data; }
async function until(label, test, timeout = 30000, interval = 250) {
  const end = Date.now() + timeout;
  while (Date.now() < end) { const value = await test(); if (value) return value; await delay(interval); }
  throw new Error(`Timeout: ${label}`);
}
function distribution(values) {
  if (!values.length) return { n: 0 };
  const sorted = [...values].sort((a, b) => a - b);
  const percentile = fraction => Number(sorted[Math.max(0, Math.ceil(sorted.length * fraction) - 1)].toFixed(2));
  return { n: values.length, p50Ms: percentile(.5), p95Ms: percentile(.95), p99Ms: percentile(.99), maxMs: percentile(1) };
}
async function pool(concurrency, total, action) {
  let next = 0;
  await Promise.all(Array.from({ length: concurrency }, (_, worker) => (async () => {
    while (next < total) { const index = next++; await action(index, worker); }
  })()));
}
async function snapshot() {
  return (await rows(`SELECT JSON_OBJECT('pending', SUM(status=0), 'processing', SUM(status=1),
    'published', SUM(status=2), 'retries', COALESCE(SUM(retry_count),0),
    'oldestUnpublishedSecs', TIMESTAMPDIFF(MICROSECOND, MIN(CASE WHEN status<>2 THEN created_time END), NOW(3))/1000000)
    FROM outbox_event`))[0];
}
async function sample() {
  if (sampling) return; sampling = true;
  const phase = currentPhase;
  try {
    const [outbox, dockerStats, mysqlConnections] = await Promise.all([
      snapshot(), docker('stats', '--no-stream', '--format', '{{json .}}', 'im-learn-mysql', 'im-learn-redis', 'im-learn-kafka'),
      rows(`SELECT JSON_OBJECT('ownedConnections',COUNT(*),'ownedActiveConnections',SUM(COMMAND<>'Sleep'),
        'globalConnected',(SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Threads_connected'))
        FROM information_schema.PROCESSLIST WHERE DB=${quote(database)}`),
    ]);
    telemetry.push({ at: new Date().toISOString(), phase, outbox, mysqlConnections: mysqlConnections[0], containers: dockerStats.split('\n').filter(Boolean).map(JSON.parse) });
    save();
  } catch (error) { telemetry.push({ phase, error: error.message }); }
  finally { sampling = false; }
}
async function seed() {
  const first = 910000000000000000n + BigInt(Date.now()) * 100n;
  const phoneBase = Date.now() % 80000000 + 10000000;
  const hash = await command(join(runDir, 'helper.exe'), ['hash']);
  const users = Array.from({ length: 121 }, (_, index) => ({ id: String(first + BigInt(index)), account: `137${phoneBase + index}` }));
  await sql(`INSERT INTO user(user_id,phone,password,nickname) VALUES ${users.map((user, index) => `(${user.id},${quote(user.account)},${quote(hash)},'load-${index}')`).join(',')};
    INSERT INTO user_balance(user_id,balance) VALUES ${users.map((user, index) => `(${user.id},${index === 0 ? 10000000 : 1000})`).join(',')};`);
  const groups = Array.from({ length: 65 }, (_, index) => String(first + 1000n + BigInt(index)));
  await sql(`INSERT INTO session(session_id,name,type) VALUES ${groups.map((id, index) => `(${id},'load-group-${index}',1)`).join(',')};`);
  for (const group of groups) await sql(`INSERT INTO user_session(user_id,session_id,role) VALUES ${users.map((user, index) => `(${user.id},${group},${index === 0 ? 0 : 2})`).join(',')};`);
  writeFileSync(join(runDir, 'fixtures.json'), JSON.stringify({ users, groups, initialSenderBalance: 10000000, initialOtherBalance: 1000 }, null, 2));
  console.log('Created isolated load data: 121 users, 65 groups');
}
if (process.argv.includes('--seed')) { await seed(); process.exit(0); }
const fixture = JSON.parse(readFileSync(join(runDir, 'fixtures.json'), 'utf8'));
const sessions = new Array(fixture.users.length);
function connect(index, observer = false) {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(`${base.replace('http:', 'ws:')}/ws/chat?access_token=${encodeURIComponent(sessions[index].token)}`);
    const pending = new Map();
    const readyTimer = setTimeout(() => { socket.close(); reject(new Error('WebSocket ready timeout')); }, 15000);
    sockets.push(socket);
    socket.addEventListener('error', () => { clearTimeout(readyTimer); reject(new Error('WebSocket connection error')); });
    socket.addEventListener('close', () => { for (const task of pending.values()) { clearTimeout(task.timer); task.resolve({ code: 'connection_closed' }); } pending.clear(); });
    socket.addEventListener('message', event => {
      const value = JSON.parse(event.data);
      if (value.eventType === 'connection.ready') {
        clearTimeout(readyTimer);
        resolve({ send(group, id) {
          return new Promise(done => {
            const timer = setTimeout(() => { pending.delete(id); done({ code: 'ack_timeout' }); }, 15000);
            pending.set(id, { resolve: done, timer });
            if (socket.readyState !== WebSocket.OPEN) { clearTimeout(timer); pending.delete(id); done({ code: 'connection_closed' }); return; }
            socket.send(JSON.stringify({ eventType: 'message.send', clientMessageId: id, sessionId: group,
              sessionType: 1, messageType: 0, body: { content: `load-${id}`.padEnd(128, 'x') } }));
          });
        } });
      } else if (value.eventType === 'message.ack' || value.eventType === 'message.error') {
        const task = pending.get(value.clientMessageId);
        if (task) { clearTimeout(task.timer); pending.delete(value.clientMessageId); task.resolve(value); }
      } else if (observer && value.eventType === 'message.new') {
        const message = value.message;
        if (!observedIDs.has(message.messageId)) {
          observedIDs.add(message.messageId);
          const previous = observedSeq.get(message.sessionId) ?? 0n;
          const seq = BigInt(message.seq);
          if (seq <= previous) outOfOrderPushes.push({ sessionId: message.sessionId, previous: String(previous), seq: String(seq) });
          observedSeq.set(message.sessionId, seq > previous ? seq : previous);
        }
        const entry = trackedMessages.get(value.message.clientMessageId);
        if (entry && entry.pushMs === undefined) entry.pushMs = performance.now() - entry.started;
      }
    });
  });
}
async function stage(name, concurrency, operation, { limit = 3000, expectedCodes = [], before, durationSeconds = seconds } = {}) {
  currentPhase = name;
  if (before) await before();
  console.log(`START ${name}, concurrency=${concurrency}`);
  const metric = { name, concurrency, expectedCodes, startedAt: new Date().toISOString(), outboxBefore: await snapshot() };
  const delays = [], successful = [], expectedDelays = [], errors = {};
  const histogram = monitorEventLoopDelay({ resolution: 10 }); histogram.enable();
  const cpuBefore = process.cpuUsage();
  const start = performance.now(), deadline = start + durationSeconds * 1000;
  let issued = 0;
  await Promise.all(Array.from({ length: concurrency }, (_, worker) => (async () => {
    while (performance.now() < deadline && issued < limit) {
      const index = issued++, time = performance.now();
      try {
        const value = await operation(worker, index);
        const result = typeof value === 'object' ? value.code : value;
        const elapsed = typeof value === 'object' ? value.latencyMs : performance.now() - time; delays.push(elapsed);
        if (result === 'ok') successful.push(elapsed);
        else if (expectedCodes.includes(result)) expectedDelays.push(elapsed);
        else errors[result] = (errors[result] || 0) + 1;
        // Stop this worker on a closed transport. Reusing it would produce
        // instant failures that consume the request budget without real load.
        if (result === 'connection_closed') break;
      } catch (error) { delays.push(performance.now() - time); const label = error.name === 'TimeoutError' ? 'timeout' : error.message; errors[label] = (errors[label] || 0) + 1; }
    }
  })()));
  const elapsed = (performance.now() - start) / 1000, cpu = process.cpuUsage(cpuBefore);
  histogram.disable();
  Object.assign(metric, { durationSecs: Number(elapsed.toFixed(3)), requests: issued, succeeded: successful.length,
    expectedRejections: expectedDelays.length, unexpectedErrors: Object.values(errors).reduce((sum, value) => sum + value, 0), errors,
    throughputRPS: Number((issued / elapsed).toFixed(2)), successRPS: Number((successful.length / elapsed).toFixed(2)),
    latencyAll: distribution(delays), latencySuccess: distribution(successful), latencyExpectedRejection: distribution(expectedDelays),
    endedBy: issued >= limit ? 'request limit (burst)' : 'time limit',
    driverCPUPercentOneCore: Number(((cpu.user + cpu.system) / (elapsed * 10000)).toFixed(2)),
    driverEventLoopP99Ms: Number((histogram.percentile(99) / 1e6).toFixed(2)), outboxAfter: await snapshot(),
  });
  cases.push(metric); save();
  console.log(`END ${name}: ${metric.successRPS} successful/s, p95=${metric.latencySuccess.p95Ms}ms, errors=${metric.unexpectedErrors}, outbox=${metric.outboxAfter.pending || 0}`);
  return metric;
}
try {
  await until('gateway ready', async () => { try { return (await fetch(base)).status === 200; } catch { return false; } }, 45000);
  await pool(6, fixture.users.length, async index => { sessions[index] = ok(await request('/api/user/loginPassword', undefined, { account: fixture.users[index].account, password: 'Test123!' })); });
  const clients = new Array(100);
  await pool(10, 100, async index => { clients[index] = await connect(index); });
  await connect(120, true);
  monitor = spawn('powershell.exe', ['-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', join(import.meta.dirname, 'load-monitor.ps1'), '-RunDir', runDir], { windowsHide: true, stdio: 'ignore' });
  sampleTimer = setInterval(() => void sample(), 5000);
  await sample();
  // Detect a slow publication baseline before creating thousands of queued events.
  currentPhase = 'relay-probe';
  const probeStart = performance.now();
  await pool(10, 10, async worker => {
    const id = randomUUID();
    trackedMessages.set(id, { phase: 'relay-probe', group: fixture.groups[64], started: performance.now() });
    const response = await clients[worker].send(fixture.groups[64], id);
    assert.equal(response.status, 'stored');
    trackedMessages.get(id).ack = true;
  });
  const probeAckSecs = (performance.now() - probeStart) / 1000;
  await until('10-event relay probe', async () => { const state = await snapshot(); return state.published >= 10; }, 30000, 500);
  const probeSecs = (performance.now() - probeStart) / 1000;
  output.relayProbe = { messages: 10, storedAckSecs: Number(probeAckSecs.toFixed(3)), allPublishedSecs: Number(probeSecs.toFixed(3)),
    measuredPublicationRPS: Number((10 / probeSecs).toFixed(2)), outbox: await snapshot() };
  save();
  if (probeSecs > 8) throw new Error('Relay probe took over 8 seconds for 10 stored messages; inspect publisher batching before larger load');
  currentPhase = 'low-load';
  const lowLoadAcks = [], lowLoadPushes = [];
  for (let index=0;index<20;index++) {
    const id=randomUUID(), started=performance.now();
    trackedMessages.set(id,{phase:'low-load',group:fixture.groups[64],started});
    const response=await clients[0].send(fixture.groups[64],id);
    assert.equal(response.status,'stored');
    trackedMessages.get(id).ack=true;
    lowLoadAcks.push(performance.now()-started);
    await until('low-load online delivery',()=>trackedMessages.get(id).pushMs!==undefined,10000,50);
    lowLoadPushes.push(trackedMessages.get(id).pushMs);
    await delay(200);
  }
  output.lowLoad={messages:20,concurrentRequests:1,pacing:'wait for delivery, then 200ms before next send',storedAck:distribution(lowLoadAcks),push:distribution(lowLoadPushes)};
  save();
  const messageCases = [];
  for (const [levelIndex, concurrency] of levels.entries()) {
    for (const kind of ['same-conversation', '20-conversations']) {
      const groups = kind === 'same-conversation' ? [fixture.groups[levelIndex]] : fixture.groups.slice(3 + levelIndex * 20, 23 + levelIndex * 20);
      // Warm permission lookup and connection path using messages excluded from latency samples.
      for (let warm = 0; warm < 10; warm++) assert.equal((await clients[warm % concurrency].send(groups[warm % groups.length], randomUUID())).status, 'stored');
      const name = `${kind}-c${concurrency}`;
      const metric = await stage(name, concurrency, async (worker, index) => {
        const id = randomUUID();
        trackedMessages.set(id, { phase: name, group: groups[index % groups.length], started: performance.now() });
        const result = await clients[worker].send(groups[index % groups.length], id);
        const entry = trackedMessages.get(id);
        entry.ack = result.status === 'stored'; entry.messageId = result.messageId;
        return entry.ack ? 'ok' : result.code || result.status || 'bad_ack';
      }, { limit: messageLimit });
      metric.groups = groups; messageCases.push(metric); save();
      if (isolateStages) {
        currentPhase = `${name}-drain`;
        const drainStarted = performance.now();
        await until(`${name} Outbox drain`, async () => { const value = await snapshot(); return !value.pending && !value.processing; }, 240000, 1000);
        await until(`${name} online delivery`, () => [...trackedMessages.values()].filter(entry => entry.phase === name && entry.ack).every(entry => entry.pushMs !== undefined));
        metric.drainAfterStageSecs = Number(((performance.now() - drainStarted) / 1000).toFixed(2));
        metric.pushLatency = distribution([...trackedMessages.values()].filter(entry => entry.phase === name && entry.ack).map(entry => entry.pushMs));
        metric.propagationFinishedAt = new Date().toISOString();
        save();
      }
    }
  }
  for (const concurrency of levels) {
    const name = `claim-hot-packet-c${concurrency}`;
    currentPhase = `${name}-setup`;
    const packets = [];
    for (let round = 0; round < claimRounds; round++) {
      const data = ok(await request('/api/redpacket/send', sessions[0].token, { clientMessageId: randomUUID(), sessionId: fixture.groups[63],
        sessionType: 1, redPacketType: 0, wrapperText: 'load', totalAmount: 100, totalCount: 100 }));
      packets.push(data.redPacketId); packetIDs.push(data.redPacketId);
    }
    let round = 0, completedInRound = 0, issuedInRound = 0;
    const successfulAmounts = [];
    // A barrier between 120-user rounds makes each round compete for one 100-share packet.
    const metric = await stage(name, concurrency, async () => {
      while (issuedInRound >= 120) { if (completedInRound >= 120) { round++; completedInRound = 0; issuedInRound = 0; } else await delay(1); }
      if (round >= packets.length) return 'round_finished';
      const userIndex = ++issuedInRound;
      const packet = packets[round];
      let response;
      const httpStarted = performance.now();
      try { response = await request(`/api/redpacket/${packet}/receive`, sessions[userIndex].token, {}); }
      finally { completedInRound++; }
      const latencyMs = performance.now() - httpStarted;
      if (response.code === 200) { successfulAmounts.push(response.data.amount); return { code: 'ok', latencyMs }; }
      return { code: `code_${response.code}`, latencyMs };
    }, { limit: claimRounds * 120, durationSeconds: 120, expectedCodes: ['code_73003'] });
    metric.packets = packets; metric.successfulAmountTotal = successfulAmounts.reduce((sum, value) => sum + value, 0);
    metric.roundsWith120UniqueClaimants = claimRounds; save();
  }
  currentPhase = 'drain-before-sync';
  const drainStart = performance.now();
  await until('Outbox drain', async () => { const value = await snapshot(); return !value.pending && !value.processing; }, 240000, 1000);
  output.outboxDrainBeforeSyncSecs = Number(((performance.now() - drainStart) / 1000).toFixed(2));
  await until('all ACKed messages observed online', () => [...trackedMessages.values()].filter(entry => entry.ack).every(entry => entry.pushMs !== undefined), 30000);
  for (const metric of messageCases) {
    const messages = [...trackedMessages.values()].filter(entry => entry.phase === metric.name && entry.ack);
    metric.pushLatency = distribution(messages.map(entry => entry.pushMs));
    metric.observedPushes = messages.length;
  }
  save();
  const syncGroup = fixture.groups[0];
  await until('content cache projected', async () => (await docker('exec', 'im-learn-redis', 'redis-cli', '--raw', 'EXISTS', `im:message-cache:{${syncGroup}}:50`)) === '1');
  const syncBody = { conversations: [{ sessionId: syncGroup, afterSeq: '0' }], limit: 50 };
  async function syncOperation() {
    const response = await request('/api/offline/sync', sessions[120].token, syncBody);
    if (response.code !== 200) return `code_${response.code}`;
    const page = response.data.conversations[0];
    if (page.messages.length !== 50 || page.nextSeq !== '50' || page.messages.some((message, index) => message.seq !== String(index + 1))) return 'bad_sync_result';
    return 'ok';
  }
  for (const concurrency of levels) await stage(`offline-cache-hit-c${concurrency}`, concurrency, syncOperation);
  await docker('exec', 'im-learn-redis', 'redis-cli', '--raw', 'UNLINK', ...Array.from({ length: 50 }, (_, index) => `im:message-cache:{${syncGroup}}:${index + 1}`));
  await stage('offline-mysql-fallback-c50', 50, syncOperation);
  assert.equal(await docker('exec', 'im-learn-redis', 'redis-cli', '--raw', 'EXISTS', `im:message-cache:{${syncGroup}}:1`), '0');
  currentPhase = 'verification';
  const audits = [];
  for (const id of packetIDs) {
    const raw = await command(join(runDir, 'helper.exe'), ['audit', id]);
    const audit = { ...JSON.parse(raw.split('\n').at(-1)), redPacketId: id };
    assert.deepEqual(audit.problems, []); audits.push(audit);
  }
  const invalidBalances = await rows(`SELECT JSON_OBJECT('userId',CAST(b.user_id AS CHAR),'actual',b.balance,'expected',CASE WHEN b.user_id=${fixture.users[0].id} THEN ${fixture.initialSenderBalance} ELSE ${fixture.initialOtherBalance} END+COALESCE(l.net,0))
    FROM user_balance b LEFT JOIN (SELECT user_id,SUM(amount) AS net FROM balance_log GROUP BY user_id) l ON l.user_id=b.user_id
    WHERE b.balance <> CASE WHEN b.user_id=${fixture.users[0].id} THEN ${fixture.initialSenderBalance} ELSE ${fixture.initialOtherBalance} END+COALESCE(l.net,0)`);
  assert.deepEqual(invalidBalances, []);
  for (const metric of cases.filter(value => value.packets)) {
    const relevant = audits.filter(value => metric.packets.includes(value.redPacketId));
    metric.databaseConfirmedClaims = relevant.reduce((sum, value) => sum + value.receiveRecordCount, 0);
    metric.committedWithoutSuccessResponse = metric.databaseConfirmedClaims - metric.succeeded;
    metric.completedPackets = relevant.filter(value => value.receiveRecordCount === 100).length;
    assert.ok(metric.committedWithoutSuccessResponse >= 0);
  }
  const sequences = await rows(`SELECT JSON_OBJECT('sessionId',CAST(m.session_id AS CHAR),'count',COUNT(*),'minSeq',MIN(seq),'maxSeq',MAX(seq),'lastSeq',s.last_seq)
    FROM message m JOIN conversation_sequence s ON m.session_id=s.session_id GROUP BY m.session_id,s.last_seq`);
  assert.ok(sequences.every(value => value.minSeq === 1 && value.count === value.maxSeq && value.maxSeq === value.lastSeq));
  const unknownMessages = [];
  for (const metric of messageCases) {
    const actual = Number(await sql(`SELECT COUNT(*) FROM message WHERE session_id IN (${metric.groups.join(',')})`));
    metric.persistedMessagesIncluding10Warmup = actual;
    if (actual !== metric.succeeded + 10) unknownMessages.push({ phase: metric.name, acked: metric.succeeded, actual });
  }
  assert.deepEqual(unknownMessages, []);
  assert.deepEqual(outOfOrderPushes, [], 'unique pushed messages must preserve per-conversation Seq order');
  output.verification = { packetAudits: audits, usersBalanceChecked: fixture.users.length, balanceMismatches: invalidBalances,
    sequenceChecks: sequences, messageCountMismatches: unknownMessages, outOfOrderPushes, observedUniqueMessages: observedIDs.size, finalOutbox: await snapshot() };
  output.kafkaConsumerLag = {};
  for (const group of ['message-writer-consumer-group','message-push-consumer-group','message-cache-consumer-group','redpacket-cache-build-group']) {
    output.kafkaConsumerLag[group] = await docker('exec', 'im-learn-kafka', '/opt/kafka/bin/kafka-consumer-groups.sh', '--bootstrap-server', 'localhost:9092', '--describe', '--group', `${group}-load-${runID}`);
  }
  output.result = cases.some(value => value.unexpectedErrors) ? 'COMPLETED_WITH_REQUEST_ERRORS' : 'PASS';
  output.finishedAt = new Date().toISOString(); save();
  console.log(`RESULT ${output.result}, ${audits.length} packet audits, ${fixture.users.length} balances, sequence checks passed`);
} catch (error) {
  output.result = 'FAIL'; output.failure = { phase: currentPhase, message: error.message }; save();
  console.error(`FAIL ${currentPhase}: ${error.message}`); process.exitCode = 1;
} finally {
  clearInterval(sampleTimer);
  while (sampling) await delay(100);
  for (const socket of sockets) socket.close();
  writeFileSync(join(runDir, 'monitor.stop'), 'stop');
  if (monitor) { await delay(500); monitor.kill(); }
  save();
}
