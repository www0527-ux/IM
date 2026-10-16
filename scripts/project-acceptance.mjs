import assert from 'node:assert/strict';
import { execFileSync, spawn } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';

const database = process.env.IM_ACCEPTANCE_DB;
const runDir = process.env.IM_ACCEPTANCE_RUN_DIR;
const runID = process.env.IM_ACCEPTANCE_RUN_ID;
assert.match(database ?? '', /^im_acceptance_[0-9_]+$/);
assert.ok(runDir && runID, 'Run through project-acceptance.ps1');
const base = 'http://127.0.0.1:10010';
const helperPath = join(runDir, 'helper.exe');
const fixturePath = join(runDir, 'fixtures.json');
const results = [];
const sockets = [];
const packetIDs = new Set();
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const quote = value => `'${String(value).replaceAll("'", "''")}'`;
function docker(...args) {
  return execFileSync('docker', args, { encoding: 'utf8', timeout: 45000 }).trim();
}
function sql(query) {
  return execFileSync('docker', ['exec', '-i', 'im-learn-mysql', 'sh', '-c',
    'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot --batch --raw --skip-column-names "$1"', 'sh', database],
    { input: query, encoding: 'utf8', timeout: 45000 }).trim();
}
function number(query) { return Number(sql(query)); }
function rows(query) { const value = sql(query); return value ? value.split('\n').map(line => JSON.parse(line)) : []; }
function redis(...args) { return docker('exec', 'im-learn-redis', 'redis-cli', '--raw', ...args.map(String)); }
function helper(mode, id) {
  return new Promise((resolve, reject) => {
    const child = spawn(helperPath, [mode, String(id)], { windowsHide: true, env: process.env });
    let stdout = '', stderr = '';
    child.stdout.on('data', chunk => { stdout += chunk; });
    child.stderr.on('data', chunk => { stderr += chunk; });
    child.on('error', reject);
    child.on('close', code => code === 0 ? resolve(JSON.parse(stdout.trim().split('\n').at(-1))) :
      reject(new Error(`Helper ${mode} failed: ${stderr.slice(-1500)}`)));
  });
}
async function until(description, condition, timeout = 30000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const value = await condition();
    if (value) return value;
    await delay(200);
  }
  throw new Error(`Timed out: ${description}`);
}
async function test(name, action) {
  const start = Date.now();
  try {
    const evidence = await action();
    results.push({ name, result: 'PASS', durationMs: Date.now() - start, evidence });
    console.log(`PASS ${name}`);
    save();
  } catch (error) {
    results.push({ name, result: 'FAIL', durationMs: Date.now() - start, error: error.message });
    console.log(`FAIL ${name}: ${error.message}`);
    save();
    throw error;
  }
}
function save() {
  writeFileSync(join(runDir, 'results.json'), JSON.stringify({ runID, database, results, packetIDs: [...packetIDs] }, null, 2));
}
async function request(path, token, body, method = body === undefined ? 'GET' : 'POST') {
  const response = await fetch(`${base}${path}`, {
    method, headers: { ...(token ? { Authorization: `Bearer ${token}` } : {}), ...(body ? { 'Content-Type': 'application/json' } : {}) },
    body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(20000),
  });
  const text = await response.text();
  let value; try { value = JSON.parse(text); } catch { value = { raw: text }; }
  return { status: response.status, ...value };
}
function ok(response) { assert.equal(response.code, 200, `Business response: ${JSON.stringify(response)}`); return response.data; }
async function login(account) { return ok(await request('/api/user/loginPassword', undefined, { account, password: 'Test123!' })); }
async function connect(token) {
  const socket = new WebSocket(`${base.replace('http:', 'ws:')}/ws/chat?access_token=${encodeURIComponent(token)}`);
  const events = [];
  socket.addEventListener('message', event => { events.push(JSON.parse(event.data)); });
  await new Promise((resolve, reject) => { socket.addEventListener('open', resolve, { once: true }); socket.addEventListener('error', () => reject(new Error('WebSocket handshake failed')), { once: true }); });
  await until('connection.ready', () => events.some(event => event.eventType === 'connection.ready'));
  sockets.push(socket);
  return { socket, events };
}
async function sendChat(client, fixture, content, clientMessageId = randomUUID()) {
  const start = client.events.length;
  client.socket.send(JSON.stringify({ eventType: 'message.send', clientMessageId,
    sessionId: fixture.singleID, sessionType: 0, receiverId: fixture.users[1].id,
    messageType: 0, body: { content } }));
  return until('message response', () => client.events.slice(start).find(event => event.clientMessageId === clientMessageId));
}
async function sync(token, sessionId, afterSeq = '0', limit = 200) {
  return ok(await request('/api/offline/sync', token, { conversations: [{ sessionId, afterSeq }], limit })).conversations[0];
}
function packetFacts(id) {
  return rows(`SELECT JSON_OBJECT('status',status,'received',received_amount,'count',received_count,'total',total_amount,'totalCount',total_count) FROM red_packet WHERE red_packet_id=${id}`)[0];
}
async function audit(id) { const value = await helper('audit', id); assert.deepEqual(value.problems, []); return value; }

if (process.argv.includes('--seed')) {
  const firstID = 900000000000000000n + BigInt(Date.now()) * 100n;
  const phoneBase = Date.now() % 80000000 + 10000000;
  const hash = execFileSync(helperPath, ['hash'], { encoding: 'utf8' }).trim();
  const users = Array.from({ length: 16 }, (_, index) => ({ id: String(firstID + BigInt(index)), account: `139${phoneBase + index}` }));
  const singleID = String(firstID + 100n), groupID = String(firstID + 101n);
  for (const [index, user] of users.entries()) {
    sql(`INSERT INTO user(user_id,phone,password,nickname) VALUES(${user.id},${quote(user.account)},${quote(hash)},'acceptance-${index}');
      INSERT INTO user_balance(user_id,balance) VALUES(${user.id},${index === 0 ? 100000 : index === 14 ? 0 : 1000});`);
  }
  sql(`INSERT INTO session(session_id,name,type,single_key) VALUES(${singleID},'acceptance-single',0,'${users[0].id}:${users[1].id}'),(${groupID},'acceptance-group',1,NULL);
    INSERT INTO user_session(user_id,session_id,role) VALUES(${users[0].id},${singleID},2),(${users[1].id},${singleID},2);
    INSERT INTO friend(user_id,friend_id) VALUES(${users[0].id},${users[1].id}),(${users[1].id},${users[0].id});`);
  for (let index = 0; index < 15; index++) sql(`INSERT INTO user_session(user_id,session_id,role) VALUES(${users[index].id},${groupID},${index === 0 ? 0 : 2});`);
  writeFileSync(fixturePath, JSON.stringify({ users, singleID, groupID }, null, 2));
  console.log('Isolated acceptance fixtures created');
  process.exit(0);
}

const fixture = JSON.parse(readFileSync(fixturePath, 'utf8'));
const { users, groupID, singleID } = fixture;
const sessions = [];
const balance = index => number(`SELECT balance FROM user_balance WHERE user_id=${users[index].id}`);
function sendBody(overrides = {}) {
  return { clientMessageId: randomUUID(), sessionId: groupID, sessionType: 1, receiverId: '0',
    redPacketType: 0, wrapperText: 'acceptance', totalAmount: 1000, totalCount: 10, ...overrides };
}
async function create(body, index = 0) {
  const value = ok(await request('/api/redpacket/send', sessions[index].token, body));
  packetIDs.add(value.redPacketId); return value;
}
const claim = (id, index = 1) => request(`/api/redpacket/${id}/receive`, sessions[index].token, {});
const details = async (id, index = 1) => ok(await request(`/api/redpacket/${id}`, sessions[index].token));
const poolKey = id => `redpacket:{${id}}:available-share-ids`;
let alice, bob;

try {
  await until('gateway and business services', async () => {
    try { const response = await request('/api/user/loginPassword', undefined, { account: users[0].account, password: 'Test123!' }); return response.code === 200; } catch { return false; }
  }, 45000);
  await test('gateway static files, routes and authentication', async () => {
    const htmlResponse = await fetch(`${base}/`); assert.equal(htmlResponse.status, 200);
    const html = await htmlResponse.text(); const asset = html.match(/src="([^"]+\.js)"/)[1];
    assert.equal((await fetch(`${base}${asset}`)).status, 200);
    assert.equal((await request('/api/unknown')).status, 404);
    assert.equal((await request('/api/redpacketish/send')).status, 404);
    assert.notEqual((await request('/api/redpacket/sent')).code, 200);
    for (const user of users) sessions.push(await login(user.account));
    assert.equal(sessions[0].userId, users[0].id);
    return { staticStatus: 200, unknownRoute: 404, logins: sessions.length };
  });
  await test('message stored ACK, receiver push and offline overlap through gateway', async () => {
    alice = await connect(sessions[0].token); bob = await connect(sessions[1].token);
    const ack = await sendChat(alice, fixture, `acceptance-${runID}`);
    assert.equal(ack.status, 'stored');
    await until('receiver push', () => bob.events.some(event => event.eventType === 'message.new' && event.message.messageId === ack.messageId));
    const page = await sync(sessions[1].token, singleID);
    assert.equal(page.messages.filter(message => message.messageId === ack.messageId).length, 1);
    assert.equal(alice.events.filter(event => event.eventType === 'message.new').length, 0);
    return { messageID: ack.messageId, seq: ack.seq, nextSeq: page.nextSeq };
  });
  await test('message repeated request and content conflict', async () => {
    const key = randomUUID();
    const first = await sendChat(alice, fixture, 'same-message', key);
    const repeated = await sendChat(alice, fixture, 'same-message', key);
    assert.equal(first.status, 'stored'); assert.equal(repeated.messageId, first.messageId); assert.equal(repeated.seq, first.seq);
    const conflict = await sendChat(alice, fixture, 'different-message', key);
    assert.equal(conflict.status, 'rejected');
    assert.equal(number(`SELECT COUNT(*) FROM message WHERE client_message_id=${quote(key)}`), 1);
    return { messageID: first.messageId, seq: first.seq, conflict: conflict.code ?? conflict.reason };
  });
  await test('offline pagination and reading permission', async () => {
    const first = await sync(sessions[1].token, singleID, '0', 1);
    assert.equal(first.messages.length, 1); assert.equal(first.hasMore, true);
    const second = await sync(sessions[1].token, singleID, first.nextSeq, 1);
    assert.equal(second.messages.length, 1); assert.notEqual(second.messages[0].messageId, first.messages[0].messageId);
    const forbidden = await request('/api/offline/sync', sessions[15].token, { conversations: [{ sessionId: singleID, afterSeq: '0' }], limit: 10 });
    assert.notEqual(forbidden.code, 200);
    return { firstSeq: first.nextSeq, secondSeq: second.nextSeq, forbiddenCode: forbidden.code };
  });
  let single;
  await test('single red packet debit, async message and both Redis projections', async () => {
    const before = balance(0);
    single = await create(sendBody({ sessionId: singleID, sessionType: 0, receiverId: users[1].id, totalAmount: 250, totalCount: 1 }));
    assert.equal(balance(0), before - 250);
    await until('red packet chat message', () => number(`SELECT COUNT(*) FROM message WHERE red_packet_id=${single.redPacketId}`) === 1);
    await until('share candidate cache', () => redis('LLEN', poolKey(single.redPacketId)) === '1');
    const allKeys = redis('KEYS', `im:redpacket:send:v1:${users[0].id}:*`).split('\n');
    assert.ok(allKeys.some(key => redis('GET', key).includes(`"RedPacketID":${single.redPacketId}`)));
    await until('red packet realtime push', () => bob.events.some(event => event.eventType === 'message.new' && event.message.messageId === single.messageId));
    return { packetID: single.redPacketId, debit: 250, candidates: 1 };
  });
  await test('single claim qualification, idempotency and detail list', async () => {
    assert.equal((await claim(single.redPacketId, 0)).code, 40101);
    assert.equal((await claim(single.redPacketId, 2)).code, 40101);
    const before = balance(1);
    const first = ok(await claim(single.redPacketId));
    const repeated = ok(await claim(single.redPacketId));
    assert.equal(first.receiveId, repeated.receiveId); assert.equal(balance(1), before + 250);
    const detail = await details(single.redPacketId); assert.equal(detail.status, 1); assert.equal(detail.receives.length, 1);
    assert.equal(detail.receives[0].receiverId, users[1].id); assert.equal(detail.receives[0].amount, 250);
    assert.equal(redis('EXISTS', poolKey(single.redPacketId)), '0');
    await audit(single.redPacketId);
    return { amount: first.amount, recordCount: detail.receives.length };
  });
  await test('invalid amount, insufficient balance and nonmember leave no funds facts', async () => {
    const countBefore = number('SELECT COUNT(*) FROM red_packet'); const before = balance(0);
    assert.equal((await request('/api/redpacket/send', sessions[0].token, sendBody({ totalAmount: 101, totalCount: 10 }))).code, 40000);
    assert.equal((await request('/api/redpacket/send', sessions[14].token, sendBody())).code, 73000);
    assert.equal((await request('/api/redpacket/send', sessions[15].token, sendBody())).code, 40101);
    assert.equal(number('SELECT COUNT(*) FROM red_packet'), countBefore); assert.equal(balance(0), before);
    return { validationCode: 40000, insufficientCode: 73000, permissionCode: 40101 };
  });
  let group;
  await test('12 simultaneous identical sends debit exactly once', async () => {
    const body = sendBody(); const before = balance(0);
    const responses = await Promise.all(Array.from({ length: 12 }, () => request('/api/redpacket/send', sessions[0].token, body)));
    const successful = responses.map(ok); assert.equal(new Set(successful.map(value => value.redPacketId)).size, 1);
    group = successful[0]; packetIDs.add(group.redPacketId);
    assert.equal(balance(0), before - 1000);
    assert.equal(number(`SELECT COUNT(*) FROM red_packet WHERE client_message_id=${quote(body.clientMessageId)}`), 1);
    assert.equal((await request('/api/redpacket/send', sessions[0].token, { ...body, totalAmount: 2000 })).code, 73001);
    return { attempts: 12, packetID: group.redPacketId, debit: 1000 };
  });
  await test('20 simultaneous claims by the same user credit exactly once', async () => {
    const before = balance(1);
    const values = (await Promise.all(Array.from({ length: 20 }, () => claim(group.redPacketId)))).map(ok);
    assert.equal(new Set(values.map(value => value.receiveId)).size, 1);
    assert.equal(balance(1), before + 100);
    assert.equal(packetFacts(group.redPacketId).count, 1);
    await audit(group.redPacketId);
    return { attempts: 20, amount: 100, receiveRecords: 1 };
  });
  await test('competing group claimants cannot oversell; sender can claim', async () => {
    const indexes = [0, ...Array.from({ length: 12 }, (_, index) => index + 2)];
    const values = await Promise.all(indexes.map(index => claim(group.redPacketId, index)));
    assert.equal(values.filter(value => value.code === 200).length, 9);
    assert.ok(values.filter(value => value.code !== 200).every(value => value.code === 73003));
    const facts = packetFacts(group.redPacketId); assert.deepEqual([facts.status, facts.count, facts.received], [1, 10, 1000]);
    const detail = await details(group.redPacketId); assert.equal(detail.receives.length, 10);
    assert.equal(detail.receives.reduce((sum, receive) => sum + receive.amount, 0), 1000);
    await audit(group.redPacketId);
    const own = await create(sendBody({ totalAmount: 10, totalCount: 1 }));
    assert.equal(ok(await claim(own.redPacketId, 0)).amount, 10);
    await audit(own.redPacketId);
    return { contenders: indexes.length, successful: 9, totalClaims: 10, totalAmount: 1000 };
  });
  await test('missing and stale Redis candidates fall back to MySQL', async () => {
    const value = await create(sendBody({ totalAmount: 303, totalCount: 3 }));
    await until('cache build', () => redis('LLEN', poolKey(value.redPacketId)) === '3');
    redis('DEL', poolKey(value.redPacketId));
    assert.equal(ok(await claim(value.redPacketId, 2)).amount, 101);
    assert.equal(redis('LLEN', poolKey(value.redPacketId)), '2');
    redis('DEL', poolKey(value.redPacketId)); redis('LPUSH', poolKey(value.redPacketId), '9223372036854775000');
    assert.equal(ok(await claim(value.redPacketId, 3)).amount, 101);
    await audit(value.redPacketId);
    return { packetID: value.redPacketId, claims: 2, remainingCandidates: redis('LLEN', poolKey(value.redPacketId)) };
  });
  await test('claim SQL failure rolls back share, receive row and packet totals', async () => {
    const value = await create(sendBody({ totalAmount: 200, totalCount: 2 }));
    const before = balance(0), receiverBefore = balance(12);
    sql(`DELETE FROM user_balance WHERE user_id=${users[12].id}`);
    try {
      assert.notEqual((await claim(value.redPacketId, 12)).code, 200);
      assert.equal(packetFacts(value.redPacketId).count, 0);
      assert.equal(number(`SELECT COUNT(*) FROM red_packet_receive WHERE red_packet_id=${value.redPacketId}`), 0);
      assert.equal(number(`SELECT COUNT(*) FROM red_packet_share WHERE red_packet_id=${value.redPacketId} AND status=1`), 0);
      assert.equal(balance(0), before);
    } finally { sql(`INSERT INTO user_balance(user_id,balance) VALUES(${users[12].id},${receiverBefore})`); }
    assert.equal(ok(await claim(value.redPacketId, 12)).amount, 100);
    await audit(value.redPacketId);
    return { packetID: value.redPacketId, rolledBack: true, retrySucceeded: true };
  });
  await test('partial expiry returns only remaining funds and concurrent refunds occur once', async () => {
    const value = await create(sendBody({ totalAmount: 1000, totalCount: 4 }));
    assert.equal(ok(await claim(value.redPacketId, 1)).amount, 250);
    const before = balance(0);
    sql(`UPDATE red_packet SET expire_at=NOW(3)-INTERVAL 1 SECOND WHERE red_packet_id=${value.redPacketId}`);
    await helper('expire', value.redPacketId);
    assert.equal(balance(0), before + 750); assert.equal(packetFacts(value.redPacketId).status, 2);
    assert.equal((await claim(value.redPacketId, 2)).code, 73002);
    assert.equal(ok(await claim(value.redPacketId, 1)).amount, 250);
    await audit(value.redPacketId);
    return { packetID: value.redPacketId, claims: 250, refund: 750, concurrentRefundAttempts: 20 };
  });
  await test('expiry during claim rolls back credit; refund wins after packet lock', async () => {
    const value = await create(sendBody({ totalAmount: 200, totalCount: 2 }));
    const receiverBefore = balance(2), senderBefore = balance(0);
    sql(`DELIMITER $$
      CREATE TRIGGER acceptance_slow_share BEFORE UPDATE ON red_packet_share FOR EACH ROW BEGIN
      IF NEW.red_packet_id=${value.redPacketId} THEN DO SLEEP(1.5); END IF; END$$
      DELIMITER ;`);
    try {
      sql(`UPDATE red_packet SET expire_at=NOW(3)+INTERVAL 900000 MICROSECOND WHERE red_packet_id=${value.redPacketId}`);
      const claiming = claim(value.redPacketId, 2);
      await delay(1000);
      const refunding = helper('expire', value.redPacketId);
      assert.equal((await claiming).code, 73002);
      await refunding;
    } finally { sql('DROP TRIGGER IF EXISTS acceptance_slow_share'); }
    assert.equal(balance(2), receiverBefore); assert.equal(balance(0), senderBefore + 200);
    const facts = packetFacts(value.redPacketId); assert.deepEqual([facts.status, facts.count, facts.received], [2, 0, 0]);
    await audit(value.redPacketId);
    return { packetID: value.redPacketId, rejectedClaim: 73002, refund: 200, receiverCredit: 0 };
  });
  await test('scheduled worker expires a packet without direct helper calls', async () => {
    const value = await create(sendBody({ totalAmount: 300, totalCount: 3 }));
    const before = balance(0);
    sql(`UPDATE red_packet SET expire_at=NOW(3)-INTERVAL 1 SECOND WHERE red_packet_id=${value.redPacketId}`);
    await until('scheduled refund', () => packetFacts(value.redPacketId).status === 2);
    assert.equal(balance(0), before + 300);
    await audit(value.redPacketId);
    return { packetID: value.redPacketId, refund: 300 };
  });
  await test('Kafka outage preserves committed funds and Outbox recovers after restart', async () => {
    await until('prior Outbox published', () => number('SELECT COUNT(*) FROM outbox_event WHERE status<>2') === 0);
    docker('stop', '--time', '2', 'im-learn-kafka');
    let value;
    try {
      const before = balance(0);
      value = await create(sendBody({ totalAmount: 500, totalCount: 5 }));
      assert.equal(balance(0), before - 500);
      assert.equal(number(`SELECT COUNT(*) FROM message WHERE red_packet_id=${value.redPacketId}`), 0);
      assert.equal(number(`SELECT COUNT(*) FROM outbox_event WHERE aggregate_id=${value.redPacketId} AND status<>2`), 2);
      assert.ok(number('SELECT COUNT(*) FROM outbox_event WHERE status<>2') >= 2);
    } finally { docker('start', 'im-learn-kafka'); }
    await until('message after Kafka recovery', () => number(`SELECT COUNT(*) FROM message WHERE red_packet_id=${value.redPacketId}`) === 1, 60000);
    await until('recovered Outbox published', () => number('SELECT COUNT(*) FROM outbox_event WHERE status<>2') === 0, 60000);
    await audit(value.redPacketId);
    return { packetID: value.redPacketId, committedDebit: 500, recoveredMessageCount: 1 };
  });
  await test('replayed creation and cache Outbox do not duplicate message, Seq or debit', async () => {
    const value = await create(sendBody({ totalAmount: 400, totalCount: 4 }));
    await until('message persisted', () => number(`SELECT COUNT(*) FROM message WHERE red_packet_id=${value.redPacketId}`) === 1);
    await until('all published', () => number('SELECT COUNT(*) FROM outbox_event WHERE status<>2') === 0);
    const seqBefore = number(`SELECT last_seq FROM conversation_sequence WHERE session_id=${groupID}`);
    const balanceBefore = balance(0);
    sql(`UPDATE outbox_event SET status=0,next_retry_at=NOW(3),lease_owner=NULL,lease_until=NULL
      WHERE event_type IN ('chat.message.create_requested','redpacket.cache.build_requested') AND aggregate_id=${value.redPacketId}`);
    assert.equal(number(`SELECT COUNT(*) FROM outbox_event WHERE aggregate_id=${value.redPacketId} AND status<>2`), 2);
    await until('replayed outbox published', () => number('SELECT COUNT(*) FROM outbox_event WHERE status<>2') === 0);
    await delay(1000);
    assert.equal(number(`SELECT COUNT(*) FROM message WHERE red_packet_id=${value.redPacketId}`), 1);
    assert.equal(number(`SELECT last_seq FROM conversation_sequence WHERE session_id=${groupID}`), seqBefore);
    assert.equal(balance(0), balanceBefore); await audit(value.redPacketId);
    return { packetID: value.redPacketId, seqUnchanged: seqBefore, balanceUnchanged: true };
  });
  await test('Redis outage refuses unauthenticated writes and retries converge after restart', async () => {
    const before = balance(0), count = number('SELECT COUNT(*) FROM red_packet'); const body = sendBody();
    const messageKey = randomUUID();
    docker('stop', '--time', '2', 'im-learn-redis');
    try {
      const response = await request('/api/redpacket/send', sessions[0].token, body);
      assert.notEqual(response.code, 200); assert.equal(balance(0), before); assert.equal(number('SELECT COUNT(*) FROM red_packet'), count);
      // Permission RPC also uses Redis-backed models: a full Redis outage can
      // prevent permission confirmation even on an already authenticated socket.
      const ack = await sendChat(alice, fixture, 'redis-down-message', messageKey);
      assert.equal(ack.status, 'unavailable');
      assert.equal(number(`SELECT COUNT(*) FROM message WHERE client_message_id=${quote(messageKey)}`), 0);
      const responseSync = await request('/api/offline/sync', sessions[1].token, { conversations: [{ sessionId: singleID, afterSeq: '0' }], limit: 10 });
      assert.notEqual(responseSync.code, 200);
    } finally { docker('start', 'im-learn-redis'); }
    await until('Redis reconnect', () => { try { return redis('PING') === 'PONG'; } catch { return false; } });
    let recoveryAttempts = 0;
    const value = await until('authenticated packet send after Redis reconnect', async () => {
      recoveryAttempts++;
      const response = await request('/api/redpacket/send', sessions[0].token, body);
      if (response.code === 200) return response.data;
      assert.ok([500, 50000, 50008].includes(response.code) || response.status >= 500,
        `Unexpected permanent failure after Redis restart: ${JSON.stringify(response)}`);
      return false;
    });
    packetIDs.add(value.redPacketId);
    const duplicate = await create(body);
    assert.equal(value.redPacketId, duplicate.redPacketId); assert.equal(balance(0), before - body.totalAmount);
    await until('message send after Redis permission recovery', async () => {
      const ack = await sendChat(alice, fixture, 'redis-down-message', messageKey);
      if (ack.status === 'stored') return true;
      assert.ok(['unavailable', 'unknown'].includes(ack.status), `Unexpected send result: ${ack.status}`);
      return false;
    });
    assert.equal(number(`SELECT COUNT(*) FROM message WHERE client_message_id=${quote(messageKey)}`), 1);
    return { writeRefusedDuringOutage: true, permissionUnavailableDuringOutage: true, retryPacketID: value.redPacketId, debit: body.totalAmount, recoveryAttempts };
  });
  await test('message cache deletion falls back to MySQL', async () => {
    const max = number(`SELECT MAX(seq) FROM message WHERE session_id=${singleID}`);
    for (let seq = 1; seq <= max; seq++) redis('DEL', `im:message-cache:{${singleID}}:${seq}`);
    const page = await sync(sessions[1].token, singleID);
    assert.equal(page.messages.length, max); assert.equal(Number(page.nextSeq), max);
    return { messagesReadFromMySQL: max, nextSeq: page.nextSeq };
  });
  await test('lucky packet 101 cents in 100 shares preserves amount and minimum share', async () => {
    const value = await create(sendBody({ redPacketType: 1, totalAmount: 101, totalCount: 100 }));
    assert.equal(number(`SELECT COUNT(*) FROM red_packet_share WHERE red_packet_id=${value.redPacketId}`), 100);
    assert.equal(number(`SELECT SUM(amount) FROM red_packet_share WHERE red_packet_id=${value.redPacketId}`), 101);
    assert.equal(number(`SELECT MIN(amount) FROM red_packet_share WHERE red_packet_id=${value.redPacketId}`), 1);
    await audit(value.redPacketId);
    return { packetID: value.redPacketId, shareCount: 100, totalAmount: 101, minimumShare: 1 };
  });
  await test('all packet audits and per-user balance-ledger reconciliation', async () => {
    for (const id of packetIDs) await audit(id);
    for (const [index, user] of users.entries()) {
      const initial = index === 0 ? 100000 : index === 14 ? 0 : 1000;
      const delta = number(`SELECT COALESCE(SUM(amount),0) FROM balance_log WHERE user_id=${user.id}`);
      assert.equal(balance(index), initial + delta, `Balance-ledger mismatch for fixture user ${index}`);
    }
    assert.equal(number('SELECT COUNT(*) FROM red_packet_share WHERE amount<=0'), 0);
    return { auditedPackets: packetIDs.size, reconciledUsers: users.length };
  });
} finally {
  for (const socket of sockets) socket.close();
  save();
}
