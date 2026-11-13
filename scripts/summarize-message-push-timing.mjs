import assert from 'node:assert/strict';
import { readFileSync, writeFileSync, existsSync } from 'node:fs';
import { join, basename } from 'node:path';

const runDir = process.argv[2];
assert.ok(runDir, 'Usage: node scripts/summarize-message-push-timing.mjs <run-directory>');
const read = path => JSON.parse(readFileSync(path, 'utf8'));
const definitions = {
  permission: ['ingress', 'write_requested'],
  batchWait: ['write_requested', 'write_start'],
  databaseBatch: ['write_start', 'committed'],
  commitToPublishEnqueue: ['committed', 'publish_enqueue'],
  publisherQueue: ['publish_enqueue', 'publish_dequeue'],
  kafkaWrite: ['kafka_write_start', 'kafka_write_end'],
  kafkaReturnToFetchSigned: ['kafka_write_end', 'push_fetched'],
  kafkaCallStartToFetch: ['kafka_write_start', 'push_fetched'],
  fetchCall: ['push_fetch_start', 'push_fetched'],
  pushHandler: ['push_handler_start', 'push_handler_end'],
  groupRPC: ['group_rpc_start', 'group_rpc_end'],
  fanoutEnqueue: ['fanout_start', 'fanout_end'],
  offsetCommit: ['push_commit_start', 'push_commit_end'],
  observerSocketQueue: ['ws_enqueue', 'ws_dequeue'],
  observerSocketWrite: ['ws_write_start', 'ws_write_end'],
  postCommitToSocketWrite: ['committed', 'ws_write_end'],
  ingressToSocketWrite: ['ingress', 'ws_write_end'],
  ingressToAckQueue: ['ingress', 'ack_queued'],
};
const duration = (row, [start, end]) => (row.tNs[end] - row.tNs[start]) / 1e6;
function distribution(values) {
  assert.ok(values.length);
  const sorted = [...values].sort((a, b) => a - b);
  const round = x => Number(x.toFixed(4));
  const p = q => round(sorted[Math.max(0, Math.ceil(sorted.length * q) - 1)]);
  return { n: values.length, meanMs: round(values.reduce((a,b)=>a+b,0)/values.length),
    minMs: p(0), p50Ms: p(.5), p95Ms: p(.95), p99Ms: p(.99), maxMs: p(1),
    negativeCount: values.filter(x => x < 0).length };
}
function summarizeRows(rows) {
  const stages = Object.fromEntries(Object.entries(definitions).map(([key, pair]) => [key, distribution(rows.map(row => duration(row, pair)))]));
  return { messages: rows.length, stages };
}
function consumerLoop(rows) {
  const sorted = [...rows].sort((a,b) => a.tNs.push_fetched-b.tNs.push_fetched);
  const span = (sorted.at(-1).tNs.push_commit_end-sorted[0].tNs.push_fetched)/1e6;
  const handler = sorted.reduce((sum,row)=>sum+duration(row,definitions.pushHandler),0);
  const commit = sorted.reduce((sum,row)=>sum+duration(row,definitions.offsetCommit),0);
  const fetch = sorted.slice(1).reduce((sum,row)=>sum+duration(row,definitions.fetchCall),0);
  const gaps = sorted.slice(1).map((row,index)=>(row.tNs.push_fetch_start-sorted[index].tNs.push_commit_end)/1e6);
  return { measuredSpanMs: span, handlerSumMs: handler, commitSumMs: commit, fetchSumExcludingFirstMs: fetch,
    commitShareOfSerialSpanPercent: 100*commit/span, handlerShareOfSerialSpanPercent:100*handler/span,
    fetchStartedBeforePreviousCommitCompleted: gaps.filter(x=>x<0).length,
    previousCommitEndToNextFetchStart: gaps.length ? distribution(gaps) : undefined };
}

const output = { environment: read(join(runDir,'environment.json')), definitions,
  notes: [
    'One realtime process monotonic clock, ns timestamps converted to ms; no cross-process clock subtraction.',
    'databaseBatch includes the PersistBatch call (queries, SQL/lock/network/COMMIT), not just Seq or fsync; counted per participating message.',
    'kafkaReturnToFetchSigned may be negative: consumer fetching and producer acknowledgement overlap. Do not clamp or add percentile columns.',
    'offsetCommit happens after enqueue of current push; serial waiting delays following records.',
    'observerSocketWrite is server WriteMessage completion, not client receipt. Client measurements are reported separately.',
    'Bursts with identical sender concurrency do not fix the offered message rate. Two trace trials and one control do not establish causality or sustainable capacity.',
    'Every measured success must have a complete trace. Failures or missing rows abort the timing summary.',
  ], trials: [], pooled: {} };
const pool = new Map();
for (const name of ['trace-r1','control','trace-r2']) {
  const dir = join(runDir,name), results = read(join(dir,'results.json'));
  assert.equal(results.result,'PASS',name);
  const observations = read(join(dir,'message-observations.json'));
  const trial = { name, result: results.result, lowLoad: results.lowLoad, cases: results.cases };
  output.trials.push(trial);
  const file = join(dir,'server-timings.jsonl');
  if (name === 'control') { assert.ok(!existsSync(file)); continue; }
  const rows = readFileSync(file,'utf8').trim().split('\n').map(JSON.parse);
  const byID = new Map(rows.map(row=>[row.clientMessageId,row]));
  assert.equal(byID.size,rows.length,'duplicate trace exports');
  trial.traceRows = rows.length;
  trial.expectedRows = observations.filter(x=>x.ok).length;
  trial.timings = {};
  for (const phase of ['low-load',...results.cases.map(x=>x.name)]) {
    let measured = observations.filter(x=>x.ok && x.phase===phase);
    if (phase==='low-load') measured=measured.slice(5);
    const selected=measured.map(x=>{
      assert.ok(byID.has(x.id),`${name}/${phase}: trace missing for ${x.id}`);
      const row=byID.get(x.id);
      for(const [stage,pair] of Object.entries(definitions)) {
        assert.ok(pair.every(key=>row.tNs[key]>0),`missing stage ${stage}`);
        if(stage!=='kafkaReturnToFetchSigned') assert.ok(duration(row,pair)>=0,`negative ${stage}`);
      }
      return row;
    });
    trial.timings[phase]=summarizeRows(selected);
    if(phase!=='low-load') trial.timings[phase].consumerLoop=consumerLoop(selected);
    if(!pool.has(phase)) pool.set(phase,[]);
    pool.get(phase).push(...selected);
  }
  assert.equal(rows.length,trial.expectedRows,'trace coverage incomplete (including warmup)');
}
for(const [phase,rows] of pool) output.pooled[phase]=summarizeRows(rows);
const stamp=basename(runDir).replace('im-message-push-timing-','');
const path=join('scripts',`message-push-timing-results-${stamp}.json`);
writeFileSync(path,JSON.stringify(output,null,2));
console.log(`Saved sanitized aggregates: ${path}`);
for(const trial of output.trials) {
  console.log(trial.name,`trace ${trial.traceRows??'off'}/${trial.expectedRows??'off'}`);
  for(const item of trial.cases) console.log(item.name,`ACK=${item.ack.p95Ms}, push=${item.push.p95Ms}, RPS=${item.ackRPS}`,
    trial.timings?.[item.name] ? `commit busy=${trial.timings[item.name].consumerLoop.commitShareOfSerialSpanPercent.toFixed(1)}%` : '');
}
for(const [phase,data] of Object.entries(output.pooled)) {
  console.log(phase,JSON.stringify(Object.fromEntries(Object.entries(data.stages).map(([name,dist])=>[name,dist.p95Ms]))));
}
