import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const [baselineDir, indexedDir, outputPath = 'scripts/relay-timings-results-20261006.json', errorOption] = process.argv.slice(2);
const allowRequestErrors = errorOption === '--allow-request-errors';
if (errorOption && !allowRequestErrors) throw new Error(`Unknown option: ${errorOption}`);
if (!baselineDir || !indexedDir) throw new Error('Pass baseline and indexed run directories');
const read = path => JSON.parse(readFileSync(path, 'utf8').replace(/^\uFEFF/, ''));
function stats(records) {
  const values = records.map(r => r.durationMs).sort((a,b)=>a-b);
  const percentile = p => values.length ? +values[Math.ceil(values.length*p)-1].toFixed(3) : null;
  return { n: values.length, failures: records.filter(r=>r.failed).length, count: records.reduce((s,r)=>s+r.count,0),
    totalMs: +values.reduce((s,n)=>s+n,0).toFixed(3), p50Ms:percentile(.5),p95Ms:percentile(.95),maxMs:percentile(1) };
}
function summarize(dir) {
  const results = read(join(dir,'results.json'));
  const records = readFileSync(join(dir,'relay.stderr.log'),'utf8').split(/\r?\n/).filter(l=>l.includes('outbox_timing ')).map(l=>JSON.parse(l.slice(l.indexOf('outbox_timing ')+14)));
  if (!records.length) throw new Error(`No Relay timings in ${dir}`);
  const phases = records => Object.fromEntries(['claim','publish','mark_published','reschedule','release'].map(p=>[p,stats(records.filter(r=>r.phase===p))]));
  const cases = results.cases.filter(c=>c.groups).map(c=>{
    if(c.requests!==600 || c.succeeded!==600 && !allowRequestErrors) throw new Error(`Not a successful fixed 600-message case: ${c.name}`);
    const selected = records.filter(r=>Date.parse(r.at)>=Date.parse(c.startedAt) && Date.parse(r.at)<=Date.parse(c.propagationFinishedAt));
    return { name:c.name,requests:c.requests,succeeded:c.succeeded,unexpectedErrors:c.unexpectedErrors,errors:c.errors,
      storedAck:c.latencySuccess,push:c.pushLatency,drainAfterStageSecs:c.drainAfterStageSecs,
      stageSecs:c.durationSecs,phases:phases(selected), productiveClaim:stats(selected.filter(r=>r.phase==='claim' && r.count>0)),emptyClaim:stats(selected.filter(r=>r.phase==='claim' && !r.count)) };
  });
  return { runID:results.runID,result:results.result,environment:read(join(dir,'environment.json')),cases,phases:phases(records),
    verification:results.verification,kafkaConsumerLag:results.kafkaConsumerLag,requestCount:results.cases.reduce((s,c)=>s+c.requests,0),unexpectedErrors:results.cases.reduce((s,c)=>s+c.unexpectedErrors,0) };
}
const output={baseline:summarize(baselineDir),indexed:summarize(indexedDir),methodology:'Six fixed 600-attempt stages each; success and error counts are retained. ACK and delivery percentiles cover successful requests; a run with request errors is not PASS. Actual service settings recorded per run. Timings include lease transaction and synchronous Kafka ACK. Per-stage windows run through online delivery; no parallel compilation during load. See accompanying report for differences between runs.'};
writeFileSync(outputPath,JSON.stringify(output,null,2));
for(const run of [output.baseline,output.indexed]) console.log(JSON.stringify({runID:run.runID,result:run.result,cases:run.cases.map(c=>({name:c.name,ackP95:c.storedAck.p95Ms,pushP95:c.push.p95Ms,claimP95:c.productiveClaim.p95Ms,publishP95:c.phases.publish.p95Ms,markP95:c.phases.mark_published.p95Ms}))},null,2));
