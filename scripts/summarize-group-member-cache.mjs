import assert from 'node:assert/strict';
import { readFileSync, writeFileSync } from 'node:fs';
import { join, basename } from 'node:path';

const runDir=process.argv[2];
assert.ok(runDir,'Usage: node scripts/summarize-group-member-cache.mjs <run-directory>');
const read=path=>JSON.parse(readFileSync(path,'utf8'));
const stages={
  databaseBatch:['write_start','committed'],
  publisherQueue:['publish_enqueue','publish_dequeue'],
  kafkaWrite:['kafka_write_start','kafka_write_end'],
  kafkaReturnToFetchSigned:['kafka_write_end','push_fetched'],
  memberLookup:['group_rpc_start','group_rpc_end'],
  pushHandler:['push_handler_start','push_handler_end'],
  offsetCommit:['push_commit_start','push_commit_end'],
  observerSocketQueue:['ws_enqueue','ws_dequeue'],
  postCommitToSocketWrite:['committed','ws_write_end'],
};
function distribution(values){
  const sorted=[...values].sort((a,b)=>a-b);
  const p=q=>Number(sorted[Math.max(0,Math.ceil(sorted.length*q)-1)].toFixed(4));
  return {n:values.length,meanMs:Number((values.reduce((a,b)=>a+b,0)/values.length).toFixed(4)),p50Ms:p(.5),p95Ms:p(.95),p99Ms:p(.99),maxMs:p(1),negativeCount:values.filter(x=>x<0).length};
}
const output={environment:read(join(runDir,'environment.json')),definitions:stages,
  notes:[
    'Same binary; cache enabled/disabled/enabled. All trials enable identical opt-in tracing.',
    'memberLookup uses historical group_rpc trace markers; with cache enabled these measure the complete cache/provider lookup, not necessarily a remote RPC.',
    'Closed-loop fixed-count bursts, not fixed arrival rate. ACK throughput differs between trials; percentile ratios do not isolate cache benefit at an identical input rate.',
    'Signed producer-return to application-fetch duration can be negative because producer acknowledgement and consumer fetching overlap.',
    'Per-stage percentiles cannot be added. Server WebSocket completion is distinct from client receipt.',
  ],trials:[]};
for(const name of ['trace-r1','control','trace-r2']){
  const dir=join(runDir,name),results=read(join(dir,'results.json'));
  assert.equal(results.result,'PASS');
  // Read only the cache flag; do not export private YAML/DSNs/tokens.
  const yaml=readFileSync(join(dir,'realtime.yaml'),'utf8');
  const disabled=/GroupMemberCache:\s*\n\s+Disabled:\s*true/.test(yaml);
  assert.equal(disabled,name==='control');
  const rows=readFileSync(join(dir,'server-timings.jsonl'),'utf8').trim().split('\n').map(JSON.parse);
  const observations=read(join(dir,'message-observations.json'));
  const byID=new Map(rows.map(row=>[row.clientMessageId,row]));
  assert.equal(byID.size,rows.length);
  assert.equal(rows.length,observations.filter(x=>x.ok).length,'incomplete trace coverage');
  const trial={name,cacheEnabled:!disabled,traceRows:rows.length,lowLoad:results.lowLoad,cases:[]};
  for(const metric of results.cases){
    const selected=observations.filter(x=>x.ok&&x.phase===metric.name).map(x=>{
      assert.ok(byID.has(x.id));return byID.get(x.id);
    });
    assert.equal(selected.length,metric.succeeded);
    const timings=Object.fromEntries(Object.entries(stages).map(([key,[a,b]])=>[key,distribution(selected.map(row=>{
      assert.ok(row.tNs[a]>0&&row.tNs[b]>0,`stage missing: ${key}`);
      const value=(row.tNs[b]-row.tNs[a])/1e6;
      if(key!=='kafkaReturnToFetchSigned')assert.ok(value>=0,`negative stage: ${key}`);
      return value;
    }))]));
    trial.cases.push({...metric,timings});
  }
  output.trials.push(trial);
}
const stamp=basename(runDir).replace('im-group-member-cache-compare-','');
const path=join('scripts',`group-member-cache-results-${stamp}.json`);
writeFileSync(path,JSON.stringify(output,null,2));
console.log(`Sanitized results: ${path}`);
for(const trial of output.trials){
  console.log(trial.name,`cache=${trial.cacheEnabled} trace=${trial.traceRows}`);
  for(const item of trial.cases)console.log(item.name,JSON.stringify({rps:item.ackRPS,ackP95:item.ack.p95Ms,pushP95:item.push.p95Ms,
    stagesP95:Object.fromEntries(Object.entries(item.timings).map(([name,value])=>[name,value.p95Ms]))}));
}
