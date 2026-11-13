import assert from 'node:assert/strict';
import {readFileSync,writeFileSync,existsSync} from 'node:fs';
import {join,basename} from 'node:path';

const run=process.argv[2];
assert.ok(run,'Usage: node scripts/summarize-push-pipeline.mjs <run-directory>');
const json=p=>JSON.parse(readFileSync(p,'utf8'));
const environment=json(join(run,'environment.json'));
const output={environment,notes:[
  'Source baseline is im-push-pipeline-backup-20261109-204320, including member cache and the original per-conversation batching.',
  'Current combines publishing, offset commit, logging and ordinary-message SQL changes; results do not isolate any individual change.',
  'Push latency is sender actual send to observer client receipt. One observer measures latency; each group has 101 members.',
  'Push percentiles contain received messages only: inspect errorCount, pushMissing and syncMissing before interpreting them.',
  'Sustained input is open-loop; burst input waits for stored ACK before that worker sends again. Neither is production capacity certification.',
  'Seq recovery checks content through a separate HTTP pull, not replay of Kafka business handlers or automatic missing-final-notification discovery.',
  'Only these summary fields are exported. Private fixtures, YAML, login tokens and DSNs are not read.',
],trials:[]};
for(const name of environment.Order){
  const dir=join(run,name),path=join(dir,'results.json');
  if(!existsSync(path)){output.trials.push({name,result:'MISSING_RESULTS'});continue}
  const result=json(path);
  const trial={name,result:result.result,methodology:result.methodology,lowLoad:result.lowLoad,cases:result.cases};
  for(const item of trial.cases){
    assert.equal(item.syncMissing,0,'stored message missing from active Seq sync');
    for(const row of item.sequenceChecks||[])assert.ok(row.count===row.maxSeq&&row.maxSeq===row.lastSeq&&row.minSeq===1);
    console.log(name,item.name,JSON.stringify({sendRPS:item.actualSendRPS,ackRPS:item.ackRPS,
      ackP95:item.ack.p95Ms,pushP95:item.push.p95Ms,pushP99:item.push.p99Ms,
      errors:item.errorCount,missingPush:item.pushMissing,missingSync:item.syncMissing,
      tailBacklog:item.backlog?.at(-1),unackedCommits:item.committedWithoutStoredAck}));
  }
  // The trace file intentionally contains timing metadata only, never message bodies.
  const timings=join(dir,'server-timings.jsonl'),observations=join(dir,'message-observations.json');
  if(existsSync(timings)&&existsSync(observations)){
    const rows=readFileSync(timings,'utf8').trim().split('\n').filter(Boolean).map(JSON.parse);
    const observed=json(observations),indexed=new Map(rows.map(x=>[x.clientMessageId,x]));
    assert.equal(indexed.size,rows.length,'duplicate trace rows');
    const completed=observed.filter(x=>x.ok&&x.pushMs!==undefined);
    trial.trace={records:rows.length,completedObserved:completed.length,missing:completed.filter(x=>!indexed.has(x.id)).length};
    assert.equal(trial.trace.missing,0,'incomplete timing coverage');
    const stages={databaseBatch:['write_start','committed'],publisherQueue:['publish_enqueue','publish_dequeue'],
      postCommitToSocketWrite:['committed','ws_write_end'],pushHandler:['push_handler_start','push_handler_end']};
    for(const item of trial.cases){
      const selected=completed.filter(x=>x.phase===item.name).map(x=>indexed.get(x.id));
      item.traceTimings={};
      for(const [stage,[a,b]] of Object.entries(stages)){
        const values=selected.map(x=>{assert.ok(x.tNs[a]>0&&x.tNs[b]>=x.tNs[a]);return (x.tNs[b]-x.tNs[a])/1e6}).sort((a,b)=>a-b);
        if(values.length)item.traceTimings[stage]={n:values.length,p50Ms:+values[Math.ceil(values.length*.5)-1].toFixed(3),p95Ms:+values[Math.ceil(values.length*.95)-1].toFixed(3)};
      }
    }
  }
  output.trials.push(trial);
}
const filename=join('scripts',basename(run).replace('im-push-pipeline-','push-pipeline-results-')+'.json');
writeFileSync(filename,JSON.stringify(output,null,2));
console.log('Sanitized results: '+filename);
