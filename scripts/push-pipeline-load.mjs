import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { spawn } from 'node:child_process';

let source=readFileSync('scripts/message-batch-compare.mjs','utf8');
assert.ok(source.includes('  let groupOffset = 0;')&&source.includes('  output.result ='));
source=source.replaceAll('[10, 50, 100]','[10, 100]');
source=source.replace('pageNumber < 100;','pageNumber < 1000;');
const mode=process.env.IM_PIPELINE_LOAD_MODE||'Burst';
if(mode==='Sustained'){
  const start=source.indexOf('  let groupOffset = 0;'),end=source.indexOf('  output.result =',start);
  source=source.slice(0,start)+String.raw`
  const durationSeconds=Number(process.env.IM_PIPELINE_DURATION_SECONDS||20);
  assert.ok(durationSeconds>=10&&durationSeconds<=60);
  const rates=(process.env.IM_PIPELINE_RATES||'100,300,600').split(',').map(Number);
  const groupCounts=(process.env.IM_PIPELINE_GROUPS||'1,20').split(',').map(Number);
  assert.ok(rates.every(x=>x>=1&&x<=1000)&&groupCounts.every(x=>x===1||x===20)&&rates.length*groupCounts.reduce((a,b)=>a+b,0)<=64);
  output.methodology={...output.methodology,model:'open-loop fixed-rate sends, round-robin across 100 clients; ACK does not pace sending',
    rates,durationSeconds,messagesPerCase:'rate * durationSeconds',concurrency:'not fixed; observed in-flight ACKs',tracing:process.env.IM_PIPELINE_TRACE_SUSTAINED==='1',profiling:process.env.IM_PIPELINE_PROFILE==='1'};
  let groupOffset=0;
  for(const rate of rates){
    for(const groupCount of groupCounts){
      const groups=fixture.groups.slice(groupOffset,groupOffset+groupCount);groupOffset+=groupCount;
      const name=(groupCount===1?'hot':'many')+'-r'+rate;
      console.log('START '+variant+' '+name+' for '+durationSeconds+'s');
      for(let index=0;index<101;index++){if(!clients[index].isOpen())clients[index]=await connect(index)}
      const warm=[];
      await pool(10,100,async index=>{const item=await sendTracked(clients[index],groups[index%groupCount],'warmup');assert.ok(item.ok);warm.push(item)});
      assert.ok(await waitUntil(()=>warm.every(x=>x.pushMs!==undefined),30000),'warmup push did not drain');
      const profile=process.env.IM_PIPELINE_PROFILE==='1'?fetch('http://127.0.0.1:19060/debug/pprof/profile?seconds=10').then(r=>{assert.ok(r.ok);return r.arrayBuffer()}).then(buffer=>writeFileSync(join(runDir,name+'.pprof'),Buffer.from(buffer))):Promise.resolve();
      const items=[],tasks=[],backlog=[],errors={};
      const count=rate*durationSeconds;
      const histogram=monitorEventLoopDelay({resolution:10});histogram.enable();
      const cpuBefore=process.cpuUsage(),began=performance.now();
      let next=0,inflight=0,maxInflight=0,lastSnapshot=0;
      const record=()=>backlog.push({second:Number(((performance.now()-began)/1000).toFixed(2)),sent:items.length,
        awaitingAck:inflight,awaitingPush:items.filter(x=>x.ok!==false&&x.pushMs===undefined).length});
      while(next<count){
        const now=performance.now();
        const due=Math.min(count,Math.floor((now-began)*rate/1000)+1);
        while(next<due){
          const index=next++,id=randomUUID(),started=performance.now();
          const item={id,group:groups[index%groupCount],phase:name,started,
            scheduledLatenessMs:Math.max(0,started-(began+index*1000/rate))};
          tracked.set(id,item);items.push(item);
          if(inflight>=1000){item.ok=false;item.error='driver_inflight_cap';item.ackMs=0;errors[item.error]=(errors[item.error]||0)+1;continue}
          inflight++;maxInflight=Math.max(maxInflight,inflight);
          tasks.push(clients[index%100].send(item.group,id).then(response=>{
            item.ackMs=performance.now()-started;item.ok=response.status==='stored';
            item.error=item.ok?undefined:response.code||response.status||'bad_ack';
            if(!item.ok)errors[item.error]=(errors[item.error]||0)+1;
          }).finally(()=>inflight--));
        }
        if(now-began-lastSnapshot>=1000){record();lastSnapshot=now-began}
        await delay(5);
      }
      const sendSeconds=(performance.now()-began)/1000;
      record();
      const duringSendDelivered=items.filter(x=>x.pushMs!==undefined).length;
      await Promise.all(tasks);
      const ackSeconds=(performance.now()-began)/1000;
      const cpu=process.cpuUsage(cpuBefore);histogram.disable();
      await profile;
      const successful=items.filter(x=>x.ok);
      const drainStart=performance.now();
      await waitUntil(()=>successful.every(x=>x.pushMs!==undefined),15000);
      const metric={name,rate,groupCount,requests:items.length,succeeded:successful.length,errorCount:items.length-successful.length,errors,
        targetDurationSecs:durationSeconds,durationSecs:Number(ackSeconds.toFixed(3)),sendDurationSecs:Number(sendSeconds.toFixed(3)),
        actualSendRPS:Number((items.length/sendSeconds).toFixed(2)),ackRPS:Number((successful.length/ackSeconds).toFixed(2)),
        ack:distribution(successful.map(x=>x.ackMs)),push:distribution(successful.filter(x=>x.pushMs!==undefined).map(x=>x.pushMs)),
        pushed:successful.filter(x=>x.pushMs!==undefined).length,duringSendDelivered,maxInflight,backlog,
        schedulingLateness:distribution(items.map(x=>x.scheduledLatenessMs)),
        driverCPUPercentOneCore:Number(((cpu.user+cpu.system)/(ackSeconds*10000)).toFixed(2)),driverEventLoopP99Ms:Number((histogram.percentile(99)/1e6).toFixed(2)),
        drainWaitSecs:Number(((performance.now()-drainStart)/1000).toFixed(3))};
      metric.pushMissing=successful.length-metric.pushed;
      const actual=await rows("SELECT JSON_OBJECT('sessionId',CAST(m.session_id AS CHAR),'count',COUNT(*),'minSeq',MIN(m.seq),'maxSeq',MAX(m.seq),'lastSeq',s.last_seq) FROM message m JOIN conversation_sequence s ON m.session_id=s.session_id WHERE m.session_id IN ("+groups.join(',')+") GROUP BY m.session_id,s.last_seq");
      assert.ok(actual.every(x=>x.minSeq===1&&x.count===x.maxSeq&&x.maxSeq===x.lastSeq),'Seq has holes');
      metric.sequenceChecks=actual;metric.databaseMessages=actual.reduce((sum,x)=>sum+x.count,0);
      metric.committedWithoutStoredAck=metric.databaseMessages-warm.length-successful.length;
      assert.ok(metric.committedWithoutStoredAck>=0,'stored ACK without persistence');
      const recovered=await recover(groups);
      metric.syncMissing=successful.filter(x=>!recovered.has(x.id)).length;
      assert.equal(metric.syncMissing,0,'stored message missing from Seq sync');
      output.cases.push(metric);save();
      console.log('END '+variant+' '+name+': actual='+metric.actualSendRPS+'/s push p95='+metric.push.p95Ms+'ms p99='+metric.push.p99Ms+'ms errors='+metric.errorCount+' missingPush='+metric.pushMissing+' missingSync='+metric.syncMissing);
    }
  }
`+source.slice(end);
}
source=source.replace('resolve({ send(group, id) {','resolve({ isOpen() { return socket.readyState === WebSocket.OPEN; }, send(group, id) {');
source=source.replace('  for (const socket of sockets) socket.close();',String.raw`
  writeFileSync(join(runDir,'message-observations.json'),JSON.stringify([...tracked.values()].map(({id,group,phase,ok,ackMs,pushMs,scheduledLatenessMs})=>({id,group,phase,ok,ackMs,pushMs,scheduledLatenessMs}))));
  await delay(1800);
  for (const socket of sockets) socket.close();`);
const args=['--input-type=module'];
if(process.env.IM_PIPELINE_VALIDATE==='1')args.push('--check');
const child=spawn(process.execPath,args,{windowsHide:true,stdio:['pipe','inherit','inherit'],env:process.env});
child.stdin.end(source);
child.on('error',error=>{console.error(error.message);process.exitCode=1});
child.on('exit',code=>{process.exitCode=code??1});
