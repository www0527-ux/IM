import assert from 'node:assert/strict';
import { readFileSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';

const runDir = process.argv[2];
assert.ok(runDir, 'pass the local comparison artifact directory');
const root = resolve(import.meta.dirname, '..');
const environment = JSON.parse(readFileSync(join(runDir, 'environment.json'), 'utf8'));
const trialNames = Array.from({ length: environment.Rounds }, (_, i) =>
  (i % 2 === 0 ? ['baseline', 'current'] : ['current', 'baseline']).map(variant => `${variant}-r${i + 1}`)).flat();
const trials = trialNames.map(name => ({ name, ...JSON.parse(readFileSync(join(runDir, name, 'results.json'), 'utf8')) }));
assert.ok(trials.every(trial => trial.result !== 'RUNNING' && trial.result !== 'FAIL'), 'comparison incomplete');
const mean = values => values.reduce((sum, value) => sum + value, 0) / values.length;
const round = value => Number(value.toFixed(2));
const comparisons = trials[0].cases.map(first => {
  const collect = variant => {
    const cases = trials.filter(trial => trial.variant === variant).map(trial => trial.cases.find(item => item.name === first.name));
    assert.ok(cases.every(Boolean), `missing scenario ${first.name}`);
    return { ackRPS: round(mean(cases.map(item => item.ackRPS))),
      meanTrialAckP95Ms: round(mean(cases.map(item => item.ack.p95Ms))),
      meanTrialPushP95Ms: round(mean(cases.map(item => item.push.p95Ms))),
      errors: cases.reduce((sum, item) => sum + item.errorCount, 0),
      pushedMissing: cases.reduce((sum, item) => sum + item.pushMissing, 0),
      syncMissing: cases.reduce((sum, item) => sum + item.syncMissing, 0),
      observedCommitCounts: cases.map(item => item.statementCounts.commits),
      sampleSuccessCounts: cases.map(item => item.succeeded) };
  };
  const baseline = collect('baseline'), current = collect('current');
  return { name: first.name, conversations: first.groupCount, concurrency: first.concurrency, baseline, current,
    throughputFactor: round(current.ackRPS / baseline.ackRPS),
    ackP95ReductionPercent: round((1 - current.meanTrialAckP95Ms / baseline.meanTrialAckP95Ms) * 100) };
});
const all = trials.flatMap(trial => trial.cases);
const summary = { environment, methodology: trials[0].methodology, comparisons,
  lowLoad: Object.fromEntries(['baseline', 'current'].map(variant => [variant, {
    meanTrialAckP95Ms: round(mean(trials.filter(trial => trial.variant === variant).map(trial => trial.lowLoad.ack.p95Ms))),
    meanTrialPushP95Ms: round(mean(trials.filter(trial => trial.variant === variant).map(trial => trial.lowLoad.push.p95Ms))),
  }])),
  verification: { measuredAttempts: all.reduce((sum, item) => sum + item.requests, 0),
    measuredSuccesses: all.reduce((sum, item) => sum + item.succeeded, 0),
    requestErrors: all.reduce((sum, item) => sum + item.errorCount, 0),
    pushedMissing: all.reduce((sum, item) => sum + item.pushMissing, 0),
    syncMissing: all.reduce((sum, item) => sum + item.syncMissing, 0),
    committedWithoutStoredAck: all.reduce((sum, item) => sum + item.committedWithoutStoredAck, 0),
    checkedConversations: all.reduce((sum, item) => sum + item.sequenceChecks.length, 0) },
  notes: [
    'Averages are means of trial statistics. Mean trial P95 is not a pooled P95.',
    'The fixed request count creates short bursts, not sustainable capacity measurements.',
    'The load driver shares the host. SQL pool budgets, fsync settings, fanout, payload and publisher implementation were held constant.',
    'Zero counterUpdates/messageInserts in raw trial data means prepared DML digests were unavailable. Do not interpret them as zero executions.',
    'COMMIT digest observations can lag or be incomplete; they illustrate batching but are not exact DML counts.',
    'Raw local fixtures/configs contain temporary credentials; this exported artifact excludes them.',
  ], trials };
const artifact = `message-batch-results-${environment.RunID}.json`;
writeFileSync(join(root, 'scripts', artifact), JSON.stringify(summary, null, 2));
const table = comparisons.map(item => `| ${item.conversations} | ${item.concurrency} | ${item.baseline.ackRPS} | ${item.current.ackRPS} | ${item.throughputFactor}× | ${item.baseline.meanTrialAckP95Ms} | ${item.current.meanTrialAckP95Ms} | ${item.baseline.meanTrialPushP95Ms} | ${item.current.meanTrialPushP95Ms} | ${item.baseline.errors}/${item.current.errors} |`).join('\n');
const markdown = `# 消息批处理对照 ${environment.RunID}

执行${environment.Rounds}轮，每场景${environment.MessageCount}次请求。以下指标为各轮的算术均值；P95为每轮P95的均值，不是合并样本P95。

| 会话数 | 并发 | 基线条/秒 | 批处理条/秒 | 倍数 | 基线ACK P95/ms | 批处理ACK P95/ms | 基线推送P95/ms | 批处理推送P95/ms | 失败：基线/批处理 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
${table}

共${summary.verification.measuredAttempts}次测量请求，成功${summary.verification.measuredSuccesses}次。检查${summary.verification.checkedConversations}个会话，实时缺失${summary.verification.pushedMissing}条，同步缺失${summary.verification.syncMissing}条。

这是固定请求数的短时闭环负载，不代表生产容量；完整环境、单轮数据和采集限制见[结果JSON](${artifact})。
`;
writeFileSync(join(root, 'scripts', `message-batch-summary-${environment.RunID}.md`), markdown);
console.log(JSON.stringify({ comparisons, verification: summary.verification, lowLoad: summary.lowLoad }, null, 2));
