import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const [baselineDir, finalDir, outputFile = join(import.meta.dirname, 'load-results-20261005.json')] = process.argv.slice(2);
if (!baselineDir || !finalDir) throw new Error('Pass baseline and final run directories');
const read = path => JSON.parse(readFileSync(path, 'utf8').replace(/^\uFEFF/, ''));
const final = read(join(finalDir, 'results.json'));
const baseline = read(join(baselineDir, 'results.json'));
const environment = read(join(finalDir, 'environment.json'));
final.methodology.claimLatencyIncludesRoundBarrier ??= true;
final.methodology.claimsMaxStageSeconds = 120;
final.methodology.maxSyncRequestsPerStage = 3000;
const claimLogRecords = new Map();
const errorCauses = { connectionLimitApiErrors: 0, deadlineApiErrors: 0, samples: [] };
for (const file of ['redpacket.stdout.log', 'redpacket.stderr.log']) {
  for (const line of readFileSync(join(finalDir, file), 'utf8').split(/\r?\n/)) {
    let entry; try { entry = JSON.parse(line); } catch { continue; }
    if (/redpacket retryable error|unhandled redpacket error/.test(entry.content || '')) {
      if (entry.content.includes('Error 1040')) errorCauses.connectionLimitApiErrors++;
      if (entry.content.includes('context deadline exceeded')) errorCauses.deadlineApiErrors++;
      if (errorCauses.samples.length < 3 || errorCauses.samples.length < 5 && entry.content.includes('context deadline exceeded')) {
        errorCauses.samples.push({ at: entry['@timestamp'], message: entry.content });
      }
    }
    const id = entry.content?.match(/\[HTTP\].*\/api\/redpacket\/(\d+)\/receive/)?.[1];
    if (!id || !entry.duration) continue;
    const ms = parseFloat(entry.duration) * (entry.duration.endsWith('ms') ? 1 : 1000);
    claimLogRecords.set(entry.trace || `${id}:${entry['@timestamp']}`, { id, ms });
  }
}
for (const metric of final.cases.filter(value => value.packets)) {
  const durations = [...claimLogRecords.values()].filter(value => metric.packets.includes(value.id)).map(value => value.ms).sort((a, b) => a - b);
  const percentile = p => Number(durations[Math.max(0, Math.ceil(durations.length * p) - 1)].toFixed(2));
  metric.serverHTTPDurationAllResponses = durations.length ? { n: durations.length, p50Ms: percentile(.5), p95Ms: percentile(.95), p99Ms: percentile(.99) } : { n: 0 };
}
const processes = readFileSync(join(finalDir, 'process-resources.jsonl'), 'utf8').replace(/^\uFEFF/, '').trim().split(/\r?\n/).map(JSON.parse);
const peaks = { containers: {}, processes: {}, driver: { cpuPercentOneCore: 0, eventLoopP99Ms: 0 } };
for (const sample of final.telemetry) for (const container of sample.containers || []) {
  peaks.containers[container.Name] = Math.max(peaks.containers[container.Name] || 0, parseFloat(container.CPUPerc));
}
for (const sample of processes) for (const service of sample.Processes || []) {
  if (!service.Running) continue;
  const value = peaks.processes[service.Service] ||= { cpuPercentOneCore: 0, workingSetMB: 0 };
  value.cpuPercentOneCore = Math.max(value.cpuPercentOneCore, service.CPUPercentOneCore);
  value.workingSetMB = Math.max(value.workingSetMB, service.WorkingSetMB);
}
for (const value of final.cases) {
  peaks.driver.cpuPercentOneCore = Math.max(peaks.driver.cpuPercentOneCore, value.driverCPUPercentOneCore);
  peaks.driver.eventLoopP99Ms = Math.max(peaks.driver.eventLoopP99Ms, value.driverEventLoopP99Ms);
}
const kafkaLag = Object.fromEntries(Object.entries(final.kafkaConsumerLag).map(([group, raw]) => {
  const partitions = raw.split(/\r?\n/).map(line => line.trim().split(/\s+/)).filter(fields => fields.length >= 6 && /^\d+$/.test(fields[2]) && /^\d+$/.test(fields[5]));
  return [group, { reportedPartitions: partitions.length, totalLag: partitions.reduce((sum, fields) => sum + Number(fields[5]), 0) }];
}));
const source = { environment, baseline, final, processResources: processes, resourcePeaks: peaks, kafkaLag };
source.errorLogEvidence = errorCauses;
source.realtimeWriteErrorEvidence = { failures: 0, deadlineErrors: 0, samples: [] };
for (const file of ['realtime.stdout.log', 'realtime.stderr.log']) {
  for (const line of readFileSync(join(finalDir, file), 'utf8').split(/\r?\n/)) {
    if (!line.includes('write message through outbox pipeline failed:')) continue;
    source.realtimeWriteErrorEvidence.failures++;
    if (line.includes('context deadline exceeded')) source.realtimeWriteErrorEvidence.deadlineErrors++;
    if (source.realtimeWriteErrorEvidence.samples.length < 3) source.realtimeWriteErrorEvidence.samples.push(line);
  }
}
const claimQueryDurations = [];
for (const line of readFileSync(join(finalDir, 'relay.stdout.log'), 'utf8').split(/\r?\n/)) {
  let entry; try { entry = JSON.parse(line); } catch { continue; }
  if (!entry.content?.includes('SELECT e.event_id') || !entry.duration) continue;
  claimQueryDurations.push(parseFloat(entry.duration) * (entry.duration.endsWith('ms') ? 1 : 1000));
}
claimQueryDurations.sort((a, b) => a - b);
source.relayClaimQueryDuration = claimQueryDurations.length ? {
  n: claimQueryDurations.length,
  p95Ms: claimQueryDurations[Math.ceil(claimQueryDurations.length * .95) - 1],
  maxMs: claimQueryDurations.at(-1),
  note: 'SQL log query duration; includes idle polling and loaded stages, excludes the rest of RunBatch and Kafka publication.',
} : { n: 0 };
if (existsSync(join(finalDir, 'mysql-settings.tsv'))) {
  source.mysqlSettings = Object.fromEntries(readFileSync(join(finalDir, 'mysql-settings.tsv'), 'utf8').trim().split(/\r?\n/).map(line => line.split('\t')));
}
writeFileSync(outputFile, JSON.stringify(source, null, 2));
console.log(JSON.stringify({ result: final.result, errorCauses, resourcePeaks: peaks, kafkaLag,
  totals: { requests: final.cases.reduce((sum, value) => sum + value.requests, 0),
    unexpectedErrors: final.cases.reduce((sum, value) => sum + value.unexpectedErrors, 0) },
  claimedPackets: final.cases.filter(value => value.packets).map(value => ({ name: value.name, completedPackets: value.completedPackets,
    serverHTTPDurationAllResponses: value.serverHTTPDurationAllResponses,
    databaseConfirmedClaims: value.databaseConfirmedClaims, committedWithoutSuccessResponse: value.committedWithoutSuccessResponse })) }, null, 2));
