import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const dir = process.argv[2];
if (!dir) throw new Error('Pass the load run directory');
const read = file => JSON.parse(readFileSync(join(dir, file), 'utf8').replace(/^\uFEFF/, ''));
const result = read('results.json');
// Older runs used the audit helper's numeric int64 JSON field. Restore the exact
// ID from the request strings before exporting; never infer it from a JS number.
if (result.verification?.packetAudits) {
  const ids = result.cases.filter(value => value.packets).flatMap(value => value.packets);
  result.verification.packetAudits.forEach((value, index) => { value.redPacketId = ids[index]; });
  for (const metric of result.cases.filter(value => value.packets)) {
    const audits = result.verification.packetAudits.filter(value => metric.packets.includes(value.redPacketId));
    metric.databaseConfirmedClaims = audits.reduce((sum, value) => sum + value.receiveRecordCount, 0);
    metric.committedWithoutSuccessResponse = metric.databaseConfirmedClaims - metric.succeeded;
    metric.completedPackets = audits.filter(value => value.receiveRecordCount === 100).length;
  }
  writeFileSync(join(dir, 'results.json'), JSON.stringify(result, null, 2));
}
const compact = {
  runID: result.runID, result: result.result, relayProbe: result.relayProbe, failure: result.failure,
  cases: result.cases.map(value => ({ name: value.name, seconds: value.durationSecs, requests: value.requests,
    success: value.succeeded, expected: value.expectedRejections, errors: value.errors, rps: value.successRPS,
    p95: value.latencySuccess.p95Ms, p99: value.latencySuccess.p99Ms,
    pending: value.outboxAfter.pending, processing: value.outboxAfter.processing,
    published: value.outboxAfter.published, pushP95: value.pushLatency?.p95Ms,
    databaseConfirmedClaims: value.databaseConfirmedClaims, committedWithoutSuccessResponse: value.committedWithoutSuccessResponse,
    driverCPUPercentOneCore: value.driverCPUPercentOneCore, eventLoopP99Ms: value.driverEventLoopP99Ms,
  })),
  drainSecs: result.outboxDrainBeforeSyncSecs,
  balancesChecked: result.verification?.usersBalanceChecked,
  packetsAudited: result.verification?.packetAudits?.length,
  finalOutbox: result.verification?.finalOutbox,
};
console.log(JSON.stringify(compact, null, 2));
writeFileSync(join(dir, 'summary.json'), JSON.stringify(compact, null, 2));
