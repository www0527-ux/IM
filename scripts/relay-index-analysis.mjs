import { readFileSync, writeFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { join } from 'node:path';

// Only a fresh, isolated copy is modified. The source load-test database is read-only.
const [sourceRunDir = join(process.env.TEMP, 'im-load-20261005_235123'), outputPath = 'scripts/relay-index-analysis-20261006.json'] = process.argv.slice(2);
const environment = JSON.parse(readFileSync(join(sourceRunDir, 'environment.json'), 'utf8').replace(/^\uFEFF/, ''));
const source = environment.Database;
if (!/^im_acceptance_[0-9_]+$/.test(source)) throw new Error('Source must be an isolated load-test schema');
const report = JSON.parse(readFileSync(join(sourceRunDir, 'results.json'), 'utf8').replace(/^\uFEFF/, ''));
const group = report.cases.find(c => c.name === 'same-conversation-c100').groups[0];
if (!/^\d+$/.test(String(group))) throw new Error('Invalid group');
const database = `im_relay_probe_${Date.now()}`;
const sql = query => execFileSync('docker', ['exec', '-i', 'im-learn-mysql', 'sh', '-c', 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot --batch --raw --skip-column-names'], { input: query, encoding: 'utf8', maxBuffer: 16 * 1024 * 1024 });
sql(`CREATE DATABASE ${database}; CREATE TABLE ${database}.outbox_event LIKE ${source}.outbox_event; INSERT INTO ${database}.outbox_event SELECT * FROM ${source}.outbox_event; UPDATE ${database}.outbox_event SET topic='relay-probe-only';`);
if (sql(`SHOW INDEX FROM ${database}.outbox_event WHERE Key_name='idx_relay_predecessor';`).trim()) {
  sql(`ALTER TABLE ${database}.outbox_event DROP INDEX idx_relay_predecessor;`);
}
const query = readFileSync('service/outboxrelay/internal/relay/store.go', 'utf8').match(/const selectQuery = `([\s\S]*?)`/)[1].replace('LIMIT ?', 'LIMIT 100');
const variants = [
  ['original', null],
  ['status-first', 'aggregate_type, aggregate_id, event_type, status, aggregate_seq'],
  ['seq-first', 'aggregate_type, aggregate_id, event_type, aggregate_seq, status'],
];
const result = { source, database, group: String(group), query, samples: [] };
for (const [variant, columns] of variants) {
  if (columns) sql(`ALTER TABLE ${database}.outbox_event ADD INDEX idx_relay_predecessor (${columns});`);
  for (const backlog of [600, 300, 100]) {
    sql(`USE ${database}; UPDATE outbox_event SET status=2,lease_owner=NULL,lease_until=NULL; UPDATE outbox_event SET status=0,next_retry_at=NOW(3) WHERE aggregate_id=${group} AND event_type='chat.message.persisted' ORDER BY aggregate_seq DESC LIMIT ${backlog}; ANALYZE TABLE outbox_event;`);
    const sample = { variant, backlog, durationsMs: [], plans: [] };
    for (let n = 0; n < 4; n++) {
      const plan = sql(`USE ${database}; START TRANSACTION; EXPLAIN ANALYZE ${query}; ROLLBACK;`);
      const root = plan.match(/actual time=[\d.]+\.\.([\d.]+) rows=([\d.]+) loops=1/);
      if (!root || Number(root[2]) !== 1) throw new Error(`Unexpected plan: ${plan}`);
      if (n > 0) { sample.durationsMs.push(Number(root[1])); sample.plans.push(plan); }
    }
    result.samples.push(sample);
    console.log(JSON.stringify({ variant, backlog, durationsMs: sample.durationsMs }));
  }
  if (columns) sql(`ALTER TABLE ${database}.outbox_event DROP INDEX idx_relay_predecessor;`);
}
writeFileSync(outputPath, JSON.stringify(result, null, 2));
