//go:build integration

package relay

import (
	"context"
	"errors"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type orderingPublisher struct {
	attempts []int64
	failed   bool
}

func (p *orderingPublisher) Publish(_ context.Context, _, _, payload string) error {
	id, err := strconv.ParseInt(payload, 10, 64)
	if err != nil {
		return err
	}
	p.attempts = append(p.attempts, id)
	if id == 1 && !p.failed {
		p.failed = true
		return errors.New("injected broker failure")
	}
	return nil
}

// This test writes ONLY to a fresh im_relay_test_* schema supplied by the runner.
// It exercises the actual locking query, leases and Worker failure recovery.
func TestMySQLPredecessorOrderingAndRecovery(t *testing.T) {
	dsn := os.Getenv("IM_RELAY_TEST_DSN")
	if dsn == "" {
		t.Skip("IM_RELAY_TEST_DSN is required")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil || !regexp.MustCompile(`^im_relay_test_[0-9_]+$`).MatchString(cfg.DBName) {
		t.Fatal("test requires an isolated schema")
	}
	ctx := context.Background()
	conn := sqlx.NewMysql(dsn)
	raw, err := conn.RawDB()
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	store := NewMySQLStore(conn)
	for _, indexed := range []bool{false, true} {
		t.Run(strconv.FormatBool(indexed), func(t *testing.T) {
			mustExec := func(query string) {
				t.Helper()
				if _, err := raw.ExecContext(ctx, query); err != nil {
					t.Fatal(err)
				}
			}
			mustExec("DELETE FROM outbox_event WHERE event_id IN (1,2,3,4)")
			if indexed {
				mustExec("ALTER TABLE outbox_event ADD INDEX idx_relay_predecessor (aggregate_type,aggregate_id,event_type,status,aggregate_seq)")
			}
			mustExec(`INSERT INTO outbox_event (event_id,event_type,aggregate_type,aggregate_id,aggregate_seq,topic,message_key,payload,created_time) VALUES
			(1,'chat.message.persisted','conversation',901,1,'test-only','901','1','2026-01-01'),
			(2,'chat.message.persisted','conversation',901,2,'test-only','901','2','2026-01-01'),
			(3,'chat.message.persisted','conversation',902,1,'test-only','902','3','2026-01-01'),
			(4,'redpacket.cache.build','redpacket',903,NULL,'test-only','903','4','2026-01-01')`)
			lock, err := raw.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer lock.Rollback()
			if _, err := lock.ExecContext(ctx, "SELECT event_id FROM outbox_event WHERE event_id=1 FOR UPDATE"); err != nil {
				t.Fatal(err)
			}
			claimed, err := store.ClaimBatch(ctx, "lock-test", 100, time.Now().Add(time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			if len(claimed) != 2 || claimed[0].EventID != 3 || claimed[1].EventID != 4 {
				t.Fatalf("locked head must block successor, not other aggregates: %+v", claimed)
			}
			if err := lock.Rollback(); err != nil {
				t.Fatal(err)
			}
			mustExec("UPDATE outbox_event SET status=0,lease_owner=NULL,lease_until=NULL WHERE event_id IN (3,4)")
			publisher := &orderingPublisher{}
			var timings []Timing
			worker := NewWorker(store, publisher, Options{WorkerID: "recovery-test", BatchSize: 100, LeaseDuration: time.Minute, MaxRetryBackoff: time.Minute, Observe: func(v Timing) { timings = append(timings, v) }})
			if n, err := worker.RunBatch(ctx); n != 4 || err == nil {
				t.Fatalf("failure batch: n=%d err=%v", n, err)
			}
			if n, err := worker.RunBatch(ctx); n != 2 || err != nil {
				t.Fatalf("future retry must block only its conversation: n=%d err=%v", n, err)
			}
			mustExec("UPDATE outbox_event SET next_retry_at=DATE_SUB(NOW(3),INTERVAL 1 SECOND) WHERE event_id=1")
			if n, err := worker.RunBatch(ctx); n != 2 || err != nil {
				t.Fatalf("recovery should claim same-conversation prefix: n=%d err=%v", n, err)
			}
			want := []int64{1, 3, 4, 1, 2}
			if len(publisher.attempts) != len(want) {
				t.Fatal(publisher.attempts)
			}
			for i, id := range want {
				if publisher.attempts[i] != id {
					t.Fatal(publisher.attempts)
				}
			}
			var pending int
			if err := raw.QueryRowContext(ctx, "SELECT COUNT(*) FROM outbox_event WHERE status<>2").Scan(&pending); err != nil || pending != 0 {
				t.Fatalf("pending=%d err=%v", pending, err)
			}
			phases := map[string]int{}
			for _, v := range timings {
				phases[v.Phase]++
				if v.DurationMs < 0 || v.At.IsZero() {
					t.Fatal(v)
				}
			}
			if phases["claim"] != 3 || phases["publish"] != 5 || phases["mark_published"] != 2 || phases["reschedule"] != 1 || phases["release"] != 1 {
				t.Fatal(phases)
			}
			mustExec("UPDATE outbox_event SET status=1,lease_owner='expired',lease_until=DATE_SUB(NOW(3),INTERVAL 1 SECOND) WHERE event_id=1")
			if store.MarkPublished(ctx, 1, "expired", time.Now()) == nil || store.Reschedule(ctx, 1, "expired", time.Now(), "test") == nil {
				t.Fatal("expired owner must not update state")
			}
			// A partially valid checkpoint must not publish the valid subset.
			mustExec("UPDATE outbox_event SET status=1,lease_owner='expired',lease_until=DATE_ADD(NOW(3),INTERVAL 1 MINUTE) WHERE event_id=3")
			if store.MarkPublishedBatch(ctx, []int64{1, 3}, "expired", time.Now()) == nil {
				t.Fatal("mixed-validity checkpoint accepted")
			}
			var state int
			if err := raw.QueryRowContext(ctx, "SELECT status FROM outbox_event WHERE event_id=3").Scan(&state); err != nil || state != 1 {
				t.Fatalf("checkpoint partially committed: %d %v", state, err)
			}
			if err := store.ReleaseBatch(ctx, []int64{1, 3}, "expired"); err != nil {
				t.Fatal(err)
			}
			if err := raw.QueryRowContext(ctx, "SELECT status FROM outbox_event WHERE event_id=1").Scan(&state); err != nil || state != 1 {
				t.Fatal("release modified expired lease")
			}

			// An inner locked row cannot be omitted by prefix expansion.
			mustExec("UPDATE outbox_event SET status=0,next_retry_at=NOW(3),lease_owner=NULL,lease_until=NULL WHERE event_id IN (1,2,3,4)")
			inner, err := raw.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer inner.Rollback()
			if _, err := inner.ExecContext(ctx, "SELECT event_id FROM outbox_event WHERE event_id=2 FOR UPDATE"); err != nil {
				t.Fatal(err)
			}
			if events, err := store.ClaimBatch(ctx, "inner-test", 100, time.Now().Add(time.Minute)); err == nil || len(events) != 0 {
				t.Fatalf("must rollback on locked inner event: %+v %v", events, err)
			}
			if err := inner.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err := raw.QueryRowContext(ctx, "SELECT COUNT(*) FROM outbox_event WHERE status<>0").Scan(&state); err != nil || state != 0 {
				t.Fatal("failed claim left leases")
			}

			// A future retry or live lease in the middle closes the prefix.
			mustExec(`INSERT INTO outbox_event (event_id,event_type,aggregate_type,aggregate_id,aggregate_seq,topic,message_key,payload)
				VALUES (5,'chat.message.persisted','conversation',901,3,'test-only','901','5')`)
			for _, barrier := range []string{"status=0,next_retry_at=DATE_ADD(NOW(3),INTERVAL 1 MINUTE)", "status=1,lease_owner='other',lease_until=DATE_ADD(NOW(3),INTERVAL 1 MINUTE)"} {
				mustExec("UPDATE outbox_event SET status=0,next_retry_at=NOW(3),lease_owner=NULL,lease_until=NULL WHERE event_id IN (1,2,3,4,5)")
				mustExec("UPDATE outbox_event SET " + barrier + " WHERE event_id=2")
				events, err := store.ClaimBatch(ctx, "barrier-test", 100, time.Now().Add(time.Minute))
				if err != nil {
					t.Fatal(err)
				}
				for _, event := range events {
					if event.EventID == 2 || event.EventID == 5 {
						t.Fatalf("crossed inner barrier: %+v", events)
					}
				}
				if len(events) != 3 {
					t.Fatal(events)
				}
			}
			// Simulate a process disappearing after claim, without cleanup.
			mustExec("UPDATE outbox_event SET status=0,next_retry_at=NOW(3),lease_owner=NULL,lease_until=NULL")
			lost, err := store.ClaimBatch(ctx, "lost-process", 5, time.Now().Add(time.Minute))
			if err != nil || len(lost) != 5 {
				t.Fatalf("lost claim: %+v %v", lost, err)
			}
			if other, err := store.ClaimBatch(ctx, "replacement", 5, time.Now().Add(time.Minute)); err != nil || len(other) != 0 {
				t.Fatalf("live lease stolen: %+v %v", other, err)
			}
			mustExec("UPDATE outbox_event SET lease_until=DATE_SUB(NOW(3),INTERVAL 1 SECOND) WHERE lease_owner='lost-process'")
			recovered, err := store.ClaimBatch(ctx, "replacement", 5, time.Now().Add(time.Minute))
			if err != nil || len(recovered) != 5 {
				t.Fatalf("lease recovery: %+v %v", recovered, err)
			}
			if err := store.ReleaseBatch(ctx, eventIDs(lost), "lost-process"); err != nil {
				t.Fatal(err)
			}
			if store.MarkPublishedBatch(ctx, eventIDs(lost), "lost-process", time.Now()) == nil {
				t.Fatal("old owner confirmed replacement lease")
			}
			if err := raw.QueryRowContext(ctx, "SELECT COUNT(*) FROM outbox_event WHERE status=1 AND lease_owner='replacement'").Scan(&state); err != nil || state != 5 {
				t.Fatal("old owner changed recovered leases")
			}
			if err := store.ReleaseBatch(ctx, eventIDs(recovered), "replacement"); err != nil {
				t.Fatal(err)
			}
			mustExec("DELETE FROM outbox_event WHERE event_id=5")
			// Uncommitted tail inserts must not block the committed prefix.
			appendTx, err := raw.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer appendTx.Rollback()
			if _, err := appendTx.ExecContext(ctx, `INSERT INTO outbox_event (event_id,event_type,aggregate_type,aggregate_id,aggregate_seq,topic,message_key,payload)
				VALUES (6,'chat.message.persisted','conversation',901,3,'test-only','901','6')`); err != nil {
				t.Fatal(err)
			}
			committed, err := store.ClaimBatch(ctx, "committed-prefix", 100, time.Now().Add(time.Minute))
			if err != nil || len(committed) != 4 {
				t.Fatalf("uncommitted tail blocked committed prefix: %+v %v", committed, err)
			}
			for _, event := range committed {
				if event.EventID == 6 {
					t.Fatal("claimed uncommitted event")
				}
			}
			if err := appendTx.Rollback(); err != nil {
				t.Fatal(err)
			}
			// Exercise the configured scale, and ensure the remaining tail cannot
			// be claimed while the first 500 records have live leases.
			var values []string
			for i := int64(0); i < 600; i++ {
				id := strconv.FormatInt(10000+i, 10)
				values = append(values, "("+id+",'chat.message.persisted','conversation',905,"+strconv.FormatInt(i+1, 10)+",'test-only','905','"+id+"')")
			}
			mustExec("INSERT INTO outbox_event (event_id,event_type,aggregate_type,aggregate_id,aggregate_seq,topic,message_key,payload) VALUES " + strings.Join(values, ","))
			large, err := store.ClaimBatch(ctx, "large-prefix", 500, time.Now().Add(time.Minute))
			if err != nil || len(large) != 500 {
				t.Fatalf("large prefix: count=%d err=%v", len(large), err)
			}
			for i, event := range large {
				if event.AggregateSeq.Int64 != int64(i+1) {
					t.Fatalf("large prefix skipped or reordered Seq: %+v", event)
				}
			}
			if tail, err := store.ClaimBatch(ctx, "overtake", 500, time.Now().Add(time.Minute)); err != nil || len(tail) != 0 {
				t.Fatalf("tail overtook live batch: count=%d err=%v", len(tail), err)
			}
			if err := store.MarkPublishedBatch(ctx, eventIDs(large), "large-prefix", time.Now()); err != nil {
				t.Fatal(err)
			}
			tail, err := store.ClaimBatch(ctx, "tail", 500, time.Now().Add(time.Minute))
			if err != nil || len(tail) != 100 || tail[0].AggregateSeq.Int64 != 501 {
				t.Fatalf("tail recovery: count=%d err=%v", len(tail), err)
			}
			mustExec("DELETE FROM outbox_event WHERE aggregate_id=905")
			if indexed {
				mustExec("ALTER TABLE outbox_event DROP INDEX idx_relay_predecessor")
			}
		})
	}
}
