package relay

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

// MySQLStore implements the state changes around a claimed Outbox event.
// ClaimBatch selects and leases a batch in one transaction.
type MySQLStore struct {
	conn sqlx.SqlConn
}

func NewMySQLStore(conn sqlx.SqlConn) *MySQLStore {
	return &MySQLStore{conn: conn}
}

func (s *MySQLStore) ClaimBatch(
	ctx context.Context,
	workerID string,
	batchSize int,
	leaseUntil time.Time,
) ([]Event, error) {
	/*
		扫描数据库，读取outbox中待发送的消息
			1.在事务中选择记录并且加锁，使用for update
			2.检查发布顺序
			3.更新所选的记录
	*/
	if workerID == "" || len(workerID) > 64 {
		return nil, fmt.Errorf("invalid relay worker ID")
	}
	if batchSize <= 0 || batchSize > 2000 {
		return nil, fmt.Errorf("batch size must be between 1 and 2000")
	}
	if !leaseUntil.After(time.Now()) {
		return nil, fmt.Errorf("lease must expire in the future")
	}

	const selectQuery = `
		SELECT e.event_id, e.topic, e.message_key, e.payload, e.retry_count,
		       e.event_type, e.aggregate_type, e.aggregate_id, e.aggregate_seq,
		       e.status, e.next_retry_at, e.lease_until, NOW(3) AS db_now
		FROM outbox_event AS e
		WHERE (
			(e.status = 0 AND e.next_retry_at <= NOW(3))
			OR
			(e.status = 1 AND e.lease_until <= NOW(3))
		)
		AND (
			e.event_type <> 'chat.message.persisted'
			OR NOT EXISTS (
				SELECT 1
				FROM outbox_event AS earlier
				WHERE earlier.event_type = 'chat.message.persisted'
				AND earlier.aggregate_type = e.aggregate_type
				AND earlier.aggregate_id = e.aggregate_id
				AND earlier.aggregate_seq < e.aggregate_seq
				AND earlier.status <> 2
			)
		)
		ORDER BY e.created_time, e.event_id
		LIMIT ?
		FOR UPDATE SKIP LOCKED`

	// Lock each head first, materialize a committed prefix using an MVCC read,
	// then lock those exact IDs with NOWAIT. A range locking read can encounter
	// the Writer's uncommitted tail inserts; materializing IDs excludes them.
	// Existing locked middle records remain in the snapshot and cause rollback.
	const prefixQuery = `SELECT event_id, topic, message_key, payload, retry_count,
		event_type, aggregate_type, aggregate_id, aggregate_seq, status,
		next_retry_at, lease_until, NOW(3) AS db_now
		FROM outbox_event
		WHERE aggregate_type=? AND aggregate_id=?
		  AND event_type='chat.message.persisted' AND status<>2
		ORDER BY aggregate_seq, event_id LIMIT ?`
	var claimed []Event
	err := s.conn.TransactCtx(
		ctx,
		func(ctx context.Context, tx sqlx.Session) error {
			if err := tx.QueryRowsCtx(ctx, &claimed, selectQuery, batchSize); err != nil {
				return fmt.Errorf("select claimable outbox events: %w", err)
			}

			heads := append([]Event(nil), claimed...)
			for _, head := range heads {
				if head.EventType != "chat.message.persisted" {
					continue
				}
				if !head.AggregateSeq.Valid || head.AggregateSeq.Int64 <= 0 {
					return fmt.Errorf("invalid outbox Seq for event %d", head.EventID)
				}
				if len(claimed) >= batchSize {
					continue
				}
				var prefix []Event
				if err := tx.QueryRowsCtx(ctx, &prefix, prefixQuery, head.AggregateType, head.AggregateID, batchSize-len(claimed)+1); err != nil {
					return fmt.Errorf("lock conversation prefix: %w", err)
				}
				if len(prefix) == 0 || prefix[0].EventID != head.EventID {
					return fmt.Errorf("outbox conversation head changed: %d", head.EventID)
				}
				// Cut at a known retry/lease barrier before acquiring extra locks.
				end := 1
				for end < len(prefix) && claimable(prefix[end]) {
					end++
				}
				placeholders, args := idArguments(eventIDs(prefix[:end]))
				var locked []Event
				if err := tx.QueryRowsCtx(ctx, &locked, `SELECT event_id,topic,message_key,payload,retry_count,
					event_type,aggregate_type,aggregate_id,aggregate_seq,status,next_retry_at,lease_until,NOW(3) AS db_now
					FROM outbox_event WHERE event_id IN (`+placeholders+`) ORDER BY aggregate_seq,event_id FOR UPDATE NOWAIT`, args...); err != nil {
					return fmt.Errorf("lock committed conversation prefix: %w", err)
				}
				if len(locked) != end || locked[0].EventID != head.EventID {
					return fmt.Errorf("committed conversation prefix changed")
				}
				previous := head.AggregateSeq.Int64
				for _, event := range locked[1:] {
					if !event.AggregateSeq.Valid || event.AggregateSeq.Int64 <= previous {
						return fmt.Errorf("invalid outbox Seq ordering")
					}
					if !claimable(event) {
						break
					}
					claimed = append(claimed, event)
					previous = event.AggregateSeq.Int64
				}
			}
			if len(claimed) == 0 {
				return nil
			}
			placeholders, args := idArguments(eventIDs(claimed))
			args = append([]any{workerID, leaseUntil}, args...)
			result, err := tx.ExecCtx(ctx, "UPDATE outbox_event SET status=1,lease_owner=?,lease_until=? WHERE event_id IN ("+placeholders+")", args...)
			if err != nil {
				return fmt.Errorf("lease outbox batch: %w", err)
			}
			return requireRows(result, len(claimed), "lease batch")
		},
	)
	if err != nil {
		return nil, fmt.Errorf("claim outbox batch: %w", err)
	}
	return claimed, nil
}

func (s *MySQLStore) MarkPublished(
	ctx context.Context,
	eventID int64,
	workerID string,
	publishedAt time.Time,
) error {
	return s.MarkPublishedBatch(ctx, []int64{eventID}, workerID, publishedAt)
}

func claimable(event Event) bool {
	return event.Status == 0 && !event.NextRetryAt.After(event.DBNow) ||
		event.Status == 1 && event.LeaseUntil.Valid && !event.LeaseUntil.Time.After(event.DBNow)
}

func idArguments(ids []int64) (string, []any) {
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	return strings.TrimSuffix(strings.Repeat("?,", len(ids)), ","), args
}

// A checkpoint is atomic: one lost lease must roll back every row in it.
func (s *MySQLStore) MarkPublishedBatch(ctx context.Context, ids []int64, workerID string, at time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	placeholders, args := idArguments(ids)
	args = append([]any{at}, args...)
	args = append(args, workerID)
	return s.conn.TransactCtx(ctx, func(ctx context.Context, tx sqlx.Session) error {
		result, err := tx.ExecCtx(ctx, `UPDATE outbox_event SET status=2,published_time=?,lease_owner=NULL,lease_until=NULL,last_error=NULL
			WHERE event_id IN (`+placeholders+`) AND status=1 AND lease_owner=? AND lease_until>NOW(3)`, args...)
		if err != nil {
			return err
		}
		return requireRows(result, len(ids), "confirm published checkpoint")
	})
}

// Best-effort, idempotent release. Already published/rescheduled records and
// expired or reassigned leases are untouched. Lease expiry covers release failure.
func (s *MySQLStore) ReleaseBatch(ctx context.Context, ids []int64, workerID string) error {
	if len(ids) == 0 {
		return nil
	}
	placeholders, args := idArguments(ids)
	args = append(args, workerID)
	_, err := s.conn.ExecCtx(ctx, `UPDATE outbox_event SET status=0,next_retry_at=NOW(3),lease_owner=NULL,lease_until=NULL
		WHERE event_id IN (`+placeholders+`) AND status=1 AND lease_owner=? AND lease_until>NOW(3)`, args...)
	return err
}

func requireRows(result sql.Result, expected int, operation string) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != int64(expected) {
		return fmt.Errorf("%s: expected %d rows, updated %d; transaction rolled back", operation, expected, affected)
	}
	return nil
}

func (s *MySQLStore) Reschedule(
	ctx context.Context,
	eventID int64,
	workerID string,
	nextRetryAt time.Time,
	lastError string,
) error {
	const query = `
UPDATE outbox_event
SET status = 0, retry_count = retry_count + 1,
    next_retry_at = ?, last_error = ?,
    lease_owner = NULL, lease_until = NULL
WHERE event_id = ? AND status = 1 AND lease_owner = ? AND lease_until > NOW(3)`
	result, err := s.conn.ExecCtx(ctx, query, nextRetryAt, lastError, eventID, workerID)
	if err != nil {
		return fmt.Errorf("reschedule outbox event %d: %w", eventID, err)
	}
	return requireOneRow(result, eventID, "reschedule")
}

func requireOneRow(result sql.Result, eventID int64, action string) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s outbox event %d: read affected rows: %w", action, eventID, err)
	}
	if affected != 1 {
		return fmt.Errorf("%s outbox event %d: lease not held (updated %d rows)", action, eventID, affected)
	}
	return nil
}
