package sequence

import (
	"context"
	"fmt"
	"math"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

// SegmentStore durably reserves non-overlapping Seq ranges.
// Redis may lose a reserved range, but MySQL never hands that range out again.
type SegmentStore interface {
	Reserve(ctx context.Context, sessionID, blockSize int64) (Segment, error)
}

type Segment struct {
	StartExclusive int64
	EndInclusive   int64
}

type MySQLSegmentStore struct {
	conn sqlx.SqlConn
}

func NewMySQLSegmentStore(conn sqlx.SqlConn) *MySQLSegmentStore {
	return &MySQLSegmentStore{conn: conn}
}

func (s *MySQLSegmentStore) Reserve(
	ctx context.Context,
	sessionID int64,
	blockSize int64,
) (Segment, error) {
	if sessionID <= 0 {
		return Segment{}, fmt.Errorf("session id must be positive")
	}
	if blockSize <= 0 {
		return Segment{}, fmt.Errorf("sequence block size must be positive")
	}

	var segment Segment
	err := s.conn.TransactCtx(
		ctx,
		func(ctx context.Context, tx sqlx.Session) error {
			const createRowQuery = `
INSERT INTO conversation_sequence (session_id, reserved_max_seq)
VALUES (?, 0)
ON DUPLICATE KEY UPDATE session_id = VALUES(session_id)`
			if _, err := tx.ExecCtx(ctx, createRowQuery, sessionID); err != nil {
				return fmt.Errorf("ensure conversation sequence row: %w", err)
			}

			const lockRowQuery = `
SELECT reserved_max_seq
FROM conversation_sequence
WHERE session_id = ?
FOR UPDATE`
			var current int64
			if err := tx.QueryRowCtx(ctx, &current, lockRowQuery, sessionID); err != nil {
				return fmt.Errorf("lock conversation sequence row: %w", err)
			}

			if current > math.MaxInt64-blockSize {
				return fmt.Errorf("conversation sequence overflow")
			}
			next := current + blockSize

			const updateQuery = `
UPDATE conversation_sequence
SET reserved_max_seq = ?
WHERE session_id = ?`
			if _, err := tx.ExecCtx(ctx, updateQuery, next, sessionID); err != nil {
				return fmt.Errorf("advance conversation sequence: %w", err)
			}

			segment = Segment{
				StartExclusive: current,
				EndInclusive:   next,
			}
			return nil
		},
	)
	if err != nil {
		return Segment{}, err
	}

	return segment, nil
}
