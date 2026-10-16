package conversation_sequence

import (
	"context"
	"errors"
	"fmt"

	"infinitechatgo/common/txctx"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var ErrRowMissing = errors.New("conversation sequence row is missing")

type Model interface {
	EnsureTx(ctx context.Context, sessionID int64) error
	LockAndIncrementTx(ctx context.Context, sessionID int64) (int64, error)
}

type model struct {
	conn sqlx.SqlConn
}

func NewModel(conn sqlx.SqlConn) Model {
	return &model{conn: conn}
}

// create if not exist
func (m *model) EnsureTx(ctx context.Context, sessionID int64) error {
	if sessionID <= 0 {
		return fmt.Errorf(
			"ensure conversation sequence: invalid session id %d",
			sessionID,
		)
	}
	conn, err := txctx.SessionConn(ctx)
	if err != nil {
		return fmt.Errorf(
			"ensure conversation sequence for session %d: %w",
			sessionID,
			err,
		)
	}

	const query = `INSERT INTO conversation_sequence(session_id,last_seq)
			VALUES(?,0)
			ON DUPLICATE KEY UPDATE session_id=session_id`
	if _, err := conn.ExecCtx(ctx, query, sessionID); err != nil {
		return fmt.Errorf(
			"ensure conversation sequence for session %d: %w",
			sessionID,
			err,
		)
	}
	return nil
}

func (m *model) LockAndIncrementTx(
	ctx context.Context,
	sessionID int64,
) (int64, error) {
	if sessionID <= 0 {
		return 0, fmt.Errorf(
			"lock conversation sequence: invalid session id %d",
			sessionID,
		)
	}

	conn, err := txctx.SessionConn(ctx)
	if err != nil {
		return 0, fmt.Errorf(
			"lock conversation sequence for session %d: %w",
			sessionID,
			err,
		)
	}

	const lockQuery = `
SELECT last_seq FROM conversation_sequence WHERE session_id = ? FOR UPDATE`

	var lastSeq int64
	if err := conn.QueryRowCtx(ctx, &lastSeq, lockQuery, sessionID); err != nil {
		if errors.Is(err, sqlx.ErrNotFound) {
			return 0, fmt.Errorf("%w: session_id=%d", ErrRowMissing, sessionID)
		}
		return 0, fmt.Errorf(
			"lock conversation sequence for session %d: %w",
			sessionID,
			err,
		)
	}

	nextSeq := lastSeq + 1

	const incrementQuery = `
UPDATE conversation_sequence SET last_seq = ? WHERE session_id = ?`

	result, err := conn.ExecCtx(ctx, incrementQuery, nextSeq, sessionID)
	if err != nil {
		return 0, fmt.Errorf(
			"increment conversation sequence for session %d: %w",
			sessionID,
			err,
		)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf(
			"increment conversation sequence for session %d: read affected rows: %w",
			sessionID,
			err,
		)
	}
	if affected != 1 {
		return 0, fmt.Errorf(
			"increment conversation sequence for session %d: updated %d rows",
			sessionID,
			affected,
		)
	}

	return nextSeq, nil
}
