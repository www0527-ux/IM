package conversation_sequence

import (
	"context"
	"errors"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var ErrNotImplemented = errors.New("conversation sequence transaction model is not implemented")

// Model contains the only allowed Seq allocation operations for the target
// pipeline. Implementations must operate on the txctx session supplied by the
// caller.
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

func (m *model) EnsureTx(context.Context, int64) error {
	// TODO(user): idempotently create last_seq=0 when the conversation row is
	// absent. Do not derive a live Seq using an unlocked MAX(message.seq).
	return ErrNotImplemented
}

func (m *model) LockAndIncrementTx(
	context.Context,
	int64,
) (int64, error) {
	// TODO(user): SELECT last_seq ... FOR UPDATE, then update last_seq+1 using
	// the same txctx session. Return the committed candidate Seq.
	return 0, ErrNotImplemented
}
