package outbox_event

import (
	"context"
	"database/sql"
	"errors"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var ErrInsertTxNotImplemented = errors.New("outbox event InsertTx is not implemented")

var _ OutboxEventModel = (*customOutboxEventModel)(nil)

type (
	// OutboxEventModel is an interface to be customized, add more methods here,
	// and implement the added methods in customOutboxEventModel.
	OutboxEventModel interface {
		outboxEventModel
		InsertTx(ctx context.Context, data *OutboxEvent) (sql.Result, error)
		withSession(session sqlx.Session) OutboxEventModel
	}

	customOutboxEventModel struct {
		*defaultOutboxEventModel
	}
)

// NewOutboxEventModel returns a model for the database table.
func NewOutboxEventModel(conn sqlx.SqlConn) OutboxEventModel {
	return &customOutboxEventModel{
		defaultOutboxEventModel: newOutboxEventModel(conn),
	}
}

func (m *customOutboxEventModel) InsertTx(
	context.Context,
	*OutboxEvent,
) (sql.Result, error) {
	// TODO(user): insert through txctx. The event must commit or roll back with
	// the red packet aggregate that produced it.
	return nil, ErrInsertTxNotImplemented
}

func (m *customOutboxEventModel) withSession(session sqlx.Session) OutboxEventModel {
	return NewOutboxEventModel(sqlx.NewSqlConnFromSession(session))
}
