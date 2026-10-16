package outbox_event

import (
	"context"
	"database/sql"
	"fmt"

	"infinitechatgo/common/txctx"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

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
	ctx context.Context,
	data *OutboxEvent,
) (sql.Result, error) {
	if data == nil || data.EventId <= 0 || data.AggregateId <= 0 ||
		data.EventType == "" || data.AggregateType == "" || data.Topic == "" ||
		data.MessageKey == "" || data.Payload == "" || data.NextRetryAt.IsZero() {
		return nil, fmt.Errorf("insert outbox event: invalid data")
	}
	conn, err := txctx.SessionConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("insert outbox event: %w", err)
	}
	result, err := conn.ExecCtx(ctx, `INSERT INTO outbox_event
		(event_id, event_type, aggregate_type, aggregate_id, aggregate_seq,
		 topic, message_key, payload, status, retry_count, next_retry_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		data.EventId, data.EventType, data.AggregateType, data.AggregateId,
		data.AggregateSeq, data.Topic, data.MessageKey, data.Payload,
		data.Status, data.RetryCount, data.NextRetryAt)
	if err != nil {
		return nil, fmt.Errorf("insert outbox event %d: %w", data.EventId, err)
	}
	return result, nil
}

func (m *customOutboxEventModel) withSession(session sqlx.Session) OutboxEventModel {
	return NewOutboxEventModel(sqlx.NewSqlConnFromSession(session))
}
