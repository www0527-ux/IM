package outbox_event

import (
	"context"
	"errors"

	messageevent "infinitechatgo/common/model/event"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var ErrNotImplemented = errors.New("message outbox transaction model is not implemented")

type Model interface {
	InsertPersistedTx(
		ctx context.Context,
		event messageevent.ChatMessagePersistedEvent,
		topic string,
		messageKey string,
	) error
}

type model struct {
	conn sqlx.SqlConn
}

func NewModel(conn sqlx.SqlConn) Model {
	return &model{conn: conn}
}

func (m *model) InsertPersistedTx(
	context.Context,
	messageevent.ChatMessagePersistedEvent,
	string,
	string,
) error {
	// TODO(user): insert MESSAGE_PERSISTED through the current txctx session.
	// aggregate_id and message_key are sessionID; aggregate_seq is message.Seq.
	return ErrNotImplemented
}
