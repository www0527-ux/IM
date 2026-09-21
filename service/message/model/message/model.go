package message

import (
	"context"
	"errors"

	messageevent "infinitechatgo/common/model/event"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var (
	ErrNotFound       = errors.New("message not found")
	ErrNotImplemented = errors.New("message transaction model is not implemented")
)

// Model is the transaction-aware boundary required by MessageWriteService.
// Both methods must use the sqlx.Session carried by txctx; they must never
// open an independent transaction.
type Model interface {
	FindByIdempotencyTx(
		ctx context.Context,
		senderID int64,
		clientMessageID string,
	) (messageevent.ChatMessage, error)
	InsertTx(ctx context.Context, message messageevent.ChatMessage) error
}

type model struct {
	conn sqlx.SqlConn
}

func NewModel(conn sqlx.SqlConn) Model {
	return &model{conn: conn}
}

func (m *model) FindByIdempotencyTx(
	context.Context,
	int64,
	string,
) (messageevent.ChatMessage, error) {
	// TODO(user): query sender_id + client_message_id through txctx session.
	// Return ErrNotFound only for a confirmed absence. Comparing immutable
	// fields and classifying conflicts belongs in the persistent writer.
	return messageevent.ChatMessage{}, ErrNotImplemented
}

func (m *model) InsertTx(
	context.Context,
	messageevent.ChatMessage,
) error {
	// TODO(user): insert through the current txctx session. Preserve the
	// message_id, sender/client, session/seq and red_packet_id unique indexes.
	return ErrNotImplemented
}
