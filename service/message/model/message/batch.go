package message

import (
	"context"
	"fmt"
	"strings"
	"time"

	messageevent "infinitechatgo/common/model/event"
	"infinitechatgo/common/txctx"
	"infinitechatgo/service/message/application"
)

type IdempotencyKey struct {
	SenderID        int64
	ClientMessageID string
}

// FindByIdempotencyBatchTx uses the persistent unique key. A missing map entry
// is an absence in this snapshot, not a reservation: INSERT and its constraints
// still arbitrate races with another process or a red packet transaction.
func (m *model) FindByIdempotencyBatchTx(ctx context.Context, keys []IdempotencyKey) (map[IdempotencyKey]messageevent.ChatMessage, error) {
	if len(keys) == 0 || len(keys) > application.MaxBatchSize {
		return nil, fmt.Errorf("invalid message idempotency batch size: %d", len(keys))
	}
	conn, err := txctx.SessionConn(ctx)
	if err != nil {
		return nil, err
	}
	placeholders := make([]string, len(keys))
	args := make([]any, 0, len(keys)*2)
	for i, key := range keys {
		if key.SenderID <= 0 || strings.TrimSpace(key.ClientMessageID) == "" {
			return nil, fmt.Errorf("invalid message idempotency batch key")
		}
		placeholders[i] = "(?, ?)"
		args = append(args, key.SenderID, key.ClientMessageID)
	}
	query := "SELECT " + messageColumns + " FROM message WHERE (sender_id, client_message_id) IN (" + strings.Join(placeholders, ", ") + ")"
	var rows []row
	if err := conn.QueryRowsCtx(ctx, &rows, query, args...); err != nil {
		return nil, fmt.Errorf("find message batch by idempotency: %w", err)
	}
	result := make(map[IdempotencyKey]messageevent.ChatMessage, len(rows))
	for _, stored := range rows {
		result[IdempotencyKey{stored.SenderID, stored.ClientMessageID}] = stored.toChatMessage()
	}
	return result, nil
}

// InsertBatchTx is one multi-row INSERT in the caller's transaction. Never use
// IGNORE or an upsert: a collision must roll back the batch before resolution.
func (m *model) InsertBatchTx(ctx context.Context, messages []messageevent.ChatMessage) error {
	if len(messages) == 0 || len(messages) > application.MaxBatchSize {
		return fmt.Errorf("invalid message insert batch size: %d", len(messages))
	}
	conn, err := txctx.SessionConn(ctx)
	if err != nil {
		return err
	}
	placeholders := make([]string, len(messages))
	args := make([]any, 0, len(messages)*11)
	for i, message := range messages {
		if message.MessageID <= 0 || message.SenderID <= 0 || message.SessionID <= 0 || message.Seq <= 0 ||
			strings.TrimSpace(message.ClientMessageID) == "" ||
			(message.Body.RedPacketID != nil && message.MessageType != messageevent.MessageTypeRedPacket) {
			return fmt.Errorf("invalid message in insert batch at index %d", i)
		}
		placeholders[i] = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
		args = append(args, message.MessageID, message.ClientMessageID, message.SenderID,
			message.SessionID, message.Seq, int64(message.SessionType), int64(message.MessageType),
			nullInt64(message.Body.RedPacketID), message.Body.Content, nullInt64(message.Body.ReplyID), time.UnixMilli(message.CreatedAt))
	}
	query := "INSERT INTO message (" + messageColumns + ") VALUES " + strings.Join(placeholders, ", ")
	if _, err := conn.ExecCtx(ctx, query, args...); err != nil {
		return fmt.Errorf("insert message batch: %w", err)
	}
	return nil
}
