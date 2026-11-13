package message

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	messageevent "infinitechatgo/common/model/event"
	"infinitechatgo/common/txctx"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

// ErrNotFound is returned only for a confirmed absence: the idempotency key
// owns no row.
var ErrNotFound = errors.New("message not found")

const messageColumns = "message_id, client_message_id, sender_id, session_id, " +
	"seq, session_type, message_type, red_packet_id, content, reply_id, created_time"

// Model is the transaction-aware boundary required by MessageWriteService.
// All methods must use the sqlx.Session carried by txctx; they must never
// open an independent transaction.
type Model interface {
	FindByIdempotencyTx(
		ctx context.Context,
		senderID int64,
		clientMessageID string,
	) (messageevent.ChatMessage, error)
	InsertTx(ctx context.Context, message messageevent.ChatMessage) error
	FindByIdempotencyBatchTx(ctx context.Context, keys []IdempotencyKey) (map[IdempotencyKey]messageevent.ChatMessage, error)
	InsertBatchTx(ctx context.Context, messages []messageevent.ChatMessage) error
}

type model struct {
	conn sqlx.SqlConn
}

func NewModel(conn sqlx.SqlConn) Model {
	return &model{conn: conn}
}

func (m *model) FindByIdempotencyTx(
	ctx context.Context,
	senderID int64,
	clientMessageID string,
) (messageevent.ChatMessage, error) {
	if senderID <= 0 || strings.TrimSpace(clientMessageID) == "" {
		return messageevent.ChatMessage{}, fmt.Errorf(
			"find message by idempotency key: invalid sender_id=%d client_message_id=%q",
			senderID,
			clientMessageID,
		)
	}

	conn, err := txctx.SessionConn(ctx)
	if err != nil {
		return messageevent.ChatMessage{}, fmt.Errorf(
			"find message by idempotency key: %w",
			err,
		)
	}

	// uk_sender_client_message serves this lookup, and reading inside the
	// caller's transaction is what makes a concurrent insert visible before the
	// Seq row lock is taken.
	query := fmt.Sprintf(
		"SELECT %s FROM message WHERE sender_id = ? AND client_message_id = ? LIMIT 1",
		messageColumns,
	)

	var stored row
	if err := conn.QueryRowCtx(
		ctx,
		&stored,
		query,
		senderID,
		clientMessageID,
	); err != nil {
		if errors.Is(err, sqlx.ErrNotFound) {
			return messageevent.ChatMessage{}, ErrNotFound
		}
		return messageevent.ChatMessage{}, fmt.Errorf(
			"find message by sender %d client message %q: %w",
			senderID,
			clientMessageID,
			err,
		)
	}

	return stored.toChatMessage(), nil
}

func (m *model) InsertTx(
	ctx context.Context,
	message messageevent.ChatMessage,
) error {
	if message.MessageID <= 0 || message.SenderID <= 0 ||
		message.SessionID <= 0 || message.Seq <= 0 ||
		strings.TrimSpace(message.ClientMessageID) == "" {
		return fmt.Errorf(
			"insert message: incomplete message message_id=%d session_id=%d seq=%d",
			message.MessageID,
			message.SessionID,
			message.Seq,
		)
	}
	if message.Body.RedPacketID != nil &&
		message.MessageType != messageevent.MessageTypeRedPacket {
		return fmt.Errorf(
			"insert message %d: red packet id on a non red packet message",
			message.MessageID,
		)
	}

	conn, err := txctx.SessionConn(ctx)
	if err != nil {
		return fmt.Errorf("insert message %d: %w", message.MessageID, err)
	}

	// A duplicate here means either uk_sender_client_message (the same request
	// raced this one) or uk_session_seq (the Seq row lock was bypassed). The
	// writer resolves that race; this layer only reports it.
	const query = `
INSERT INTO message
    (message_id, client_message_id, sender_id, session_id, seq, session_type,
     message_type, red_packet_id, content, reply_id, created_time)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	if _, err := conn.ExecCtx(
		ctx,
		query,
		message.MessageID,
		message.ClientMessageID,
		message.SenderID,
		message.SessionID,
		message.Seq,
		int64(message.SessionType),
		int64(message.MessageType),
		nullInt64(message.Body.RedPacketID),
		message.Body.Content,
		nullInt64(message.Body.ReplyID),
		time.UnixMilli(message.CreatedAt),
	); err != nil {
		return fmt.Errorf("insert message %d: %w", message.MessageID, err)
	}
	return nil
}

// IsDuplicateKeyError reports MySQL error 1062, the signal that a unique index
// rejected the write. The persistent writer uses it to tell an idempotent race
// apart from an infrastructure failure.
func IsDuplicateKeyError(err error) bool {
	var mysqlErr *mysqlDriver.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1062
}

// row mirrors the message table so a transaction-scoped read can be converted
// back into the event DTO. The table has no receiver_id column, so a message
// read back from MySQL always carries ReceiverID=nil: for a single chat the
// receiver is derived from the session instead of stored.
type row struct {
	MessageID       int64         `db:"message_id"`
	ClientMessageID string        `db:"client_message_id"`
	SenderID        int64         `db:"sender_id"`
	SessionID       int64         `db:"session_id"`
	Seq             int64         `db:"seq"`
	SessionType     int64         `db:"session_type"`
	MessageType     int64         `db:"message_type"`
	RedPacketID     sql.NullInt64 `db:"red_packet_id"`
	Content         string        `db:"content"`
	ReplyID         sql.NullInt64 `db:"reply_id"`
	CreatedTime     time.Time     `db:"created_time"`
}

func (r row) toChatMessage() messageevent.ChatMessage {
	message := messageevent.ChatMessage{
		MessageID:       r.MessageID,
		ClientMessageID: r.ClientMessageID,
		SessionID:       r.SessionID,
		SessionType:     messageevent.SessionType(r.SessionType),
		Seq:             r.Seq,
		SenderID:        r.SenderID,
		MessageType:     messageevent.MessageType(r.MessageType),
		Body: messageevent.MessageBody{
			Content: r.Content,
		},
		CreatedAt: r.CreatedTime.UnixMilli(),
	}
	if r.RedPacketID.Valid {
		redPacketID := r.RedPacketID.Int64
		message.Body.RedPacketID = &redPacketID
		if message.MessageType == messageevent.MessageTypeRedPacket {
			message.Body.RedPacketWrapperText = r.Content
		}
	}
	if r.ReplyID.Valid {
		replyID := r.ReplyID.Int64
		message.Body.ReplyID = &replyID
	}
	return message
}

func nullInt64(value *int64) sql.NullInt64 {
	if value == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: *value, Valid: true}
}
