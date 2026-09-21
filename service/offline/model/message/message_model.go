package message

import (
	"context"
	"errors"
	"fmt"
	"strings"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var _ MessageModel = (*customMessageModel)(nil)

type (
	// MessageModel is an interface to be customized, add more methods here,
	// and implement the added methods in customMessageModel.
	MessageModel interface {
		messageModel
		InsertIdempotently(ctx context.Context, data *Message) (*Message, bool, error)
		MaxSeq(ctx context.Context, sessionID int64) (int64, error)
		FindBySessionSeqs(ctx context.Context, sessionID int64, seqs []int64) ([]*Message, error)
		FindBySessionSeqRange(ctx context.Context, sessionID, afterSeq, throughSeq int64) ([]*Message, error)
		withSession(session sqlx.Session) MessageModel
	}

	customMessageModel struct {
		*defaultMessageModel
	}
)

// ErrIdempotencyConflict means the same idempotency key was reused for a
// different message, or a server message ID collided with another row.
var ErrIdempotencyConflict = errors.New("message idempotency conflict")

// NewMessageModel returns a model for the database table.
func NewMessageModel(conn sqlx.SqlConn) MessageModel {
	return &customMessageModel{
		defaultMessageModel: newMessageModel(conn),
	}
}

func (m *customMessageModel) FindBySessionSeqRange(
	ctx context.Context,
	sessionID int64,
	afterSeq int64,
	throughSeq int64,
) ([]*Message, error) {
	if throughSeq <= afterSeq {
		return []*Message{}, nil
	}
	query := fmt.Sprintf(
		"SELECT %s FROM %s WHERE session_id = ? AND seq > ? AND seq <= ? ORDER BY seq ASC",
		messageRows,
		m.table,
	)
	var messages []*Message
	if err := m.conn.QueryRowsCtx(
		ctx,
		&messages,
		query,
		sessionID,
		afterSeq,
		throughSeq,
	); err != nil {
		return nil, err
	}
	return messages, nil
}

func (m *customMessageModel) withSession(session sqlx.Session) MessageModel {
	return NewMessageModel(sqlx.NewSqlConnFromSession(session))
}

func (m *customMessageModel) MaxSeq(
	ctx context.Context,
	sessionID int64,
) (int64, error) {
	const query = `SELECT COALESCE(MAX(seq), 0) FROM message WHERE session_id = ?`
	var maxSeq int64
	if err := m.conn.QueryRowCtx(ctx, &maxSeq, query, sessionID); err != nil {
		return 0, err
	}
	return maxSeq, nil
}

func (m *customMessageModel) FindBySessionSeqs(
	ctx context.Context,
	sessionID int64,
	seqs []int64,
) ([]*Message, error) {
	if len(seqs) == 0 {
		return []*Message{}, nil
	}

	placeholders := make([]string, len(seqs))
	args := make([]any, 0, len(seqs)+1)
	args = append(args, sessionID)
	for index, seq := range seqs {
		placeholders[index] = "?"
		args = append(args, seq)
	}

	query := fmt.Sprintf(
		"SELECT %s FROM %s WHERE session_id = ? AND seq IN (%s) ORDER BY seq ASC",
		messageRows,
		m.table,
		strings.Join(placeholders, ","),
	)
	var messages []*Message
	if err := m.conn.QueryRowsCtx(ctx, &messages, query, args...); err != nil {
		return nil, err
	}
	return messages, nil
}

// InsertIdempotently makes Kafka redelivery safe. The returned bool is true
// only when this call inserted a new row; false means the same row existed.
func (m *customMessageModel) InsertIdempotently(
	ctx context.Context,
	data *Message,
) (*Message, bool, error) {
	if _, err := m.Insert(ctx, data); err == nil {
		return data, true, nil
	} else if !isDuplicateKeyError(err) {
		//非唯一键错误,认为为数据库system错误,直接返回
		return nil, false, err
	}

	existing, err := m.FindOneBySenderIdClientMessageId(
		ctx,
		data.SenderId,
		data.ClientMessageId,
	)
	if err == nil {
		if sameMessage(existing, data) {
			return existing, false, nil
		}

		return nil, false, fmt.Errorf(
			"%w: sender_id=%d client_message_id=%q",
			ErrIdempotencyConflict,
			data.SenderId,
			data.ClientMessageId,
		)
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, false, err
	}

	// No row owns the business idempotency key, so the duplicate was caused by
	// message_id colliding with another row.
	return nil, false, fmt.Errorf(
		"%w: message_id=%d",
		ErrIdempotencyConflict,
		data.MessageId,
	)
}

func isDuplicateKeyError(err error) bool {
	var mysqlErr *mysqlDriver.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 //1062 is the MySQL error code for duplicate entry
}

func sameMessage(left, right *Message) bool {
	return left.MessageId == right.MessageId &&
		left.ClientMessageId == right.ClientMessageId &&
		left.SenderId == right.SenderId &&
		left.SessionId == right.SessionId &&
		left.Seq == right.Seq &&
		left.SessionType == right.SessionType &&
		left.MessageType == right.MessageType &&
		left.RedPacketId.Valid == right.RedPacketId.Valid &&
		(!left.RedPacketId.Valid || left.RedPacketId.Int64 == right.RedPacketId.Int64) &&
		left.Content == right.Content &&
		left.ReplyId.Valid == right.ReplyId.Valid &&
		(!left.ReplyId.Valid || left.ReplyId.Int64 == right.ReplyId.Int64) &&
		left.CreatedTime.UnixMilli() == right.CreatedTime.UnixMilli()
}
