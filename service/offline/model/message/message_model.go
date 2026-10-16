package message

import (
	"context"
	"fmt"
	"strings"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var _ MessageModel = (*customMessageModel)(nil)

type (
	// MessageModel is an interface to be customized, add more methods here,
	// and implement the added methods in customMessageModel.
	MessageModel interface {
		messageModel
		MaxSeq(ctx context.Context, sessionID int64) (int64, error)
		FindBySessionSeqs(ctx context.Context, sessionID int64, seqs []int64) ([]*Message, error)
		FindBySessionSeqRange(ctx context.Context, sessionID, afterSeq, throughSeq int64) ([]*Message, error)
	}

	customMessageModel struct {
		*defaultMessageModel
	}
)

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
