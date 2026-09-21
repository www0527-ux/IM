package red_packet_share

import (
	"context"
	"errors"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var ErrInsertBatchTxNotImplemented = errors.New("red packet share InsertBatchTx is not implemented")

var _ RedPacketShareModel = (*customRedPacketShareModel)(nil)

type (
	// RedPacketShareModel is an interface to be customized, add more methods here,
	// and implement the added methods in customRedPacketShareModel.
	RedPacketShareModel interface {
		redPacketShareModel
		// InsertBatchTx persists every precomputed share in the surrounding
		// transaction. Partial insertion must be impossible.
		InsertBatchTx(ctx context.Context, shares []*RedPacketShare) error
		withSession(session sqlx.Session) RedPacketShareModel
	}

	customRedPacketShareModel struct {
		*defaultRedPacketShareModel
	}
)

// NewRedPacketShareModel returns a model for the database table.
func NewRedPacketShareModel(conn sqlx.SqlConn) RedPacketShareModel {
	return &customRedPacketShareModel{
		defaultRedPacketShareModel: newRedPacketShareModel(conn),
	}
}

func (m *customRedPacketShareModel) InsertBatchTx(
	context.Context,
	[]*RedPacketShare,
) error {
	// TODO(user): implement one transaction-bound batch insert. Reject an empty
	// transaction context instead of executing on the base connection.
	return ErrInsertBatchTxNotImplemented
}

func (m *customRedPacketShareModel) withSession(session sqlx.Session) RedPacketShareModel {
	return NewRedPacketShareModel(sqlx.NewSqlConnFromSession(session))
}
