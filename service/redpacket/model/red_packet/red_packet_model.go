package red_packet

import (
	"context"
	"database/sql"
	"errors"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var ErrInsertTxNotImplemented = errors.New("red packet InsertTx is not implemented")

var _ RedPacketModel = (*customRedPacketModel)(nil)

type (
	// RedPacketModel is an interface to be customized, add more methods here,
	// and implement the added methods in customRedPacketModel.
	RedPacketModel interface {
		redPacketModel
		// InsertTx must insert through txctx.GetSession(ctx). It must fail when
		// called outside a transaction and must not fall back to m.conn.
		InsertTx(ctx context.Context, data *RedPacket) (sql.Result, error)
		withSession(session sqlx.Session) RedPacketModel
	}

	customRedPacketModel struct {
		*defaultRedPacketModel
	}
)

// NewRedPacketModel returns a model for the database table.
func NewRedPacketModel(conn sqlx.SqlConn) RedPacketModel {
	return &customRedPacketModel{
		defaultRedPacketModel: newRedPacketModel(conn),
	}
}

func (m *customRedPacketModel) InsertTx(
	context.Context,
	*RedPacket,
) (sql.Result, error) {
	// TODO(user): implement the transaction-bound insert after reviewing the
	// generated column order in red_packet_model_gen.go.
	return nil, ErrInsertTxNotImplemented
}

func (m *customRedPacketModel) withSession(session sqlx.Session) RedPacketModel {
	return NewRedPacketModel(sqlx.NewSqlConnFromSession(session))
}
