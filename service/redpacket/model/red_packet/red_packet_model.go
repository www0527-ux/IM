package red_packet

import (
	"context"
	"database/sql"
	"fmt"

	"infinitechatgo/common/txctx"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

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
	ctx context.Context,
	data *RedPacket,
) (sql.Result, error) {
	if data == nil || data.RedPacketId <= 0 || data.MessageId <= 0 ||
		data.SenderId <= 0 || data.SessionId <= 0 || data.ClientMessageId == "" ||
		data.TotalAmount <= 0 || data.TotalCount <= 0 || data.ExpireAt.IsZero() {
		return nil, fmt.Errorf("insert red packet: invalid data")
	}
	conn, err := txctx.SessionConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("insert red packet: %w", err)
	}
	result, err := conn.ExecCtx(ctx, `INSERT INTO red_packet
		(red_packet_id, message_id, sender_id, receiver_id, client_message_id,
		 session_id, session_type, red_packet_type, wrapper_text, total_amount,
		 total_count, received_amount, received_count, status, version, expire_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		data.RedPacketId, data.MessageId, data.SenderId, data.ReceiverId,
		data.ClientMessageId, data.SessionId, data.SessionType, data.RedPacketType,
		data.WrapperText, data.TotalAmount, data.TotalCount, data.ReceivedAmount,
		data.ReceivedCount, data.Status, data.Version, data.ExpireAt)
	if err != nil {
		return nil, fmt.Errorf("insert red packet %d: %w", data.RedPacketId, err)
	}
	return result, nil
}

func (m *customRedPacketModel) withSession(session sqlx.Session) RedPacketModel {
	return NewRedPacketModel(sqlx.NewSqlConnFromSession(session))
}
