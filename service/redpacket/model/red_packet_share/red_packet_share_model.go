package red_packet_share

import (
	"context"
	"fmt"
	"strings"

	"infinitechatgo/common/txctx"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

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
	ctx context.Context,
	shares []*RedPacketShare,
) error {
	if len(shares) == 0 {
		return fmt.Errorf("insert red packet shares: empty batch")
	}
	conn, err := txctx.SessionConn(ctx)
	if err != nil {
		return fmt.Errorf("insert red packet shares: %w", err)
	}
	var query strings.Builder
	query.WriteString("INSERT INTO red_packet_share (share_id, red_packet_id, ordinal, amount, status) VALUES ")
	args := make([]any, 0, len(shares)*5)
	for index, share := range shares {
		if share == nil || share.ShareId <= 0 || share.RedPacketId <= 0 ||
			share.Ordinal <= 0 || share.Amount <= 0 {
			return fmt.Errorf("insert red packet shares: invalid share at index %d", index)
		}
		if index > 0 {
			query.WriteByte(',')
		}
		query.WriteString("(?, ?, ?, ?, ?)")
		args = append(args, share.ShareId, share.RedPacketId, share.Ordinal,
			share.Amount, share.Status)
	}
	if _, err := conn.ExecCtx(ctx, query.String(), args...); err != nil {
		return fmt.Errorf("insert %d red packet shares: %w", len(shares), err)
	}
	return nil
}

func (m *customRedPacketShareModel) withSession(session sqlx.Session) RedPacketShareModel {
	return NewRedPacketShareModel(sqlx.NewSqlConnFromSession(session))
}
