package balance_log

import (
	"context"
	"database/sql"
	"fmt"

	"infinitechatgo/common/txctx"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var _ BalanceLogModel = (*customBalanceLogModel)(nil)

type (
	// BalanceLogModel is an interface to be customized, add more methods here,
	// and implement the added methods in customBalanceLogModel.
	BalanceLogModel interface {
		balanceLogModel
		InsertTx(ctx context.Context, data *BalanceLog) (sql.Result, error)
		withSession(session sqlx.Session) BalanceLogModel
	}

	customBalanceLogModel struct {
		*defaultBalanceLogModel
	}
)

// NewBalanceLogModel returns a model for the database table.
func NewBalanceLogModel(conn sqlx.SqlConn) BalanceLogModel {
	return &customBalanceLogModel{
		defaultBalanceLogModel: newBalanceLogModel(conn),
	}
}

func (m *customBalanceLogModel) InsertTx(
	ctx context.Context,
	data *BalanceLog,
) (sql.Result, error) {
	if data == nil || data.BalanceLogId <= 0 || data.UserId <= 0 ||
		data.Amount == 0 || data.RelatedId <= 0 {
		return nil, fmt.Errorf("insert balance log: invalid data")
	}
	conn, err := txctx.SessionConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("insert balance log: %w", err)
	}
	result, err := conn.ExecCtx(ctx, `INSERT INTO balance_log
		(balance_log_id, user_id, amount, type, related_id)
		VALUES (?, ?, ?, ?, ?)`, data.BalanceLogId, data.UserId, data.Amount,
		data.Type, data.RelatedId)
	if err != nil {
		return nil, fmt.Errorf("insert balance log %d: %w", data.BalanceLogId, err)
	}
	return result, nil
}

func (m *customBalanceLogModel) withSession(session sqlx.Session) BalanceLogModel {
	return NewBalanceLogModel(sqlx.NewSqlConnFromSession(session))
}
