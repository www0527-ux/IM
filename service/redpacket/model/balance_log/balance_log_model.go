package balance_log

import (
	"context"
	"database/sql"
	"errors"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var ErrInsertTxNotImplemented = errors.New("balance log InsertTx is not implemented")

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
	context.Context,
	*BalanceLog,
) (sql.Result, error) {
	// TODO(user): insert through txctx and preserve the business unique key.
	return nil, ErrInsertTxNotImplemented
}

func (m *customBalanceLogModel) withSession(session sqlx.Session) BalanceLogModel {
	return NewBalanceLogModel(sqlx.NewSqlConnFromSession(session))
}
