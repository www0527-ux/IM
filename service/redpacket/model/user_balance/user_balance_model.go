package user_balance

import (
	"context"
	"errors"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var ErrDebitIfEnoughTxNotImplemented = errors.New("user balance DebitIfEnoughTx is not implemented")

var _ UserBalanceModel = (*customUserBalanceModel)(nil)

type (
	// UserBalanceModel is an interface to be customized, add more methods here,
	// and implement the added methods in customUserBalanceModel.
	UserBalanceModel interface {
		userBalanceModel
		// DebitIfEnoughTx atomically subtracts amount only when the current
		// balance is sufficient. The bool is true only when one row changed.
		DebitIfEnoughTx(ctx context.Context, userID, amount int64) (bool, error)
		withSession(session sqlx.Session) UserBalanceModel
	}

	customUserBalanceModel struct {
		*defaultUserBalanceModel
	}
)

// NewUserBalanceModel returns a model for the database table.
func NewUserBalanceModel(conn sqlx.SqlConn) UserBalanceModel {
	return &customUserBalanceModel{
		defaultUserBalanceModel: newUserBalanceModel(conn),
	}
}

func (m *customUserBalanceModel) DebitIfEnoughTx(
	context.Context,
	int64,
	int64,
) (bool, error) {
	// TODO(user): implement a single conditional UPDATE inside txctx.
	return false, ErrDebitIfEnoughTxNotImplemented
}

func (m *customUserBalanceModel) withSession(session sqlx.Session) UserBalanceModel {
	return NewUserBalanceModel(sqlx.NewSqlConnFromSession(session))
}
