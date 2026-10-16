package user_balance

import (
	"context"
	"fmt"

	"infinitechatgo/common/txctx"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

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
	ctx context.Context,
	userID int64,
	amount int64,
) (bool, error) {
	if userID <= 0 || amount <= 0 {
		return false, fmt.Errorf("debit balance: user ID and amount must be positive")
	}
	conn, err := txctx.SessionConn(ctx)
	if err != nil {
		return false, fmt.Errorf("debit balance: %w", err)
	}
	result, err := conn.ExecCtx(ctx,
		"UPDATE user_balance SET balance = balance - ? WHERE user_id = ? AND balance >= ?",
		amount, userID, amount)
	if err != nil {
		return false, fmt.Errorf("debit balance for user %d: %w", userID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("debit balance for user %d: rows affected: %w", userID, err)
	}
	return affected == 1, nil
}

func (m *customUserBalanceModel) withSession(session sqlx.Session) UserBalanceModel {
	return NewUserBalanceModel(sqlx.NewSqlConnFromSession(session))
}
