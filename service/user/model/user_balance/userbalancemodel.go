package user_balance

import (
	"context"
	"database/sql"
	"fmt"
	"infinitechatgo/common/txctx"

	"github.com/zeromicro/go-zero/core/stores/cache"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var _ UserBalanceModel = (*customUserBalanceModel)(nil)

type (
	// UserBalanceModel is an interface to be customized, add more methods here,
	// and implement the added methods in customUserBalanceModel.
	UserBalanceModel interface {
		userBalanceModel
		InsertTx(
			ctx context.Context,
			data *UserBalance,
		) (sql.Result, error)
	}

	customUserBalanceModel struct {
		*defaultUserBalanceModel
	}
)

// NewUserBalanceModel returns a model for the database table.
func NewUserBalanceModel(conn sqlx.SqlConn, c cache.CacheConf, opts ...cache.Option) UserBalanceModel {
	return &customUserBalanceModel{
		defaultUserBalanceModel: newUserBalanceModel(conn, c, opts...),
	}
}
func (m *customUserBalanceModel) InsertTx(
	ctx context.Context,
	data *UserBalance,
) (sql.Result, error) {
	session := txctx.GetSession(ctx)

	//?
	if session == nil {
		return m.Insert(ctx, data)
	}

	query := fmt.Sprintf(
		"insert into %s (%s) values (?,?,?,?)",
		m.table,
		userBalanceRowsExpectAutoSet,
	)
	return session.ExecCtx(
		ctx,
		query,
		data.UserId,
		data.Balance,
		data.CreatedTime,
		data.UpdatedTime,
	)
}
