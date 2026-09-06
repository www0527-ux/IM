package balance_log

import (
	"context"
	"fmt"

	"github.com/zeromicro/go-zero/core/stores/cache"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var _ BalanceLogModel = (*customBalanceLogModel)(nil)

type (
	// BalanceLogModel is an interface to be customized, add more methods here,
	// and implement the added methods in customBalanceLogModel.
	BalanceLogModel interface {
		balanceLogModel

		CountByUserId(
			ctx context.Context,
			userId int64,
		) (int64, error)

		FindPageByUserId(
			ctx context.Context,
			userId int64,
			pageNum int,
			pageSize int,
		) ([]*BalanceLog, error)
	}

	customBalanceLogModel struct {
		*defaultBalanceLogModel
	}
)

// NewBalanceLogModel returns a model for the database table.
func NewBalanceLogModel(conn sqlx.SqlConn, c cache.CacheConf, opts ...cache.Option) BalanceLogModel {
	return &customBalanceLogModel{
		defaultBalanceLogModel: newBalanceLogModel(conn, c, opts...),
	}
}
func (m *customBalanceLogModel) CountByUserId(
	ctx context.Context,
	userId int64,
) (int64, error) {
	query := fmt.Sprintf(
		"select count(*) from %s where `user_id` = ?",
		m.table,
	)

	var count int64
	//跳过redis,接受一行结果
	err := m.QueryRowNoCacheCtx(
		ctx,
		&count,
		query,
		userId,
	)
	if err != nil {
		return 0, err
	}

	return count, nil
}
func (m *customBalanceLogModel) FindPageByUserId(
	ctx context.Context,
	userId int64,
	pageNum int,
	pageSize int,
) ([]*BalanceLog, error) {
	offset := (pageNum - 1) * pageSize

	query := fmt.Sprintf(
		"select %s from %s "+
			"where `user_id` = ? "+
			"order by `created_time` desc "+
			"limit ?, ?",
		balanceLogRows,
		m.table,
	)

	var logs []*BalanceLog

	err := m.QueryRowsNoCacheCtx(
		ctx,
		&logs,
		query,
		userId,
		offset,
		pageSize,
	)
	if err != nil {
		return nil, err
	}

	return logs, nil
}
