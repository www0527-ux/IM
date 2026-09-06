package user

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"infinitechatgo/common/txctx"
	"strings"

	"github.com/zeromicro/go-zero/core/stores/cache"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

// 编译期接口检查
var _ UserModel = (*customUserModel)(nil)

type (
	// UserModel is an interface to be customized, add more methods here,
	// and implement the added methods in customUserModel.
	UserModel interface {
		userModel
		ClearCache(ctx context.Context, data *User) error //事务提交后的缓存清理
		InsertTx(
			ctx context.Context,
			data *User,
		) (sql.Result, error)

		UpdateTx(
			ctx context.Context,
			data *User,
		) error

		DeleteTx(
			ctx context.Context,
			userId int64,
		) error

		FindActiveUserIdsForUpdateTx(
			ctx context.Context,
			userIds []int64,
		) ([]int64, error)
		FindBasicByIds(
			ctx context.Context,
			userIds []int64,
		) ([]*UserBasicView, error)
	}
	//结构体嵌入
	customUserModel struct {
		*defaultUserModel
	}
)

// NewUserModel returns a model for the database table.
func NewUserModel(conn sqlx.SqlConn, c cache.CacheConf, opts ...cache.Option) UserModel {

	return &customUserModel{
		defaultUserModel: newUserModel(conn, c, opts...),
	}
}
func (m *customUserModel) InsertTx(
	ctx context.Context,
	data *User,
) (sql.Result, error) {
	session := txctx.GetSession(ctx)
	//?
	if session == nil {
		return m.Insert(ctx, data)
	}
	query := fmt.Sprintf(
		"insert into %s (%s) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		m.table,
		userRowsExpectAutoSet, //列名列表，其中remove了go-zero认为可能由数据库自动生成的部分
	)
	return session.ExecCtx(
		ctx,
		query,
		data.UserId,
		data.Phone,
		data.Email,
		data.Password,
		data.Nickname,
		data.Avatar,
		data.Gender,
		data.Description,
		data.State,
		data.Role,
		data.CreatedTime,
		data.UpdatedTime,
		data.IsDelete,
	)
}
func (m *customUserModel) UpdateTx(
	ctx context.Context,
	data *User,
) error {
	session := txctx.GetSession(ctx)

	if session == nil {
		return m.Update(ctx, data)
	}

	query := fmt.Sprintf(
		"update %s set %s where `user_id` = ?",
		m.table,
		userRowsWithPlaceHolder,
	)

	_, err := session.ExecCtx(
		ctx,
		query,
		data.Phone,
		data.Email,
		data.Password,
		data.Nickname,
		data.Avatar,
		data.Gender,
		data.Description,
		data.State,
		data.Role,
		data.CreatedTime,
		data.UpdatedTime,
		data.IsDelete,
		data.UserId,
	)

	return err
}

func (m *customUserModel) DeleteTx(
	ctx context.Context,
	userId int64,
) error {
	session := txctx.GetSession(ctx)

	if session == nil {
		return m.Delete(ctx, userId)
	}

	query := fmt.Sprintf(
		"delete from %s where `user_id` = ?",
		m.table,
	)

	_, err := session.ExecCtx(ctx, query, userId)
	return err
}

func (m *customUserModel) FindActiveUserIdsForUpdateTx(
	ctx context.Context,
	userIds []int64,
) ([]int64, error) {
	session := txctx.GetSession(ctx)
	if session == nil {
		return nil, errors.New(
			"FindActiveUserIdsForUpdateTx must be called inside a transaction",
		)
	}
	if len(userIds) == 0 {
		return []int64{}, nil
	}

	placeholders := strings.TrimSuffix(
		strings.Repeat("?,", len(userIds)),
		",",
	)
	query := fmt.Sprintf(
		"select `user_id` from %s "+
			"where `user_id` in (%s) "+
			"and `state` = 0 and `is_delete` = 0 "+
			"order by `user_id` for update",
		m.table,
		placeholders,
	)

	args := make([]any, 0, len(userIds))
	for _, userId := range userIds {
		args = append(args, userId)
	}

	var activeUserIds []int64
	if err := session.QueryRowsCtx(
		ctx,
		&activeUserIds,
		query,
		args...,
	); err != nil {
		return nil, err
	}

	return activeUserIds, nil
}

func (m *customUserModel) ClearCache(
	ctx context.Context,
	data *User,
) error {
	keys := []string{
		fmt.Sprintf("%s%v", cacheUserUserIdPrefix, data.UserId),
	}
	if data.Email.Valid {
		keys = append(
			keys,
			fmt.Sprintf("%s%v", cacheUserEmailPrefix, data.Email),
		)
	}
	if data.Phone.Valid {
		keys = append(
			keys,
			fmt.Sprintf("%s%v", cacheUserPhonePrefix, data.Phone),
		)
	}
	return m.DelCacheCtx(ctx, keys...)
}
