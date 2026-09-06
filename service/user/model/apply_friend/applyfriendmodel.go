package apply_friend

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"infinitechatgo/common/txctx"
	"time"

	"github.com/zeromicro/go-zero/core/stores/cache"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var _ ApplyFriendModel = (*customApplyFriendModel)(nil)

type (
	// ApplyFriendModel is an interface to be customized, add more methods here,
	// and implement the added methods in customApplyFriendModel.
	ApplyFriendModel interface {
		applyFriendModel
		FindPageByUserId(
			ctx context.Context,
			userId int64,
			pageNum int,
			pageSize int,
		) ([]*ApplyFriendView, int64, error)
		CountByReceiverAndStatus(
			ctx context.Context,
			receiverId int64,
			status int64,
		) (int64, error)
		MarkReceivedUnreadAsRead(ctx context.Context, receiverId int64) (int64, error)
		FindOneForUpdateTx(
			ctx context.Context,
			applyFriendId int64,
		) (*ApplyFriend, error)
		UpdateTx(
			ctx context.Context,
			data *ApplyFriend,
		) error
		ClearCache(
			ctx context.Context,
			data *ApplyFriend,
		) error
	}

	ApplyFriendView struct {
		ApplyFriendId int64          `db:"apply_friend_id"`
		UserId        int64          `db:"user_id"`
		Nickname      sql.NullString `db:"nickname"`
		Avatar        sql.NullString `db:"avatar"`
		Message       string         `db:"message"`
		Status        int64          `db:"status"`
		IsReceiver    int64          `db:"is_receiver"`
		UpdatedTime   time.Time      `db:"updated_time"`
	}

	customApplyFriendModel struct {
		*defaultApplyFriendModel
	}
)

// NewApplyFriendModel returns a model for the database table.
func NewApplyFriendModel(conn sqlx.SqlConn, c cache.CacheConf, opts ...cache.Option) ApplyFriendModel {
	return &customApplyFriendModel{
		defaultApplyFriendModel: newApplyFriendModel(conn, c, opts...),
	}
}

func (m *customApplyFriendModel) FindPageByUserId(
	ctx context.Context,
	userId int64,
	pageNum int,
	pageSize int,
) ([]*ApplyFriendView, int64, error) {
	countQuery := fmt.Sprintf(
		"select count(*) from %s where `sender_id` = ? or `receiver_id` = ?",
		m.table,
	)

	var total int64
	if err := m.QueryRowNoCacheCtx(
		ctx,
		&total,
		countQuery,
		userId,
		userId,
	); err != nil {
		return nil, 0, err
	}

	if total == 0 {
		return []*ApplyFriendView{}, 0, nil
	}

	offset := (pageNum - 1) * pageSize
	query := fmt.Sprintf(
		"select a.`apply_friend_id`, "+
			"case when a.`receiver_id` = ? then a.`sender_id` else a.`receiver_id` end as `user_id`, "+
			"u.`nickname`, u.`avatar`, a.`message`, a.`status`, "+
			"(a.`receiver_id` = ?) as `is_receiver`, a.`updated_time` "+
			"from %s a left join `user` u on u.`user_id` = "+
			"case when a.`receiver_id` = ? then a.`sender_id` else a.`receiver_id` end "+
			"and u.`is_delete` = 0 "+
			"where a.`sender_id` = ? or a.`receiver_id` = ? "+
			"order by a.`updated_time` desc, a.`apply_friend_id` desc "+
			"limit ? offset ?",
		m.table,
	)

	var applications []*ApplyFriendView
	if err := m.QueryRowsNoCacheCtx(
		ctx,
		&applications,
		query,
		userId,
		userId,
		userId,
		userId,
		userId,
		pageSize,
		offset,
	); err != nil {
		return nil, 0, err
	}

	return applications, total, nil
}

func (m *customApplyFriendModel) CountByReceiverAndStatus(
	ctx context.Context,
	receiverId int64,
	status int64,
) (int64, error) {
	query := fmt.Sprintf(
		"select count(*) from %s where `receiver_id` = ? and `status` = ?",
		m.table,
	)

	var total int64
	if err := m.QueryRowNoCacheCtx(
		ctx,
		&total,
		query,
		receiverId,
		status,
	); err != nil {
		return 0, err
	}

	return total, nil
}

func (m *customApplyFriendModel) MarkReceivedUnreadAsRead(
	ctx context.Context,
	receiverId int64,
) (int64, error) {
	query := fmt.Sprintf(
		"select `apply_friend_id`, `sender_id`, `receiver_id`, `message`, `status`, "+
			"`created_time`, `updated_time` from %s "+
			"where `receiver_id` = ? and `status` = 0",
		m.table,
	)

	var applications []*ApplyFriend
	if err := m.QueryRowsNoCacheCtx(
		ctx,
		&applications,
		query,
		receiverId,
	); err != nil {
		return 0, err
	}
	if len(applications) == 0 {
		return 0, nil
	}

	cacheKeys := make([]string, 0, len(applications)*2)
	for _, application := range applications {
		cacheKeys = append(
			cacheKeys,
			fmt.Sprintf(
				"%s%v",
				cacheApplyFriendApplyFriendIdPrefix,
				application.ApplyFriendId,
			),
			fmt.Sprintf(
				"%s%v:%v",
				cacheApplyFriendSenderIdReceiverIdPrefix,
				application.SenderId,
				application.ReceiverId,
			),
		)
	}

	result, err := m.ExecCtx(
		ctx,
		func(ctx context.Context, conn sqlx.SqlConn) (sql.Result, error) {
			updateQuery := fmt.Sprintf(
				"update %s set `status` = 3 where `receiver_id` = ? and `status` = 0",
				m.table,
			)
			return conn.ExecCtx(ctx, updateQuery, receiverId)
		},
		cacheKeys...,
	)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}
func (m *customApplyFriendModel) FindOneForUpdateTx(
    ctx context.Context,
    applyFriendId int64,
) (*ApplyFriend, error) {
	session:=txctx.GetSession(ctx)
	if session==nil{
		return nil, errors.New(
			"FindOneForUpdateTx must be called inside a transaction",
		)
	}
	//锁住记录，防止并发错误
	query := fmt.Sprintf(
        "select %s from %s where `apply_friend_id` = ? for update",
        applyFriendRows,
        m.table,
    )
	 var application ApplyFriend
    if err := session.QueryRowCtx(
        ctx,
        &application,
        query,
        applyFriendId,
    ); err != nil {
        return nil, err
    }

    return &application, nil
}
func (m *customApplyFriendModel) UpdateTx(
    ctx context.Context,
    data *ApplyFriend,
) error{
	session := txctx.GetSession(ctx)
    if session == nil {
        return errors.New(
            "UpdateTx must be called inside a transaction",
        )
    }
	query:=fmt.Sprintf(
		"update %s set %s where `apply_friend_id`=?",
		m.table,
		applyFriendRowsWithPlaceHolder,
	)
	 _, err := session.ExecCtx(
        ctx,
        query,
        data.SenderId,
        data.ReceiverId,
        data.Message,
        data.Status,
        data.CreatedTime,
        data.UpdatedTime,
        data.ApplyFriendId,
    )
	return err
}
func (m *customApplyFriendModel) ClearCache(
    ctx context.Context,
    data *ApplyFriend,
) error {
    return m.DelCacheCtx(
        ctx,
        fmt.Sprintf(
            "%s%v",
            cacheApplyFriendApplyFriendIdPrefix,
            data.ApplyFriendId,
        ),
        fmt.Sprintf(
            "%s%v:%v",
            cacheApplyFriendSenderIdReceiverIdPrefix,
            data.SenderId,
            data.ReceiverId,
        ),
    )
}