package friend

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

var _ FriendModel = (*customFriendModel)(nil)

type (
	// FriendModel is an interface to be customized, add more methods here,
	// and implement the added methods in customFriendModel.
	FriendModel interface {
		friendModel
		CountByUserId(ctx context.Context, userId int64, keyword string) (int64, error)
		FindPageByUserId(
			ctx context.Context,
			userId int64,
			keyword string,
			pageNum int,
			pageSize int,
		) ([]*FriendView, error)
		FindNormalFriendIds(
			ctx context.Context,
			userId int64,
			friendIds []int64,
		) ([]int64, error)
		FindNormalFriendIdsForUpdateTx(
			ctx context.Context,
			userId int64,
			friendIds []int64,
		) ([]int64, error)
		UpsertTx(
			ctx context.Context,
			data *Friend,
		) (*Friend, error)
		UpdatePairStatusTx(
			ctx context.Context,
			firstUserId int64,
			secondUserId int64,
			status int64,
		) ([]*Friend, error)
		ClearCache(
			ctx context.Context,
			data *Friend,
		) error
	}

	FriendView struct {
		UserId      int64          `db:"user_id"`
		Nickname    string         `db:"nickname"`
		Avatar      sql.NullString `db:"avatar"`
		Description sql.NullString `db:"description"`
		Gender      int64          `db:"gender"`
		Status      int64          `db:"status"`
		SessionId   int64          `db:"session_id"`
	}

	customFriendModel struct {
		*defaultFriendModel
	}
)

// NewFriendModel returns a model for the database table.
func NewFriendModel(conn sqlx.SqlConn, c cache.CacheConf, opts ...cache.Option) FriendModel {
	return &customFriendModel{
		defaultFriendModel: newFriendModel(conn, c, opts...),
	}
}

func (m *customFriendModel) UpsertTx(
	ctx context.Context,
	data *Friend,
) (*Friend, error) {
	session := txctx.GetSession(ctx)
	if session == nil {
		return nil, errors.New(
			"UpsertTx must be called inside a transaction",
		)
	}

	query := fmt.Sprintf(
		"insert into %s (`user_id`, `friend_id`, `status`) "+
			"values (?, ?, ?) "+
			"on duplicate key update "+
			"`status` = ?, `updated_time` = current_timestamp",
		m.table,
	)

	_, err := session.ExecCtx(
		ctx,
		query,
		data.UserId,
		data.FriendId,
		data.Status,
		data.Status,
	)
	if err != nil {
		return nil, err
	}

	query = fmt.Sprintf(
		"select %s from %s "+
			"where `user_id` = ? and `friend_id` = ? limit 1",
		friendRows,
		m.table,
	)

	var relation Friend
	if err := session.QueryRowCtx(
		ctx,
		&relation,
		query,
		data.UserId,
		data.FriendId,
	); err != nil {
		return nil, err
	}

	return &relation, nil
}

// UpdatePairStatusTx updates both directional friend records in one transaction.
// Existing rows are locked in a stable order to reduce the chance of deadlocks.
func (m *customFriendModel) UpdatePairStatusTx(
	ctx context.Context,
	firstUserId int64,
	secondUserId int64,
	status int64,
) ([]*Friend, error) {
	session := txctx.GetSession(ctx)
	if session == nil {
		return nil, errors.New(
			"UpdatePairStatusTx must be called inside a transaction",
		)
	}
	if firstUserId == secondUserId {
		return nil, errors.New("friend user ids must be different")
	}
	if firstUserId > secondUserId {
		firstUserId, secondUserId = secondUserId, firstUserId
	}

	query := fmt.Sprintf(
		"select %s from %s "+
			"where (`user_id` = ? and `friend_id` = ?) "+
			"or (`user_id` = ? and `friend_id` = ?) "+
			"order by `user_id`, `friend_id` for update",
		friendRows,
		m.table,
	)

	var relations []*Friend
	if err := session.QueryRowsCtx(
		ctx,
		&relations,
		query,
		firstUserId,
		secondUserId,
		secondUserId,
		firstUserId,
	); err != nil {
		return nil, err
	}
	if len(relations) == 0 {
		return nil, ErrNotFound
	}

	query = fmt.Sprintf(
		"update %s set `status` = ?, `updated_time` = current_timestamp "+
			"where (`user_id` = ? and `friend_id` = ?) "+
			"or (`user_id` = ? and `friend_id` = ?)",
		m.table,
	)
	if _, err := session.ExecCtx(
		ctx,
		query,
		status,
		firstUserId,
		secondUserId,
		secondUserId,
		firstUserId,
	); err != nil {
		return nil, err
	}

	for _, relation := range relations {
		relation.Status = status
	}

	return relations, nil
}

func (m *customFriendModel) ClearCache(
	ctx context.Context,
	data *Friend,
) error {
	keys := []string{
		fmt.Sprintf(
			"%s%v:%v",
			cacheFriendUserIdFriendIdPrefix,
			data.UserId,
			data.FriendId,
		),
	}

	if data.Id != 0 {
		keys = append(
			keys,
			fmt.Sprintf(
				"%s%v",
				cacheFriendIdPrefix,
				data.Id,
			),
		)
	}

	return m.DelCacheCtx(ctx, keys...)
}

func (m *customFriendModel) CountByUserId(
	ctx context.Context,
	userId int64,
	keyword string,
) (int64, error) {
	query := fmt.Sprintf(
		"select count(*) from %s f "+
			"inner join `user` u on u.`user_id` = f.`friend_id` "+
			"where f.`user_id` = ? and f.`status` != 2 and u.`is_delete` = 0",
		m.table,
	)
	args := []any{userId}

	keyword = strings.TrimSpace(keyword)
	if keyword != "" {
		query += " and (u.`nickname` like ? or u.`email` = ? or u.`phone` = ?)"
		args = append(args, "%"+keyword+"%", keyword, keyword)
	}

	var total int64
	if err := m.QueryRowNoCacheCtx(ctx, &total, query, args...); err != nil {
		return 0, err
	}

	return total, nil
}

func (m *customFriendModel) FindPageByUserId(
	ctx context.Context,
	userId int64,
	keyword string,
	pageNum int,
	pageSize int,
) ([]*FriendView, error) {
	offset := (pageNum - 1) * pageSize
	query := fmt.Sprintf(
		"select f.`friend_id` as `user_id`, u.`nickname`, u.`avatar`, "+
			"u.`description`, u.`gender`, f.`status`, "+
			"coalesce((select us1.`session_id` from `user_session` us1 "+
			"inner join `user_session` us2 "+
			"on us2.`session_id` = us1.`session_id` "+
			"and us2.`user_id` = f.`friend_id` and us2.`status` = 0 "+
			"inner join `session` s on s.`session_id` = us1.`session_id` "+
			"and s.`type` = 0 and s.`status` = 0 "+
			"where us1.`user_id` = f.`user_id` and us1.`status` = 0 "+
			"order by s.`created_time` desc limit 1), 0) as `session_id` "+
			"from %s f "+
			"inner join `user` u on u.`user_id` = f.`friend_id` "+
			"where f.`user_id` = ? and f.`status` != 2 and u.`is_delete` = 0",
		m.table,
	)
	args := []any{userId}

	keyword = strings.TrimSpace(keyword)
	if keyword != "" {
		query += " and (u.`nickname` like ? or u.`email` = ? or u.`phone` = ?)"
		args = append(args, "%"+keyword+"%", keyword, keyword)
	}

	query += " order by f.`updated_time` desc, f.`id` desc limit ? offset ?"
	args = append(args, pageSize, offset)

	var friends []*FriendView
	if err := m.QueryRowsNoCacheCtx(ctx, &friends, query, args...); err != nil {
		return nil, err
	}

	return friends, nil
}

func (m *customFriendModel) FindNormalFriendIds(
	ctx context.Context,
	userId int64,
	friendIds []int64,
) ([]int64, error) {
	if len(friendIds) == 0 {
		return []int64{}, nil
	}

	placeholders := strings.TrimSuffix(
		strings.Repeat("?,", len(friendIds)),
		",",
	)
	query := fmt.Sprintf(
		"select f.`friend_id` from %s f "+
			"inner join `user` u on u.`user_id` = f.`friend_id` "+
			"where f.`user_id` = ? and f.`friend_id` in (%s) "+
			"and f.`status` = 0 and u.`state` = 0 and u.`is_delete` = 0",
		m.table,
		placeholders,
	)

	args := make([]any, 0, len(friendIds)+1)
	args = append(args, userId)
	for _, friendId := range friendIds {
		args = append(args, friendId)
	}

	var validIds []int64
	if err := m.QueryRowsNoCacheCtx(
		ctx,
		&validIds,
		query,
		args...,
	); err != nil {
		return nil, err
	}

	return validIds, nil
}

func (m *customFriendModel) FindNormalFriendIdsForUpdateTx(
	ctx context.Context,
	userId int64,
	friendIds []int64,
) ([]int64, error) {
	txSession := txctx.GetSession(ctx)
	if txSession == nil {
		return nil, errors.New(
			"FindNormalFriendIdsForUpdateTx must be called inside a transaction",
		)
	}
	if len(friendIds) == 0 {
		return []int64{}, nil
	}

	placeholders := strings.TrimSuffix(
		strings.Repeat("?,", len(friendIds)),
		",",
	)
	query := fmt.Sprintf(
		"select f.`friend_id` from %s f "+
			"inner join `user` u on u.`user_id` = f.`friend_id` "+
			"where f.`user_id` = ? and f.`friend_id` in (%s) "+
			"and f.`status` = 0 and u.`state` = 0 and u.`is_delete` = 0 "+
			"order by f.`friend_id` for update",
		m.table,
		placeholders,
	)

	args := make([]any, 0, len(friendIds)+1)
	args = append(args, userId)
	for _, friendId := range friendIds {
		args = append(args, friendId)
	}

	var validIds []int64
	if err := txSession.QueryRowsCtx(
		ctx,
		&validIds,
		query,
		args...,
	); err != nil {
		return nil, err
	}

	return validIds, nil
}
