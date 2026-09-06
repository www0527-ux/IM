package usersession

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"infinitechatgo/common/txctx"
	"strings"
	"time"

	"github.com/zeromicro/go-zero/core/stores/cache"
	"github.com/zeromicro/go-zero/core/stores/sqlc"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

const cacheUserSessionPrefix = "cache:userSession:userId:sessionId:"

var ErrNotFound = sqlx.ErrNotFound

type UserSession struct {
	UserId      int64     `db:"user_id"`
	SessionId   int64     `db:"session_id"`
	Role        int64     `db:"role"`
	Status      int64     `db:"status"`
	CreatedTime time.Time `db:"created_time"`
	UpdatedTime time.Time `db:"updated_time"`
}

type GroupMemberView struct {
	UserId     int64          `db:"user_id"`
	Nickname   string         `db:"nickname"`
	Avatar     sql.NullString `db:"avatar"`
	Role       int64          `db:"role"`
	JoinedTime time.Time      `db:"joined_time"`
}

type JoinedGroupView struct {
	SessionId  int64          `db:"session_id"`
	Name       string         `db:"name"`
	Avatar     sql.NullString `db:"avatar"`
	Role       int64          `db:"role"`
	JoinedTime time.Time      `db:"joined_time"`
}

type UserSessionModel interface {
	Insert(ctx context.Context, data *UserSession) (sql.Result, error)
	InsertTx(ctx context.Context, data *UserSession) (sql.Result, error)
	FindOne(ctx context.Context, userId, sessionId int64) (*UserSession, error)
	FindOneForUpdateTx(
		ctx context.Context,
		userId int64,
		sessionId int64,
	) (*UserSession, error)
	FindByUserIdsForUpdateTx(
		ctx context.Context,
		sessionId int64,
		userIds []int64,
	) ([]*UserSession, error)
	FindSingleSessionId(ctx context.Context, userId, friendId int64) (int64, error)
	FindMemberIdsBySessionId(
		ctx context.Context,
		sessionId int64,
	) ([]int64, error)
	Update(ctx context.Context, data *UserSession) error
	Delete(ctx context.Context, userId, sessionId int64) error
	ClearCache(ctx context.Context, data *UserSession) error
	UpsertTx(
		ctx context.Context,
		data *UserSession,
	) error
	InviteMemberTx(
		ctx context.Context,
		data *UserSession,
	) (bool, error)
	UpdateStatusTx(
		ctx context.Context,
		userId int64,
		sessionId int64,
		status int64,
	) error
	CountNormalBySessionId(
		ctx context.Context,
		sessionId int64,
	) (int64, error)
	FindPageBySessionId(
		ctx context.Context,
		sessionId int64,
		pageNum int,
		pageSize int,
	) ([]*GroupMemberView, error)
	CountNormalGroupsByUserId(
		ctx context.Context,
		userId int64,
	) (int64, error)
	FindGroupPageByUserId(
		ctx context.Context,
		userId int64,
		pageNum int,
		pageSize int,
	) ([]*JoinedGroupView, error)
}

func (m *defaultUserSessionModel) FindMemberIdsBySessionId(
	ctx context.Context,
	sessionId int64,
) ([]int64, error) {
	query := fmt.Sprintf(
		"select `user_id` from %s "+
			"where `session_id` = ? and `status` = 0 "+
			"order by `user_id` asc",
		m.table,
	)

	var members []struct {
		UserId int64 `db:"user_id"`
	}
	if err := m.QueryRowsNoCacheCtx(
		ctx,
		&members,
		query,
		sessionId,
	); err != nil {
		return nil, err
	}

	userIds := make([]int64, 0, len(members))
	for _, member := range members {
		userIds = append(userIds, member.UserId)
	}

	return userIds, nil
}

func (m *defaultUserSessionModel) FindSingleSessionId(
	ctx context.Context,
	userId int64,
	friendId int64,
) (int64, error) {
	query := "select us1.`session_id` from `user_session` us1 " +
		"inner join `user_session` us2 on us2.`session_id` = us1.`session_id` " +
		"inner join `session` s on s.`session_id` = us1.`session_id` " +
		"where us1.`user_id` = ? and us2.`user_id` = ? " +
		"and us1.`status` = 0 and us2.`status` = 0 " +
		"and s.`type` = 0 and s.`status` = 0 " +
		"order by s.`created_time` desc limit 1"

	var sessionId int64
	if err := m.QueryRowNoCacheCtx(
		ctx,
		&sessionId,
		query,
		userId,
		friendId,
	); err != nil {
		if err == sqlc.ErrNotFound {
			return 0, ErrNotFound
		}
		return 0, err
	}

	return sessionId, nil
}

type defaultUserSessionModel struct {
	sqlc.CachedConn
	table string //表名
}

var _ UserSessionModel = (*defaultUserSessionModel)(nil)

func NewUserSessionModel(
	conn sqlx.SqlConn,
	c cache.CacheConf,
	opts ...cache.Option,
) UserSessionModel {
	return &defaultUserSessionModel{
		CachedConn: sqlc.NewConn(conn, c, opts...),
		table:      "`user_session`",
	}
}
func userSessionCacheKey(userId, sessionId int64) string {
	return fmt.Sprintf(
		"%s%d:%d",
		cacheUserSessionPrefix,
		userId,
		sessionId,
	)
}
func (m *defaultUserSessionModel) Insert(
	ctx context.Context,
	data *UserSession,
) (sql.Result, error) {
	key := userSessionCacheKey(data.UserId, data.SessionId)
	return m.ExecCtx(ctx, func(ctx context.Context, conn sqlx.SqlConn) (sql.Result, error) {
		query := fmt.Sprintf(
			"insert into %s (`user_id`, `session_id`, `role`, `status`) values (?, ?, ?, ?)",
			m.table,
		)

		return conn.ExecCtx(
			ctx,
			query,
			data.UserId,
			data.SessionId,
			data.Role,
			data.Status,
		)
	}, key)
}

func (m *defaultUserSessionModel) InsertTx(
	ctx context.Context,
	data *UserSession,
) (sql.Result, error) {
	session := txctx.GetSession(ctx)
	if session == nil {
		return nil, errors.New(
			"InsertTx must be called inside a transaction",
		)
	}

	query := fmt.Sprintf(
		"insert into %s (`user_id`, `session_id`, `role`, `status`) "+
			"values (?, ?, ?, ?)",
		m.table,
	)

	return session.ExecCtx(
		ctx,
		query,
		data.UserId,
		data.SessionId,
		data.Role,
		data.Status,
	)
}

func (m *defaultUserSessionModel) ClearCache(
	ctx context.Context,
	data *UserSession,
) error {
	return m.DelCacheCtx(
		ctx,
		userSessionCacheKey(data.UserId, data.SessionId),
	)
}
func (m *defaultUserSessionModel) FindOne(
	ctx context.Context,
	userId, sessionId int64,
) (*UserSession, error) {
	key := userSessionCacheKey(userId, sessionId)

	var resp UserSession

	err := m.QueryRowCtx(
		ctx,
		&resp,
		key,
		func(ctx context.Context, conn sqlx.SqlConn, v any) error {
			query := fmt.Sprintf(
				"select `user_id`, `session_id`, `role`, `status`, `created_time`, `updated_time` "+
					"from %s where `user_id` = ? and `session_id` = ? limit 1",
				m.table,
			)

			return conn.QueryRowCtx(
				ctx,
				v,
				query,
				userId,
				sessionId,
			)
		},
	)

	switch err {
	case nil:
		return &resp, nil
	case sqlc.ErrNotFound:
		return nil, ErrNotFound
	default:
		return nil, err
	}
}

func (m *defaultUserSessionModel) FindOneForUpdateTx(
	ctx context.Context,
	userId int64,
	sessionId int64,
) (*UserSession, error) {
	txSession := txctx.GetSession(ctx)
	if txSession == nil {
		return nil, errors.New(
			"FindOneForUpdateTx must be called inside a transaction",
		)
	}

	query := fmt.Sprintf(
		"select `user_id`, `session_id`, `role`, `status`, `created_time`, `updated_time` "+
			"from %s where `user_id` = ? and `session_id` = ? limit 1 for update",
		m.table,
	)
	var result UserSession
	if err := txSession.QueryRowCtx(
		ctx,
		&result,
		query,
		userId,
		sessionId,
	); err != nil {
		return nil, err
	}

	return &result, nil
}

func (m *defaultUserSessionModel) FindByUserIdsForUpdateTx(
	ctx context.Context,
	sessionId int64,
	userIds []int64,
) ([]*UserSession, error) {
	txSession := txctx.GetSession(ctx)
	if txSession == nil {
		return nil, errors.New(
			"FindByUserIdsForUpdateTx must be called inside a transaction",
		)
	}
	if len(userIds) == 0 {
		return []*UserSession{}, nil
	}

	placeholders := strings.TrimSuffix(
		strings.Repeat("?,", len(userIds)),
		",",
	)
	query := fmt.Sprintf(
		"select `user_id`, `session_id`, `role`, `status`, `created_time`, `updated_time` "+
			"from %s where `session_id` = ? and `user_id` in (%s) "+
			"order by `user_id` for update",
		m.table,
		placeholders,
	)

	args := make([]any, 0, len(userIds)+1)
	args = append(args, sessionId)
	for _, userId := range userIds {
		args = append(args, userId)
	}

	var result []*UserSession
	if err := txSession.QueryRowsCtx(
		ctx,
		&result,
		query,
		args...,
	); err != nil {
		return nil, err
	}

	return result, nil
}
func (m *defaultUserSessionModel) Update(
	ctx context.Context,
	data *UserSession,
) error {
	key := userSessionCacheKey(data.UserId, data.SessionId)

	_, err := m.ExecCtx(ctx, func(
		ctx context.Context,
		conn sqlx.SqlConn,
	) (sql.Result, error) {
		query := fmt.Sprintf(
			"update %s set `role` = ?, `status` = ? "+
				"where `user_id` = ? and `session_id` = ?",
			m.table,
		)

		return conn.ExecCtx(
			ctx,
			query,
			data.Role,
			data.Status,
			data.UserId,
			data.SessionId,
		)
	}, key)

	return err
}
func (m *defaultUserSessionModel) Delete(
	ctx context.Context,
	userId, sessionId int64,
) error {
	key := userSessionCacheKey(userId, sessionId)

	_, err := m.ExecCtx(ctx, func(
		ctx context.Context,
		conn sqlx.SqlConn,
	) (sql.Result, error) {
		query := fmt.Sprintf(
			"delete from %s where `user_id` = ? and `session_id` = ?",
			m.table,
		)

		return conn.ExecCtx(
			ctx,
			query,
			userId,
			sessionId,
		)
	}, key)

	return err
}
func (m *defaultUserSessionModel) UpsertTx(
	ctx context.Context,
	data *UserSession,
) error {
	session := txctx.GetSession(ctx)
	if session == nil {
		return errors.New(
			"UpsertTx must be called inside a transaction",
		)
	}

	query := fmt.Sprintf(
		"insert into %s "+
			"(`user_id`, `session_id`, `role`, `status`) "+
			"values (?, ?, ?, ?) "+
			"on duplicate key update "+
			"`role` = ?, `status` = ?, "+
			"`updated_time` = current_timestamp",
		m.table,
	)

	_, err := session.ExecCtx(
		ctx,
		query,
		data.UserId,
		data.SessionId,
		data.Role,
		data.Status,
		data.Role,
		data.Status,
	)

	return err
}

func (m *defaultUserSessionModel) InviteMemberTx(
	ctx context.Context,
	data *UserSession,
) (bool, error) {
	txSession := txctx.GetSession(ctx)
	if txSession == nil {
		return false, errors.New(
			"InviteMemberTx must be called inside a transaction",
		)
	}

	query := fmt.Sprintf(
		"insert into %s "+
			"(`user_id`, `session_id`, `role`, `status`) "+
			"values (?, ?, ?, ?) "+
			"on duplicate key update "+
			"`role` = if(`status` <> values(`status`), values(`role`), `role`), "+
			"`updated_time` = if(`status` <> values(`status`), current_timestamp, `updated_time`), "+
			"`status` = values(`status`)",
		m.table,
	)

	result, err := txSession.ExecCtx(
		ctx,
		query,
		data.UserId,
		data.SessionId,
		data.Role,
		data.Status,
	)
	if err != nil {
		return false, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}

	return rowsAffected > 0, nil
}

func (m *defaultUserSessionModel) UpdateStatusTx(
	ctx context.Context,
	userId int64,
	sessionId int64,
	status int64,
) error {
	txSession := txctx.GetSession(ctx)
	if txSession == nil {
		return errors.New(
			"UpdateStatusTx must be called inside a transaction",
		)
	}

	query := fmt.Sprintf(
		"update %s set `status` = ?, `updated_time` = current_timestamp "+
			"where `user_id` = ? and `session_id` = ?",
		m.table,
	)
	_, err := txSession.ExecCtx(
		ctx,
		query,
		status,
		userId,
		sessionId,
	)

	return err
}

func (m *defaultUserSessionModel) CountNormalBySessionId(
	ctx context.Context,
	sessionId int64,
) (int64, error) {
	query := fmt.Sprintf(
		"select count(*) from %s us "+
			"inner join `user` u on u.`user_id` = us.`user_id` "+
			"where us.`session_id` = ? and us.`status` = 0 "+
			"and u.`state` = 0 and u.`is_delete` = 0",
		m.table,
	)

	var total int64
	if err := m.QueryRowNoCacheCtx(
		ctx,
		&total,
		query,
		sessionId,
	); err != nil {
		return 0, err
	}

	return total, nil
}

func (m *defaultUserSessionModel) FindPageBySessionId(
	ctx context.Context,
	sessionId int64,
	pageNum int,
	pageSize int,
) ([]*GroupMemberView, error) {
	offset := (pageNum - 1) * pageSize
	query := fmt.Sprintf(
		"select us.`user_id`, u.`nickname`, u.`avatar`, "+
			"us.`role`, us.`created_time` as `joined_time` "+
			"from %s us "+
			"inner join `user` u on u.`user_id` = us.`user_id` "+
			"where us.`session_id` = ? and us.`status` = 0 "+
			"and u.`state` = 0 and u.`is_delete` = 0 "+
			"order by us.`role`, us.`created_time`, us.`user_id` "+
			"limit ? offset ?",
		m.table,
	)

	var members []*GroupMemberView
	if err := m.QueryRowsNoCacheCtx(
		ctx,
		&members,
		query,
		sessionId,
		pageSize,
		offset,
	); err != nil {
		return nil, err
	}

	return members, nil
}

func (m *defaultUserSessionModel) CountNormalGroupsByUserId(
	ctx context.Context,
	userId int64,
) (int64, error) {
	query := fmt.Sprintf(
		"select count(*) from %s us "+
			"inner join `session` s on s.`session_id` = us.`session_id` "+
			"where us.`user_id` = ? and us.`status` = 0 "+
			"and s.`type` = 1 and s.`status` = 0",
		m.table,
	)

	var total int64
	if err := m.QueryRowNoCacheCtx(
		ctx,
		&total,
		query,
		userId,
	); err != nil {
		return 0, err
	}

	return total, nil
}

func (m *defaultUserSessionModel) FindGroupPageByUserId(
	ctx context.Context,
	userId int64,
	pageNum int,
	pageSize int,
) ([]*JoinedGroupView, error) {
	offset := (pageNum - 1) * pageSize
	query := fmt.Sprintf(
		"select s.`session_id`, s.`name`, s.`avatar`, "+
			"us.`role`, us.`created_time` as `joined_time` "+
			"from %s us "+
			"inner join `session` s on s.`session_id` = us.`session_id` "+
			"where us.`user_id` = ? and us.`status` = 0 "+
			"and s.`type` = 1 and s.`status` = 0 "+
			"order by us.`updated_time` desc, s.`session_id` desc "+
			"limit ? offset ?",
		m.table,
	)

	var groups []*JoinedGroupView
	if err := m.QueryRowsNoCacheCtx(
		ctx,
		&groups,
		query,
		userId,
		pageSize,
		offset,
	); err != nil {
		return nil, err
	}

	return groups, nil
}
