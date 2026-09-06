package session

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"infinitechatgo/common/txctx"

	"github.com/zeromicro/go-zero/core/stores/cache"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var _ SessionModel = (*customSessionModel)(nil)

type (
	// SessionModel is an interface to be customized, add more methods here,
	// and implement the added methods in customSessionModel.
	SessionModel interface {
		sessionModel
		InsertTx(ctx context.Context, data *Session) (sql.Result, error)
		ClearCache(ctx context.Context, data *Session) error
		FindOrCreateSingleTx(
			ctx context.Context,
			data *Session,
		) (*Session, error)
		FindOneForUpdateTx(
			ctx context.Context,
			sessionId int64,
		) (*Session, error)
	}

	customSessionModel struct {
		*defaultSessionModel
	}
)

// NewSessionModel returns a model for the database table.
func NewSessionModel(conn sqlx.SqlConn, c cache.CacheConf, opts ...cache.Option) SessionModel {
	return &customSessionModel{
		defaultSessionModel: newSessionModel(conn, c, opts...),
	}
}

func (m *customSessionModel) InsertTx(
	ctx context.Context,
	data *Session,
) (sql.Result, error) {
	txSession := txctx.GetSession(ctx)
	if txSession == nil {
		return nil, errors.New(
			"InsertTx must be called inside a transaction",
		)
	}

	query := fmt.Sprintf(
		"insert into %s (%s) values (?, ?, ?, ?, ?, ?, ?,?)",
		m.table,
		sessionRowsExpectAutoSet,
	)

	return txSession.ExecCtx(
		ctx,
		query,
		data.SessionId,
		data.Name,
		data.Type,
		data.SingleKey,
		data.Status,
		data.Avatar,
		data.CreatedTime,
		data.UpdatedTime,
	)
}

func (m *customSessionModel) ClearCache(
	ctx context.Context,
	data *Session,
) error {
	keys := []string{
		fmt.Sprintf(
			"%s%v",
			cacheSessionSessionIdPrefix,
			data.SessionId,
		),
	}
	if data.SingleKey.Valid {
		keys = append(
			keys,
			fmt.Sprintf(
				"%s%v",
				cacheSessionSingleKeyPrefix,
				data.SingleKey,
			),
		)
	}
	return m.DelCacheCtx(ctx, keys...)
}

func (m *customSessionModel) FindOneForUpdateTx(
	ctx context.Context,
	sessionId int64,
) (*Session, error) {
	txSession := txctx.GetSession(ctx)
	if txSession == nil {
		return nil, errors.New(
			"FindOneForUpdateTx must be called inside a transaction",
		)
	}

	query := fmt.Sprintf(
		"select %s from %s where `session_id` = ? limit 1 for update",
		sessionRows,
		m.table,
	)

	var result Session
	if err := txSession.QueryRowCtx(
		ctx,
		&result,
		query,
		sessionId,
	); err != nil {
		return nil, err
	}

	return &result, nil
}
func (m *customSessionModel) FindOrCreateSingleTx(
	ctx context.Context,
	data *Session,
) (*Session, error) {
	//传入候选session
	txSession := txctx.GetSession(ctx)
	if txSession == nil {
		return nil, errors.New(
			"FindOrCreateSingleTx must be called inside a transaction",
		)
	}
	if !data.SingleKey.Valid || data.SingleKey.String == "" {
		return nil, errors.New(
			"single key is required",
		)
	}
	//upsert，在已有的情况下就只更新state和time.
	query := fmt.Sprintf(
		"insert into %s (%s) "+
			"values (?, ?, ?, ?, ?, ?, ?, ?) "+
			"on duplicate key update "+
			"`status` = ?, `updated_time` = current_timestamp",
		m.table,
		sessionRowsExpectAutoSet,
	)
	_, err := txSession.ExecCtx(
		ctx,
		query,
		data.SessionId,
		data.Name,
		data.Type,
		data.SingleKey,
		data.Status,
		data.Avatar,
		data.CreatedTime,
		data.UpdatedTime,
		data.Status,
	)
	if err != nil {
		return nil, err
	}
	query = fmt.Sprintf(
		"select %s from %s "+
			"where `single_key` = ? ",
		sessionRows,
		m.table,
	)

	var result Session
	if err := txSession.QueryRowCtx(
		ctx,
		&result,
		query,
		data.SingleKey,
	); err != nil {
		return nil, err
	}

	return &result, nil
}
