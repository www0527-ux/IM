package txctx

import (
	"context"
	"errors"
	"fmt"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

//used for get session from context:
//txCtx := context.WithValue(ctx, txKey{}, session)

// 不使用字符串是因为会发生包冲突
type txKey struct{}

func GetSession(ctx context.Context) sqlx.Session {
	session, _ := ctx.Value(txKey{}).(sqlx.Session)
	return session
}

// ErrMissingTransaction means a transaction-scoped operation ran without a
// session in its context.
var ErrMissingTransaction = errors.New("transaction context is required")

// SessionConn returns a connection bound to the transaction session carried by
// ctx. Transaction-aware models must use it so every statement joins the
// caller's transaction instead of opening an independent one. A Seq or message
// written outside that transaction would break the all-or-nothing invariant,
// so a missing session is an error and never a silent fallback to the pool.
func SessionConn(ctx context.Context) (sqlx.SqlConn, error) {
	session := GetSession(ctx)
	if session == nil {
		return nil, ErrMissingTransaction
	}
	return sqlx.NewSqlConnFromSession(session), nil
}

// fn的返回值决定是否要回滚
func WithTransaction(
	ctx context.Context,
	conn sqlx.SqlConn,
	fn func(ctx context.Context) error,
) error {
	if GetSession(ctx) != nil {
		return fn(ctx)
	}
	return conn.TransactCtx(
		ctx,
		func(ctx context.Context, session sqlx.Session) error {
			txCtx := context.WithValue(ctx, txKey{}, session)
			return safeRun(func() error {
				return fn(txCtx)
			})
		},
	)
}
func safeRun(fn func() error) (err error) {
	defer func() {
		if value := recover(); value != nil {
			if recoveredErr, ok := value.(error); ok {
				err = recoveredErr
			} else {
				err = fmt.Errorf("panic:%v", value)
			}
		}
	}()
	return fn()
}
