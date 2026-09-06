package txctx

import (
	"context"
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
