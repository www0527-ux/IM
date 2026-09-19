package syncer

import (
	"context"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type MySQLSessionAccessStore struct {
	conn sqlx.SqlConn
}

func NewMySQLSessionAccessStore(conn sqlx.SqlConn) *MySQLSessionAccessStore {
	return &MySQLSessionAccessStore{conn: conn}
}

func (s *MySQLSessionAccessStore) IsActiveMember(
	ctx context.Context,
	userID int64,
	sessionID int64,
) (bool, error) {
	const query = `
SELECT COUNT(1)
FROM user_session
WHERE user_id = ? AND session_id = ? AND status = 0`
	var count int64
	if err := s.conn.QueryRowCtx(ctx, &count, query, userID, sessionID); err != nil {
		return false, err
	}
	return count > 0, nil
}
