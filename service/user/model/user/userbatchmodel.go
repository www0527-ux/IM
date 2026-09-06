package user
import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type UserBasicView struct {
	UserId   int64          `db:"user_id"`
	Nickname string         `db:"nickname"`
	Avatar   sql.NullString `db:"avatar"`
}
func (m *customUserModel) FindBasicByIds(
	ctx context.Context,
	userIds []int64,
) ([]*UserBasicView, error) {
	if len(userIds) == 0 {
		return []*UserBasicView{}, nil
	}
	placeholders := strings.TrimSuffix(
		strings.Repeat("?,", len(userIds)),
		",",
	)
	query := fmt.Sprintf(
		"select `user_id`, `nickname`, `avatar` "+
			"from %s "+
			"where `user_id` in (%s) "+
			"and `state` = 0 and `is_delete` = 0",
		m.table,
		placeholders,
	)
	args:=make([]any,0,len(userIds))
	for _, userId := range userIds {
		args = append(args, userId)
	}

	//为数据库返回的每一行创建一个对象并返回地址
	var users []*UserBasicView
	if err:=m.QueryRowsNoCacheCtx(
		ctx,
		&users,
		query,
		args...,
	);
	err != nil {
		return nil, err
	}
	return users, nil
}
