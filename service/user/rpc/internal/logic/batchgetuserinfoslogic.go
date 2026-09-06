package logic

import (
	"context"

	"infinitechatgo/service/user/rpc/internal/svc"
	"infinitechatgo/service/user/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxBatchUserCount = 100

type BatchGetUserInfosLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewBatchGetUserInfosLogic(ctx context.Context, svcCtx *svc.ServiceContext) *BatchGetUserInfosLogic {
	return &BatchGetUserInfosLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *BatchGetUserInfosLogic) BatchGetUserInfos(in *user.BatchGetUserInfosReq) (*user.BatchGetUserInfosResp, error) {
	if in == nil || len(in.UserIds) == 0 {
		return &user.BatchGetUserInfosResp{
			Users: []*user.BatchUserInfoItem{},
		}, nil
	}
	if len(in.UserIds) > maxBatchUserCount {
		return nil, status.Error(
			codes.InvalidArgument,
			"user_ids exceeds batch limit",
		)
	}
	seen := make(map[int64]struct{}, len(in.UserIds))
	userIds := make([]int64, 0, len(in.UserIds))
	for _, userId := range in.UserIds {
		//使用map去重
		if _, exists := seen[userId]; exists {
			continue
		}
		//一个没有任何字段的结构体类型
		seen[userId] = struct{}{}
		userIds = append(userIds, userId)
	}
	userViews, err := l.svcCtx.UserModel.FindBasicByIds(
		l.ctx,
		userIds,
	)
	if err != nil {
		l.Errorf("批量查询用户信息失败: %v", err)

		return nil, status.Error(
			codes.Internal,
			"query user infos failed",
		)
	}
	userById := make(
		map[int64]*user.BatchUserInfoItem,
		len(userViews),
	)
	for _, view := range userViews {
		if view == nil {
			continue
		}
		//结构体转化
		userById[view.UserId] = &user.BatchUserInfoItem{
			UserId:   view.UserId,
			Nickname: view.Nickname,
			Avatar:   view.Avatar.String,
		}
	}
	//恢复请求顺序
	users := make(
		[]*user.BatchUserInfoItem,
		0,
		len(userById),
	)
	for _, userId := range userIds {
		if item, exists := userById[userId]; exists {
			users = append(users, item)
		}
	}
	return &user.BatchGetUserInfosResp{
		Users: users,
	}, nil
}
