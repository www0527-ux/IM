package logic

import (
	"context"

	"infinitechatgo/service/user/rpc/internal/svc"
	"infinitechatgo/service/user/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GetGroupMemberIdsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetGroupMemberIdsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetGroupMemberIdsLogic {
	return &GetGroupMemberIdsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetGroupMemberIdsLogic) GetGroupMemberIds(in *user.SessionIdReq) (*user.GroupMemberIdsResp, error) {
	if in == nil || in.SessionId <= 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"session_id is required",
		)
	}

	userIds, err := l.svcCtx.UserSessionModel.FindMemberIdsBySessionId(
		l.ctx,
		in.SessionId,
	)
	if err != nil {
		l.Errorf(
			"查询群成员ID失败，sessionId=%d, err=%v",
			in.SessionId,
			err,
		)
		return nil, status.Error(
			codes.Internal,
			"query group member ids failed",
		)
	}

	return &user.GroupMemberIdsResp{
		UserIds: userIds,
	}, nil
}
