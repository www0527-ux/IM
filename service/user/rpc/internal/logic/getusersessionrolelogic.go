package logic

import (
	"context"
	"errors"

	userSessionModel "infinitechatgo/service/user/model/user_session"
	"infinitechatgo/service/user/rpc/internal/svc"
	"infinitechatgo/service/user/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GetUserSessionRoleLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetUserSessionRoleLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetUserSessionRoleLogic {
	return &GetUserSessionRoleLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetUserSessionRoleLogic) GetUserSessionRole(in *user.UserSessionRoleReq) (*user.UserSessionRoleResp, error) {
	if in == nil || in.UserId <= 0 || in.SessionId <= 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"user_id and session_id are required",
		)
	}

	member, err := l.svcCtx.UserSessionModel.FindOne(
		l.ctx,
		in.UserId,
		in.SessionId,
	)
	if err != nil {
		if errors.Is(err, userSessionModel.ErrNotFound) {
			return nil, status.Error(
				codes.NotFound,
				"group member not found",
			)
		}

		l.Errorf(
			"查询群成员角色失败，userId=%d, sessionId=%d, err=%v",
			in.UserId,
			in.SessionId,
			err,
		)
		return nil, status.Error(
			codes.Internal,
			"query group member role failed",
		)
	}

	if member.Status != 0 {
		return nil, status.Error(
			codes.NotFound,
			"active group member not found",
		)
	}

	return &user.UserSessionRoleResp{
		Role: int32(member.Role),
	}, nil
}
