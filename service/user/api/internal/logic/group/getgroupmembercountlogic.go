// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package group

import (
	"context"
	"errors"

	"infinitechatgo/common/auth"
	"infinitechatgo/common/common"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"
	"infinitechatgo/service/user/api/internal/types/constants"
	chatSessionModel "infinitechatgo/service/user/model/session"
	userSessionModel "infinitechatgo/service/user/model/user_session"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetGroupMemberCountLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetGroupMemberCountLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetGroupMemberCountLogic {
	return &GetGroupMemberCountLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetGroupMemberCountLogic) GetGroupMemberCount(
	req *types.GroupMemberCountRequest,
) (*types.GroupMemberCountResponse, error) {
	identity, ok := auth.IdentityFromContext(l.ctx)
	if !ok {
		return nil, common.NotLoginError
	}
	if req.SessionId <= 0 {
		return nil, common.ParamsError
	}

	groupSession, err := l.svcCtx.SessionModel.FindOne(
		l.ctx,
		req.SessionId,
	)
	if errors.Is(err, chatSessionModel.ErrNotFound) {
		return nil, common.GroupNotFound
	}
	if err != nil {
		return nil, common.WrapError(common.SystemError, err)
	}
	if groupSession.Type != constants.SessionTypeGroup ||
		groupSession.Status != constants.SessionStatusNormal {
		return nil, common.GroupNotFound
	}

	currentMember, err := l.svcCtx.UserSessionModel.FindOne(
		l.ctx,
		identity.UserID,
		req.SessionId,
	)
	if errors.Is(err, userSessionModel.ErrNotFound) {
		return nil, common.NoAuthError
	}
	if err != nil {
		return nil, common.WrapError(common.SystemError, err)
	}
	if currentMember.Status != constants.UserSessionStatusNormal {
		return nil, common.NoAuthError
	}

	memberCount, err :=
		l.svcCtx.UserSessionModel.CountNormalBySessionId(
			l.ctx,
			req.SessionId,
		)
	if err != nil {
		return nil, common.WrapError(common.SystemError, err)
	}

	return &types.GroupMemberCountResponse{
		MemberCount: memberCount,
	}, nil
}
