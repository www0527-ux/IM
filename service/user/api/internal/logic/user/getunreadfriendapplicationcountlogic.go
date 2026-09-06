// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package user

import (
	"context"

	"infinitechatgo/common/auth"
	"infinitechatgo/common/common"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"
	"infinitechatgo/service/user/api/internal/types/constants"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetUnreadFriendApplicationCountLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetUnreadFriendApplicationCountLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetUnreadFriendApplicationCountLogic {
	return &GetUnreadFriendApplicationCountLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetUnreadFriendApplicationCountLogic) GetUnreadFriendApplicationCount() (resp *types.UnreadFriendApplicationCountResponse, err error) {
	identity, ok := auth.IdentityFromContext(l.ctx)
	if !ok {
		return nil, common.NotLoginError
	}

	count, err := l.svcCtx.ApplyFriendModel.CountByReceiverAndStatus(
		l.ctx,
		identity.UserID,
		constants.FriendApplicationStatusUnread,
	)
	if err != nil {
		return nil, common.WrapError(common.SystemError, err)
	}

	return &types.UnreadFriendApplicationCountResponse{
		Count: count,
	}, nil
}
