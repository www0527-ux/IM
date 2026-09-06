// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package user

import (
	"context"

	"infinitechatgo/common/auth"
	"infinitechatgo/common/common"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type MarkFriendApplicationsReadLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewMarkFriendApplicationsReadLogic(ctx context.Context, svcCtx *svc.ServiceContext) *MarkFriendApplicationsReadLogic {
	return &MarkFriendApplicationsReadLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *MarkFriendApplicationsReadLogic) MarkFriendApplicationsRead() (resp *types.MarkFriendApplicationsReadResponse, err error) {
	identity, ok := auth.IdentityFromContext(l.ctx)
	if !ok {
		return nil, common.NotLoginError
	}

	updated, err := l.svcCtx.ApplyFriendModel.MarkReceivedUnreadAsRead(
		l.ctx,
		identity.UserID,
	)
	if err != nil {
		return nil, common.WrapError(common.SystemError, err)
	}

	return &types.MarkFriendApplicationsReadResponse{
		Updated: updated,
	}, nil
}
