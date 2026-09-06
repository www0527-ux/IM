// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package user

import (
	"context"
	"errors"
	"time"

	"infinitechatgo/common/auth"
	"infinitechatgo/common/common"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"
	"infinitechatgo/service/user/api/internal/types/constants"
	applyFriendModel "infinitechatgo/service/user/model/apply_friend"

	"github.com/zeromicro/go-zero/core/logx"
)

type RejectFriendApplicationLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewRejectFriendApplicationLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RejectFriendApplicationLogic {
	return &RejectFriendApplicationLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *RejectFriendApplicationLogic) RejectFriendApplication(req *types.FriendApplicationOperationRequest) (resp *types.OperationResponse, err error) {
	identity, ok := auth.IdentityFromContext(l.ctx)
	if !ok {
		return nil, common.NotLoginError
	}

	application, err := l.svcCtx.ApplyFriendModel.FindOne(
		l.ctx,
		req.ApplyFriendId,
	)
	if errors.Is(err, applyFriendModel.ErrNotFound) {
		return nil, common.FriendApplicationNotFound
	}
	if err != nil {
		return nil, common.WrapError(common.SystemError, err)
	}
	if application == nil {
		return nil, common.FriendApplicationNotFound
	}
	if application.ReceiverId != identity.UserID {
		return nil, common.NoAuthError
	}
	if application.Status == constants.FriendApplicationStatusRejected {
		return &types.OperationResponse{Success: true}, nil
	}
	if !isPendingFriendApplication(application.Status) {
		return nil, common.FriendApplicationStateError
	}

	application.Status = constants.FriendApplicationStatusRejected
	application.UpdatedTime = time.Now()
	if err := l.svcCtx.ApplyFriendModel.Update(l.ctx, application); err != nil {
		return nil, common.WrapError(common.SystemError, err)
	}

	return &types.OperationResponse{Success: true}, nil
}
