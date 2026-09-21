// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package user

import (
	"context"
	"errors"
	"strings"
	"time"

	"infinitechatgo/common/auth"
	"infinitechatgo/common/common"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"
	"infinitechatgo/service/user/api/internal/types/constants"
	applyFriendModel "infinitechatgo/service/user/model/apply_friend"
	friendModel "infinitechatgo/service/user/model/friend"
	userModel "infinitechatgo/service/user/model/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type SendFriendApplicationLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewSendFriendApplicationLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SendFriendApplicationLogic {
	return &SendFriendApplicationLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *SendFriendApplicationLogic) SendFriendApplication(req *types.SendFriendApplicationRequest) (resp *types.SendFriendApplicationResponse, err error) {
	identity, ok := auth.IdentityFromContext(l.ctx)
	if !ok {
		return nil, common.NotLoginError
	}

	//不能添加自己
	if identity.UserID == req.ReceiverId {
		return nil, common.CannotAddSelfError
	}

	receiver, err := l.svcCtx.UserModel.FindOne(l.ctx, req.ReceiverId)
	if errors.Is(err, userModel.ErrNotFound) {
		return nil, common.UserNotExists
	}
	if err != nil {
		return nil, common.WrapError(common.SystemError, err)
	}
	if receiver == nil || receiver.IsDelete == 1 || receiver.State == 2 {
		return nil, common.UserNotExists
	}
	//判断是否已经是好友了
	relation, err := l.svcCtx.FriendModel.FindOneByUserIdFriendId(
		l.ctx,
		identity.UserID,
		req.ReceiverId,
	)
	if err == nil && relation != nil && relation.Status != constants.FriendStatusDeleted {
		return nil, common.AlreadyFriendsError
	}
	if err != nil && !errors.Is(err, friendModel.ErrNotFound) {
		return nil, common.WrapError(common.SystemError, err)
	}
	//对方已经发来申请则提示先处理
	reverseApplication, reverseErr := l.svcCtx.ApplyFriendModel.FindOneBySenderIdReceiverId(
		l.ctx,
		req.ReceiverId,
		identity.UserID,
	)
	if reverseErr == nil && reverseApplication != nil &&
		isPendingFriendApplication(reverseApplication.Status) {
		return nil, common.FriendApplicationReceivedError
	}
	if reverseErr != nil && !errors.Is(reverseErr, applyFriendModel.ErrNotFound) {
		return nil, common.WrapError(common.SystemError, reverseErr)
	}

	message := strings.TrimSpace(req.Message)
	if message == "" {
		message = "请求添加好友"
	}
	now := time.Now()

	application, err := l.svcCtx.ApplyFriendModel.FindOneBySenderIdReceiverId(
		l.ctx,
		identity.UserID,
		req.ReceiverId,
	)
	if err == nil && application != nil {
		if isPendingFriendApplication(application.Status) {
			//已读或者未读
			return nil, common.FriendApplicationPendingError
		}

		application.Message = message
		application.Status = constants.FriendApplicationStatusUnread
		application.UpdatedTime = now
		if updateErr := l.svcCtx.ApplyFriendModel.Update(l.ctx, application); updateErr != nil {
			return nil, common.WrapError(common.SystemError, updateErr)
		}

		return &types.SendFriendApplicationResponse{
			ApplyFriendId: application.ApplyFriendId,
			Status:        application.Status,
		}, nil
	}
	if err != nil && !errors.Is(err, applyFriendModel.ErrNotFound) {
		return nil, common.WrapError(common.SystemError, err)
	}

	applicationId, err := l.svcCtx.IDGenerator.NextID()
	if err != nil {
		return nil, common.WrapError(common.SystemError, err)
	}
	application = &applyFriendModel.ApplyFriend{
		ApplyFriendId: applicationId,
		SenderId:      identity.UserID,
		ReceiverId:    req.ReceiverId,
		Message:       message,
		Status:        constants.FriendApplicationStatusUnread,
		CreatedTime:   now,
		UpdatedTime:   now,
	}
	if _, err = l.svcCtx.ApplyFriendModel.Insert(l.ctx, application); err != nil {
		return nil, common.WrapError(common.SystemError, err)
	}

	return &types.SendFriendApplicationResponse{
		ApplyFriendId: application.ApplyFriendId,
		Status:        application.Status,
	}, nil

}

func isPendingFriendApplication(status int64) bool {
	return status == constants.FriendApplicationStatusUnread ||
		status == constants.FriendApplicationStatusRead
}
