// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package group

import (
	"context"
	"errors"

	"infinitechatgo/common/auth"
	"infinitechatgo/common/common"
	"infinitechatgo/common/txctx"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"
	"infinitechatgo/service/user/api/internal/types/constants"
	chatSessionModel "infinitechatgo/service/user/model/session"
	userSessionModel "infinitechatgo/service/user/model/user_session"

	"github.com/zeromicro/go-zero/core/logx"
)

type LeaveGroupLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewLeaveGroupLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LeaveGroupLogic {
	return &LeaveGroupLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *LeaveGroupLogic) LeaveGroup(
	req *types.LeaveGroupRequest,
) (*types.OperationResponse, error) {
	identity, ok := auth.IdentityFromContext(l.ctx)
	if !ok {
		return nil, common.NotLoginError
	}
	if req.SessionId <= 0 {
		return nil, common.ParamsError
	}

	var changedMember *userSessionModel.UserSession
	err := txctx.WithTransaction(
		l.ctx,
		l.svcCtx.SqlConn,
		func(ctx context.Context) error {
			groupSession, findErr :=
				l.svcCtx.SessionModel.FindOneForUpdateTx(
					ctx,
					req.SessionId,
				)
			if errors.Is(findErr, chatSessionModel.ErrNotFound) {
				return common.GroupNotFound
			}
			if findErr != nil {
				return common.WrapError(common.SystemError, findErr)
			}
			if groupSession.Type != constants.SessionTypeGroup ||
				groupSession.Status != constants.SessionStatusNormal {
				return common.GroupNotFound
			}

			member, findErr :=
				l.svcCtx.UserSessionModel.FindOneForUpdateTx(
					ctx,
					identity.UserID,
					req.SessionId,
				)
			if errors.Is(findErr, userSessionModel.ErrNotFound) {
				return nil
			}
			if findErr != nil {
				return common.WrapError(common.SystemError, findErr)
			}
			if member.Status == constants.UserSessionStatusExited {
				return nil
			}
			if member.Role == constants.UserSessionRoleOwner {
				return common.GroupOwnerCannotLeave
			}

			if updateErr :=
				l.svcCtx.UserSessionModel.UpdateStatusTx(
					ctx,
					member.UserId,
					member.SessionId,
					constants.UserSessionStatusExited,
				); updateErr != nil {
				return common.WrapError(common.SystemError, updateErr)
			}
			member.Status = constants.UserSessionStatusExited
			changedMember = member

			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	if changedMember != nil {
		notifyGroupMembersChanged(l.ctx, l.svcCtx, req.SessionId)
		if cacheErr := l.svcCtx.UserSessionModel.ClearCache(
			l.ctx,
			changedMember,
		); cacheErr != nil {
			l.Errorf(
				"清理退出群成员缓存失败，userId=%d, sessionId=%d, err=%v",
				changedMember.UserId,
				changedMember.SessionId,
				cacheErr,
			)
		}
	}

	return &types.OperationResponse{Success: true}, nil
}
