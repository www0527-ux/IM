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

type KickGroupMemberLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewKickGroupMemberLogic(ctx context.Context, svcCtx *svc.ServiceContext) *KickGroupMemberLogic {
	return &KickGroupMemberLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *KickGroupMemberLogic) KickGroupMember(
	req *types.KickGroupMemberRequest,
) (*types.OperationResponse, error) {
	identity, ok := auth.IdentityFromContext(l.ctx)
	if !ok {
		return nil, common.NotLoginError
	}
	if req.SessionId <= 0 || req.MemberId <= 0 {
		return nil, common.ParamsError
	}
	if req.MemberId == identity.UserID {
		return nil, common.GroupKickNoAuth
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

			operator, findErr :=
				l.svcCtx.UserSessionModel.FindOneForUpdateTx(
					ctx,
					identity.UserID,
					req.SessionId,
				)
			if errors.Is(findErr, userSessionModel.ErrNotFound) {
				return common.GroupKickNoAuth
			}
			if findErr != nil {
				return common.WrapError(common.SystemError, findErr)
			}
			if operator.Status != constants.UserSessionStatusNormal ||
				(operator.Role != constants.UserSessionRoleOwner &&
					operator.Role != constants.UserSessionRoleAdmin) {
				return common.GroupKickNoAuth
			}

			target, findErr :=
				l.svcCtx.UserSessionModel.FindOneForUpdateTx(
					ctx,
					req.MemberId,
					req.SessionId,
				)
			if errors.Is(findErr, userSessionModel.ErrNotFound) {
				return nil
			}
			if findErr != nil {
				return common.WrapError(common.SystemError, findErr)
			}
			if target.Status == constants.UserSessionStatusExited {
				return nil
			}
			if target.Role == constants.UserSessionRoleOwner {
				return common.GroupKickNoAuth
			}
			if operator.Role == constants.UserSessionRoleAdmin &&
				target.Role != constants.UserSessionRoleMember {
				return common.GroupKickNoAuth
			}

			if updateErr :=
				l.svcCtx.UserSessionModel.UpdateStatusTx(
					ctx,
					target.UserId,
					target.SessionId,
					constants.UserSessionStatusExited,
				); updateErr != nil {
				return common.WrapError(common.SystemError, updateErr)
			}
			target.Status = constants.UserSessionStatusExited
			changedMember = target

			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	if changedMember != nil {
		if cacheErr := l.svcCtx.UserSessionModel.ClearCache(
			l.ctx,
			changedMember,
		); cacheErr != nil {
			l.Errorf(
				"清理被移除群成员缓存失败，userId=%d, sessionId=%d, err=%v",
				changedMember.UserId,
				changedMember.SessionId,
				cacheErr,
			)
		}
	}

	return &types.OperationResponse{Success: true}, nil
}
