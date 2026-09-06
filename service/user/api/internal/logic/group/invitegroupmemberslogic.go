// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package group

import (
	"context"
	"errors"
	"time"

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

type InviteGroupMembersLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewInviteGroupMembersLogic(ctx context.Context, svcCtx *svc.ServiceContext) *InviteGroupMembersLogic {
	return &InviteGroupMembersLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *InviteGroupMembersLogic) InviteGroupMembers(
	req *types.InviteGroupMembersRequest,
) (*types.InviteGroupMembersResponse, error) {
	identity, ok := auth.IdentityFromContext(l.ctx)
	if !ok {
		return nil, common.NotLoginError
	}
	if req.SessionId <= 0 {
		return nil, common.ParamsError
	}

	memberIds, err := normalizeMemberIds(
		identity.UserID,
		req.MemberIds,
	)
	if err != nil {
		return nil, err
	}
	if len(memberIds) == 0 {
		return nil, common.GroupMemberInvalid
	}

	var changedMembers []*userSessionModel.UserSession
	err = txctx.WithTransaction(
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
				return common.GroupInviteNoAuth
			}
			if findErr != nil {
				return common.WrapError(common.SystemError, findErr)
			}
			if operator.Status != constants.UserSessionStatusNormal ||
				(operator.Role != constants.UserSessionRoleOwner &&
					operator.Role != constants.UserSessionRoleAdmin) {
				return common.GroupInviteNoAuth
			}

			existingMembers, findErr :=
				l.svcCtx.UserSessionModel.FindByUserIdsForUpdateTx(
					ctx,
					req.SessionId,
					memberIds,
				)
			if findErr != nil {
				return common.WrapError(common.SystemError, findErr)
			}

			activeMemberIds := make(
				map[int64]struct{},
				len(existingMembers),
			)
			for _, member := range existingMembers {
				if member.Status == constants.UserSessionStatusNormal {
					activeMemberIds[member.UserId] = struct{}{}
				}
			}

			candidateIds := make([]int64, 0, len(memberIds))
			for _, memberId := range memberIds {
				if _, exists := activeMemberIds[memberId]; !exists {
					candidateIds = append(candidateIds, memberId)
				}
			}
			if len(candidateIds) == 0 {
				return nil
			}

			validIds, findErr :=
				l.svcCtx.FriendModel.FindNormalFriendIdsForUpdateTx(
					ctx,
					identity.UserID,
					candidateIds,
				)
			if findErr != nil {
				return common.WrapError(common.SystemError, findErr)
			}
			if len(validIds) != len(candidateIds) {
				return common.GroupMemberInvalid
			}

			now := time.Now()
			for _, memberId := range candidateIds {
				member := &userSessionModel.UserSession{
					UserId:      memberId,
					SessionId:   req.SessionId,
					Role:        constants.UserSessionRoleMember,
					Status:      constants.UserSessionStatusNormal,
					CreatedTime: now,
					UpdatedTime: now,
				}
				changed, inviteErr :=
					l.svcCtx.UserSessionModel.InviteMemberTx(
						ctx,
						member,
					)
				if inviteErr != nil {
					return common.WrapError(
						common.SystemError,
						inviteErr,
					)
				}
				if changed {
					changedMembers = append(changedMembers, member)
				}
			}

			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	for _, member := range changedMembers {
		if cacheErr := l.svcCtx.UserSessionModel.ClearCache(
			l.ctx,
			member,
		); cacheErr != nil {
			l.Errorf(
				"清理群成员缓存失败，userId=%d, sessionId=%d, err=%v",
				member.UserId,
				member.SessionId,
				cacheErr,
			)
		}
	}

	return &types.InviteGroupMembersResponse{
		AddedCount: int64(len(changedMembers)),
	}, nil
}
