// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package group

import (
	"context"
	"database/sql"
	"sort"
	"strings"
	"time"

	"infinitechatgo/common/auth"
	"infinitechatgo/common/common"
	"infinitechatgo/common/txctx"
	commonutils "infinitechatgo/common/utils"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"
	"infinitechatgo/service/user/api/internal/types/constants"
	chatSessionModel "infinitechatgo/service/user/model/session"
	userSessionModel "infinitechatgo/service/user/model/user_session"

	"github.com/zeromicro/go-zero/core/logx"
)

const (
	defaultGroupName      = "新群聊"
	maxInitialGroupMember = 100
)

type CreateGroupLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewCreateGroupLogic(
	ctx context.Context,
	svcCtx *svc.ServiceContext,
) *CreateGroupLogic {
	return &CreateGroupLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *CreateGroupLogic) CreateGroup(
	req *types.CreateGroupRequest,
) (*types.CreateGroupResponse, error) {
	identity, ok := auth.IdentityFromContext(l.ctx)
	if !ok {
		return nil, common.NotLoginError
	}

	memberIds, err := normalizeMemberIds(
		identity.UserID,
		req.MemberIds,
	)
	if err != nil {
		return nil, err
	}

	validIds, err := l.svcCtx.FriendModel.FindNormalFriendIds(
		l.ctx,
		identity.UserID,
		memberIds,
	)
	if err != nil {
		return nil, common.WrapError(common.SystemError, err)
	}
	if len(validIds) != len(memberIds) {
		return nil, common.GroupMemberInvalid
	}

	groupName := strings.TrimSpace(req.Name)
	if groupName == "" {
		groupName = defaultGroupName
	}

	sessionId, err := commonutils.NextID()
	if err != nil {
		return nil, common.WrapError(common.SystemError, err)
	}

	now := time.Now()
	groupSession := &chatSessionModel.Session{
		SessionId: sessionId,
		Name:      groupName,
		Type:      constants.SessionTypeGroup,
		SingleKey: sql.NullString{Valid: false},
		Status:    constants.SessionStatusNormal,
		Avatar: sql.NullString{
			String: req.Avatar,
			Valid:  req.Avatar != "",
		},
		CreatedTime: now,
		UpdatedTime: now,
	}

	members := make(
		[]*userSessionModel.UserSession,
		0,
		len(memberIds)+1,
	)
	members = append(members, &userSessionModel.UserSession{
		UserId:      identity.UserID,
		SessionId:   sessionId,
		Role:        constants.UserSessionRoleOwner,
		Status:      constants.UserSessionStatusNormal,
		CreatedTime: now,
		UpdatedTime: now,
	})
	for _, memberId := range memberIds {
		members = append(members, &userSessionModel.UserSession{
			UserId:      memberId,
			SessionId:   sessionId,
			Role:        constants.UserSessionRoleMember,
			Status:      constants.UserSessionStatusNormal,
			CreatedTime: now,
			UpdatedTime: now,
		})
	}

	err = txctx.WithTransaction(
		l.ctx,
		l.svcCtx.SqlConn,
		func(ctx context.Context) error {
			if _, insertErr := l.svcCtx.SessionModel.InsertTx(
				ctx,
				groupSession,
			); insertErr != nil {
				return common.WrapError(
					common.SystemError,
					insertErr,
				)
			}

			for _, member := range members {
				if memberErr := l.svcCtx.UserSessionModel.UpsertTx(
					ctx,
					member,
				); memberErr != nil {
					return common.WrapError(
						common.SystemError,
						memberErr,
					)
				}
			}

			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	if cacheErr := l.svcCtx.SessionModel.ClearCache(
		l.ctx,
		groupSession,
	); cacheErr != nil {
		l.Errorf(
			"清理群聊缓存失败，sessionId=%d, err=%v",
			sessionId,
			cacheErr,
		)
	}
	for _, member := range members {
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

	return &types.CreateGroupResponse{
		SessionId:   sessionId,
		Name:        groupName,
		Avatar:      req.Avatar,
		MemberCount: int64(len(members)),
	}, nil
}

func normalizeMemberIds(
	creatorId int64,
	memberIds []int64,
) ([]int64, error) {
	seen := make(map[int64]struct{}, len(memberIds))
	normalized := make([]int64, 0, len(memberIds))
	for _, memberId := range memberIds {
		if memberId <= 0 {
			return nil, common.GroupMemberInvalid
		}
		if memberId == creatorId {
			continue
		}
		if _, exists := seen[memberId]; exists {
			continue
		}

		seen[memberId] = struct{}{}
		normalized = append(normalized, memberId)
	}
	if len(normalized) > maxInitialGroupMember {
		return nil, common.GroupMemberLimitError
	}
	sort.Slice(normalized, func(i, j int) bool {
		return normalized[i] < normalized[j]
	})

	return normalized, nil
}
