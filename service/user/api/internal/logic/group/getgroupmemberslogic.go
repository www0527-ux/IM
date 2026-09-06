// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package group

import (
	"context"
	"errors"
	"time"

	"infinitechatgo/common/auth"
	"infinitechatgo/common/common"
	pageDto "infinitechatgo/common/model/dto"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"
	"infinitechatgo/service/user/api/internal/types/constants"
	chatSessionModel "infinitechatgo/service/user/model/session"
	userSessionModel "infinitechatgo/service/user/model/user_session"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetGroupMembersLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetGroupMembersLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetGroupMembersLogic {
	return &GetGroupMembersLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetGroupMembersLogic) GetGroupMembers(
	req *types.GroupMemberPageRequest,
) (*types.GroupMemberPageResponse, error) {
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

	pageNum := int(req.PageNum)
	pageSize := int(req.PageSize)
	total, err := l.svcCtx.UserSessionModel.CountNormalBySessionId(
		l.ctx,
		req.SessionId,
	)
	if err != nil {
		return nil, common.WrapError(common.SystemError, err)
	}

	items := make([]types.GroupMemberItem, 0, pageSize)
	if total > 0 {
		members, findErr :=
			l.svcCtx.UserSessionModel.FindPageBySessionId(
				l.ctx,
				req.SessionId,
				pageNum,
				pageSize,
			)
		if findErr != nil {
			return nil, common.WrapError(common.SystemError, findErr)
		}

		for _, member := range members {
			items = append(items, types.GroupMemberItem{
				UserId:     member.UserId,
				Nickname:   member.Nickname,
				Avatar:     member.Avatar.String,
				Role:       member.Role,
				JoinedTime: member.JoinedTime.Format(time.RFC3339),
			})
		}
	}

	page := pageDto.NewPageResponse(
		items,
		total,
		pageNum,
		pageSize,
	)

	return &types.GroupMemberPageResponse{
		List:        page.List,
		Total:       page.Total,
		PageSize:    page.PageSize,
		PageNum:     page.PageNum,
		Pages:       page.Pages,
		HasNext:     page.HasNext,
		HasPrevious: page.HasPrevious,
	}, nil
}
