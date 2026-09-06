// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package group

import (
	"context"
	"time"

	"infinitechatgo/common/auth"
	"infinitechatgo/common/common"
	pageDto "infinitechatgo/common/model/dto"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetJoinedGroupsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetJoinedGroupsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetJoinedGroupsLogic {
	return &GetJoinedGroupsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetJoinedGroupsLogic) GetJoinedGroups(
	req *types.JoinedGroupPageRequest,
) (*types.JoinedGroupPageResponse, error) {
	identity, ok := auth.IdentityFromContext(l.ctx)
	if !ok {
		return nil, common.NotLoginError
	}

	pageNum := int(req.PageNum)
	pageSize := int(req.PageSize)
	total, err := l.svcCtx.UserSessionModel.CountNormalGroupsByUserId(
		l.ctx,
		identity.UserID,
	)
	if err != nil {
		return nil, common.WrapError(common.SystemError, err)
	}

	items := make([]types.JoinedGroupItem, 0, pageSize)
	if total > 0 {
		groups, findErr :=
			l.svcCtx.UserSessionModel.FindGroupPageByUserId(
				l.ctx,
				identity.UserID,
				pageNum,
				pageSize,
			)
		if findErr != nil {
			return nil, common.WrapError(common.SystemError, findErr)
		}

		for _, group := range groups {
			items = append(items, types.JoinedGroupItem{
				SessionId:  group.SessionId,
				Name:       group.Name,
				Avatar:     group.Avatar.String,
				Role:       group.Role,
				JoinedTime: group.JoinedTime.Format(time.RFC3339),
			})
		}
	}

	page := pageDto.NewPageResponse(
		items,
		total,
		pageNum,
		pageSize,
	)

	return &types.JoinedGroupPageResponse{
		List:        page.List,
		Total:       page.Total,
		PageSize:    page.PageSize,
		PageNum:     page.PageNum,
		Pages:       page.Pages,
		HasNext:     page.HasNext,
		HasPrevious: page.HasPrevious,
	}, nil
}
