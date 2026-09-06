// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package user

import (
	"context"

	"infinitechatgo/common/auth"
	"infinitechatgo/common/common"
	pageDto "infinitechatgo/common/model/dto"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetFriendsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetFriendsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetFriendsLogic {
	return &GetFriendsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetFriendsLogic) GetFriends(req *types.FriendPageRequest) (resp *types.FriendPageResponse, err error) {
	identity, ok := auth.IdentityFromContext(l.ctx)
	if !ok {
		return nil, common.NotLoginError
	}

	pageNum := int(req.PageNum)
	pageSize := int(req.PageSize)
	total, err := l.svcCtx.FriendModel.CountByUserId(
		l.ctx,
		identity.UserID,
		req.Keyword,
	)
	if err != nil {
		return nil, common.WrapError(common.SystemError, err)
	}

	items := make([]types.FriendItem, 0, pageSize)
	if total > 0 {
		friends, findErr := l.svcCtx.FriendModel.FindPageByUserId(
			l.ctx,
			identity.UserID,
			req.Keyword,
			pageNum,
			pageSize,
		)
		if findErr != nil {
			return nil, common.WrapError(common.SystemError, findErr)
		}

		for _, friend := range friends {
			items = append(items, types.FriendItem{
				UserId:      friend.UserId,
				Nickname:    friend.Nickname,
				Avatar:      friend.Avatar.String,
				Description: friend.Description.String,
				Gender:      friend.Gender,
				Status:      friend.Status,
				SessionId:   friend.SessionId,
			})
		}
	}

	page := pageDto.NewPageResponse(
		items,
		total,
		pageNum,
		pageSize,
	)

	return &types.FriendPageResponse{
		List:        page.List,
		Total:       page.Total,
		PageSize:    page.PageSize,
		PageNum:     page.PageNum,
		Pages:       page.Pages,
		HasNext:     page.HasNext,
		HasPrevious: page.HasPrevious,
	}, nil
}
