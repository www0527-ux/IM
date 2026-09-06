// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package user

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

type GetFriendApplicationsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetFriendApplicationsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetFriendApplicationsLogic {
	return &GetFriendApplicationsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetFriendApplicationsLogic) GetFriendApplications(req *types.FriendApplicationPageRequest) (resp *types.FriendApplicationPageResponse, err error) {
	identity, ok := auth.IdentityFromContext(l.ctx)
	if !ok {
		return nil, common.NotLoginError
	}

	pageNum := int(req.PageNum)
	pageSize := int(req.PageSize)
	applications, total, err := l.svcCtx.ApplyFriendModel.FindPageByUserId(
		l.ctx,
		identity.UserID,
		pageNum,
		pageSize,
	)
	if err != nil {
		return nil, common.WrapError(common.SystemError, err)
	}

	items := make([]types.FriendApplicationItem, 0, len(applications))
	for _, application := range applications {
		items = append(items, types.FriendApplicationItem{
			ApplyFriendId: application.ApplyFriendId,
			UserId:        application.UserId,
			Nickname:      application.Nickname.String,
			Avatar:        application.Avatar.String,
			Message:       application.Message,
			Status:        application.Status,
			IsReceiver:    application.IsReceiver == 1,
			UpdatedTime: application.UpdatedTime.Format(
				time.RFC3339,
			),
		})
	}

	page := pageDto.NewPageResponse(
		items,
		total,
		pageNum,
		pageSize,
	)

	return &types.FriendApplicationPageResponse{
		List:        page.List,
		Total:       page.Total,
		PageSize:    page.PageSize,
		PageNum:     page.PageNum,
		Pages:       page.Pages,
		HasNext:     page.HasNext,
		HasPrevious: page.HasPrevious,
	}, nil
}
