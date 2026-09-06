// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package user

import (
	"context"
	"errors"

	"infinitechatgo/common/auth"
	"infinitechatgo/common/common"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"
	"infinitechatgo/service/user/api/internal/types/constants"
	friendModel "infinitechatgo/service/user/model/friend"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetFriendDetailLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetFriendDetailLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetFriendDetailLogic {
	return &GetFriendDetailLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetFriendDetailLogic) GetFriendDetail(req *types.FriendDetailRequest) (resp *types.FriendItem, err error) {
	identity, ok := auth.IdentityFromContext(l.ctx)
	if !ok {
		return nil, common.NotLoginError
	}

	relation, err := l.svcCtx.FriendModel.FindOneByUserIdFriendId(
		l.ctx,
		identity.UserID,
		req.FriendId,
	)
	if errors.Is(err, friendModel.ErrNotFound) ||
		(relation != nil && relation.Status == constants.FriendStatusDeleted) {
		return nil, common.NotFoundError
	}
	if err != nil {
		return nil, common.WrapError(common.SystemError, err)
	}

	return buildFriendItem(
		l.ctx,
		l.svcCtx,
		identity.UserID,
		relation,
	)
}
