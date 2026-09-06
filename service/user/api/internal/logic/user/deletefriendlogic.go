// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package user

import (
	"context"
	"errors"

	"infinitechatgo/common/auth"
	"infinitechatgo/common/common"
	"infinitechatgo/common/txctx"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"
	"infinitechatgo/service/user/api/internal/types/constants"
	friendModel "infinitechatgo/service/user/model/friend"

	"github.com/zeromicro/go-zero/core/logx"
)

type DeleteFriendLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewDeleteFriendLogic(
	ctx context.Context,
	svcCtx *svc.ServiceContext,
) *DeleteFriendLogic {
	return &DeleteFriendLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *DeleteFriendLogic) DeleteFriend(
	req *types.FriendOperationRequest,
) (*types.OperationResponse, error) {
	identity, ok := auth.IdentityFromContext(l.ctx)
	if !ok {
		return nil, common.NotLoginError
	}
	if identity.UserID == req.FriendId {
		return nil, common.FriendRelationNotFound
	}

	var relations []*friendModel.Friend
	err := txctx.WithTransaction(
		l.ctx,
		l.svcCtx.SqlConn,
		func(ctx context.Context) error {
			var updateErr error
			relations, updateErr =
				l.svcCtx.FriendModel.UpdatePairStatusTx(
					ctx,
					identity.UserID,
					req.FriendId,
					constants.FriendStatusDeleted,
				)
			if errors.Is(updateErr, friendModel.ErrNotFound) {
				return common.FriendRelationNotFound
			}
			if updateErr != nil {
				return common.WrapError(
					common.SystemError,
					updateErr,
				)
			}

			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	for _, relation := range relations {
		if cacheErr := l.svcCtx.FriendModel.ClearCache(
			l.ctx,
			relation,
		); cacheErr != nil {
			l.Errorf(
				"清理好友关系缓存失败，userId=%d, friendId=%d, err=%v",
				relation.UserId,
				relation.FriendId,
				cacheErr,
			)
		}
	}

	return &types.OperationResponse{Success: true}, nil
}
