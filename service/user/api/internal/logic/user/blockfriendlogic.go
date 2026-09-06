// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package user

import (
	"context"
	"errors"
	"time"

	"infinitechatgo/common/auth"
	"infinitechatgo/common/common"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"
	"infinitechatgo/service/user/api/internal/types/constants"
	friendModel "infinitechatgo/service/user/model/friend"

	"github.com/zeromicro/go-zero/core/logx"
)

type BlockFriendLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewBlockFriendLogic(ctx context.Context, svcCtx *svc.ServiceContext) *BlockFriendLogic {
	return &BlockFriendLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *BlockFriendLogic) BlockFriend(req *types.FriendOperationRequest) (resp *types.OperationResponse, err error) {
	identity, ok := auth.IdentityFromContext(l.ctx)
	if !ok {
		return nil, common.NotLoginError
	}

	relation, err := l.svcCtx.FriendModel.FindOneByUserIdFriendId(
		l.ctx,
		identity.UserID,
		req.FriendId,
	)
	if errors.Is(err, friendModel.ErrNotFound) {
		return nil, common.FriendRelationNotFound
	}
	if err != nil {
		return nil, common.WrapError(common.SystemError, err)
	}
	if relation == nil || relation.Status == constants.FriendStatusDeleted {
		return nil, common.FriendRelationNotFound
	}
	if relation.Status == constants.FriendStatusBlocked {
		return &types.OperationResponse{Success: true}, nil
	}

	relation.Status = constants.FriendStatusBlocked
	relation.UpdatedTime = time.Now()
	if err := l.svcCtx.FriendModel.Update(l.ctx, relation); err != nil {
		return nil, common.WrapError(common.SystemError, err)
	}

	return &types.OperationResponse{Success: true}, nil
}
