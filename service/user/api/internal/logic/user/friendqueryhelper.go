package user

import (
	"context"
	"errors"

	"infinitechatgo/common/common"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"
	friendModel "infinitechatgo/service/user/model/friend"
	userModel "infinitechatgo/service/user/model/user"
	userSessionModel "infinitechatgo/service/user/model/user_session"
)
//用于展示好友的详细信息
func buildFriendItem(
	ctx context.Context,
	svcCtx *svc.ServiceContext,
	currentUserId int64,
	relation *friendModel.Friend,
) (*types.FriendItem, error) {
	friendUser, err := svcCtx.UserModel.FindOne(ctx, relation.FriendId)
	if errors.Is(err, userModel.ErrNotFound) ||
		friendUser == nil || friendUser.IsDelete == 1 {
		return nil, common.UserNotExists
	}
	if err != nil {
		return nil, common.WrapError(common.SystemError, err)
	}

	var sessionId int64
	sessionId, err = svcCtx.UserSessionModel.FindSingleSessionId(
		ctx,
		currentUserId,
		relation.FriendId,
	)
	if errors.Is(err, userSessionModel.ErrNotFound) {
		sessionId = 0
	} else if err != nil {
		return nil, common.WrapError(common.SystemError, err)
	}

	return &types.FriendItem{
		UserId:      friendUser.UserId,
		Nickname:    friendUser.Nickname,
		Avatar:      friendUser.Avatar.String,
		Description: friendUser.Description.String,
		Gender:      friendUser.Gender,
		Status:      relation.Status,
		SessionId:   sessionId,
	}, nil
}
