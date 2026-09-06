// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package user

import (
	"context"
	"errors"

	"infinitechatgo/common/common"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"
	userModel "infinitechatgo/service/user/model/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetUserInfoLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetUserInfoLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetUserInfoLogic {
	return &GetUserInfoLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetUserInfoLogic) GetUserInfo(req *types.UserIdRequest) (resp *types.UserInfoResponse, err error) {
	currentUser, err := l.svcCtx.UserModel.FindOne(
		l.ctx,
		req.UserId,
	)
	if errors.Is(err, userModel.ErrNotFound) {
		return nil, common.UserNotExists
	}
	if err != nil {
		return nil, common.WrapError(
			common.SystemError,
			err,
		)
	}

	//判断是否已经软删除了
	if currentUser.IsDelete == 1 {
		return nil, common.UserNotExists
	}

	var account string
	switch {
	case currentUser.Email.Valid:
		account = currentUser.Email.String
	case currentUser.Phone.Valid:
		account = currentUser.Phone.String
	}
	return &types.UserInfoResponse{
		UserId:      currentUser.UserId,
		Nickname:    currentUser.Nickname,
		Avatar:      currentUser.Avatar.String,
		Description: currentUser.Description.String,
		Account:     account,
		Gender:      currentUser.Gender,
	}, nil
}
