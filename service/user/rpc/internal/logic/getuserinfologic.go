package logic

import (
	"context"
	"errors"

	userModel "infinitechatgo/service/user/model/user"
	"infinitechatgo/service/user/rpc/internal/svc"
	"infinitechatgo/service/user/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GetUserInfoLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetUserInfoLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetUserInfoLogic {
	return &GetUserInfoLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetUserInfoLogic) GetUserInfo(in *user.UserIdReq) (*user.UserInfoResp, error) {
	if in == nil || in.UserId <= 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"user_id is required",
		)
	}

	userInfo, err := l.svcCtx.UserModel.FindOne(l.ctx, in.UserId)
	if err != nil {
		if errors.Is(err, userModel.ErrNotFound) {
			return nil, status.Error(
				codes.NotFound,
				"user not found",
			)
		}

		l.Errorf(
			"查询用户信息失败，userId=%d, err=%v",
			in.UserId,
			err,
		)
		return nil, status.Error(
			codes.Internal,
			"query user info failed",
		)
	}

	if userInfo.State != 0 || userInfo.IsDelete != 0 {
		return nil, status.Error(
			codes.NotFound,
			"user not found",
		)
	}

	return &user.UserInfoResp{
		UserId:   userInfo.UserId,
		Nickname: userInfo.Nickname,
		Avatar:   userInfo.Avatar.String,
	}, nil
}
