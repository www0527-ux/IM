// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package user

import (
	"context"
	"database/sql"
	"errors"

	"infinitechatgo/common/auth"
	"infinitechatgo/common/common"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"
	userModel "infinitechatgo/service/user/model/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateAvatarLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewUpdateAvatarLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateAvatarLogic {
	return &UpdateAvatarLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UpdateAvatarLogic) UpdateAvatar(req *types.UpdateAvatarRequest) (resp *types.UpdateAvatarResponse, err error) {
	identity, ok := auth.IdentityFromContext(l.ctx)
	if !ok {
		return nil, common.NotLoginError
	}

	currentUser, err := l.svcCtx.UserModel.FindOne(
		l.ctx,
		identity.UserID,
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
	currentUser.Avatar = sql.NullString{
		String: req.Avatar,
		Valid:  true,
	}
	if err:=l.svcCtx.UserModel.Update(
		l.ctx,
		currentUser,
	);err!=nil{
		return nil, common.WrapError(
			common.SystemError,
			err,
		)
	}
	return &types.UpdateAvatarResponse{
		Avatar: req.Avatar,
	}, nil
}