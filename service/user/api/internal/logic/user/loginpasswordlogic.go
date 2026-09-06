// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package user

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"infinitechatgo/common/common"
	commonutils "infinitechatgo/common/utils"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"
	"infinitechatgo/service/user/api/internal/types/constants"
	userModel "infinitechatgo/service/user/model/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type LoginPasswordLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewLoginPasswordLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LoginPasswordLogic {
	return &LoginPasswordLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *LoginPasswordLogic) LoginPassword(req *types.LoginPasswordRequest) (resp *types.LoginResponse, err error) {
	account := strings.TrimSpace(req.Account)
	var (
		currentUser *userModel.User
		findErr     error
	)
	switch {
	case constants.EmailRegex.MatchString(account):
		account = strings.ToLower(account)
		currentUser, findErr = l.svcCtx.UserModel.FindOneByEmail(
			l.ctx,
			sql.NullString{
				String: account,
				Valid:  true,
			},
		)

	case constants.PhoneRegex.MatchString(account):
		currentUser, findErr = l.svcCtx.UserModel.FindOneByPhone(
			l.ctx,
			sql.NullString{
				String: account,
				Valid:  true,
			},
		)

	default:
		return nil, common.PhoneEmailError
	}
	//业务错误：用户不存在
	if errors.Is(findErr, userModel.ErrNotFound) {
		return nil, common.LoginError
	}
	if findErr != nil {
		return nil, common.WrapError(
			common.SystemError,
			findErr,
		)
	}
	if err := commonutils.ComparePassword(
		currentUser.Password,
		req.Password,
	); err != nil {
		return nil, common.LoginError
	}
	tokenPair, err := l.svcCtx.TokenService.Issue(
		l.ctx,
		currentUser.UserId,
	)
	if err != nil {
		return nil, common.WrapError(
			common.SystemError,
			err,
		)
	}
	return &types.LoginResponse{
		UserId:       currentUser.UserId,
		Account:      account,
		Nickname:     currentUser.Nickname,
		Avatar:       currentUser.Avatar.String,
		Gender:       int32(currentUser.Gender),
		Description:  currentUser.Description.String,
		Token:        tokenPair.AccessToken,
		RefreshToken: tokenPair.RefreshToken,

		// 等以后实现实时服务分配。
		WsServerUri: "",
	}, nil
}
