// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package user

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"infinitechatgo/common/common"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"
	"infinitechatgo/service/user/api/internal/types/constants"
	userModel "infinitechatgo/service/user/model/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type LoginCodeLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewLoginCodeLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LoginCodeLogic {
	return &LoginCodeLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *LoginCodeLogic) LoginCode(req *types.LoginCodeRequest) (resp *types.LoginResponse, err error) {
	account := strings.TrimSpace(req.Account)

	var (
		captchaKey string
		isEmail    bool
	)

	switch {
	case constants.EmailRegex.MatchString(account):
		account = strings.ToLower(account)
		captchaKey = constants.LoginCodeEmailPrefix + account
		isEmail = true

	case constants.PhoneRegex.MatchString(account):
		captchaKey = constants.LoginCodePhonePrefix + account

	default:
		return nil, common.PhoneEmailError
	}

	savedCaptcha, err := l.svcCtx.Redis.GetCtx(
		l.ctx,
		captchaKey,
	)
	if err != nil {
		return nil, common.WrapError(
			common.SystemError,
			err,
		)
	}

	if savedCaptcha == "" || savedCaptcha != req.Code {
		return nil, common.VerificationError
	}

	var currentUser *userModel.User

	if isEmail {
		currentUser, err = l.svcCtx.UserModel.FindOneByEmail(
			l.ctx,
			sql.NullString{
				String: account,
				Valid:  true,
			},
		)
	} else {
		currentUser, err = l.svcCtx.UserModel.FindOneByPhone(
			l.ctx,
			sql.NullString{
				String: account,
				Valid:  true,
			},
		)
	}

	if errors.Is(err, userModel.ErrNotFound) {
		return nil, common.UserNotExists
	}
	if err != nil {
		return nil, common.WrapError(
			common.SystemError,
			err,
		)
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

	// Token签发成功后，验证码即完成使命，防止重复使用。
	if _, deleteErr := l.svcCtx.Redis.DelCtx(
		l.ctx,
		captchaKey,
	); deleteErr != nil {
		l.Errorf(
			"删除登录验证码失败，account=%s, err=%v",
			account,
			deleteErr,
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
		WsServerUri:  "",
	}, nil
}
