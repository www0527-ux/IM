// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package user

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"infinitechatgo/common/common"
	commonutils "infinitechatgo/common/utils"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"
	"infinitechatgo/service/user/api/internal/types/constants"
	userModel "infinitechatgo/service/user/model/user"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdatePasswordLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewUpdatePasswordLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdatePasswordLogic {
	return &UpdatePasswordLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UpdatePasswordLogic) UpdatePassword(req *types.UpdatePasswordRequest) (resp string, err error) {
	account := strings.TrimSpace(req.Account)

	var (
		captchaKey  string
		currentUser *userModel.User
	)

	switch {
	case constants.EmailRegex.MatchString(account):
		account = strings.ToLower(account)
		captchaKey = constants.LoginCodeEmailPrefix + account
		currentUser, err = l.svcCtx.UserModel.FindOneByEmail(
			l.ctx,
			sql.NullString{
				String: account,
				Valid:  true,
			},
		)

	case constants.PhoneRegex.MatchString(account):
		captchaKey = constants.LoginCodePhonePrefix + account
		currentUser, err = l.svcCtx.UserModel.FindOneByPhone(
			l.ctx,
			sql.NullString{
				String: account,
				Valid:  true,
			},
		)

	default:
		return "", common.PhoneEmailError
	}

	if errors.Is(err, userModel.ErrNotFound) {
		return "", common.UserNotExists
	}
	if err != nil {
		return "", common.WrapError(
			common.SystemError,
			err,
		)
	}

	savedCaptcha, err := l.svcCtx.Redis.GetCtx(
		l.ctx,
		captchaKey,
	)
	if err != nil {
		return "", common.WrapError(
			common.SystemError,
			err,
		)
	}
	if savedCaptcha == "" || savedCaptcha != req.Code {
		return "", common.VerificationError
	}

	if req.Password != req.ConfirmPassword {
		return "", common.PasswordMatchError
	}

	hashedPassword, err := commonutils.HashPassword(req.Password)
	if err != nil {
		return "", common.WrapError(
			common.SystemError,
			err,
		)
	}

	currentUser.Password = hashedPassword
	currentUser.UpdatedTime = time.Now()
	if err := l.svcCtx.UserModel.Update(
		l.ctx,
		currentUser,
	); err != nil {
		return "", common.WrapError(
			common.OperationError,
			err,
		)
	}

	// 密码修改成功后立即作废验证码，防止同一验证码重复修改密码。
	if _, deleteErr := l.svcCtx.Redis.DelCtx(
		l.ctx,
		captchaKey,
	); deleteErr != nil {
		l.Errorf(
			"删除修改密码验证码失败，account=%s, err=%v",
			account,
			deleteErr,
		)
	}

	return constants.PasswordUpdateSuccessMsg, nil
}
