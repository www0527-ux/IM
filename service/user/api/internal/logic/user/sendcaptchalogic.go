// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package user

import (
	"context"
	"infinitechatgo/common/common"
	commonutils "infinitechatgo/common/utils"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"
	"infinitechatgo/service/user/api/internal/types/constants"
	emailutils "infinitechatgo/service/user/api/internal/utils"
	"strings"

	"github.com/zeromicro/go-zero/core/logx"
)

type SendCaptchaLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewSendCaptchaLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SendCaptchaLogic {
	return &SendCaptchaLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *SendCaptchaLogic) SendCaptcha(req *types.SendCaptchaReq) (resp string, err error) {
	// todo: add your logic here and delete this line
	account := strings.TrimSpace(req.Account)

	var redisKey string
	var isEmail bool
	switch {
	case constants.EmailRegex.MatchString(account):
		account = strings.ToLower(account)
		redisKey = constants.LoginCodeEmailPrefix + account
		isEmail = true
	case constants.PhoneRegex.MatchString(account):
		redisKey = constants.LoginCodePhonePrefix + account
	default:
		common.Throw(common.PhoneEmailError)
	}

	captcha, err := commonutils.RandomNumber(6)
	if err != nil {
		return "", common.WrapError(common.SystemError, err)
	}
	codeStr, err := l.svcCtx.Redis.GetCtx(l.ctx, redisKey)
	if err != nil {
		return "", common.WrapError(common.SystemError, err)
	}
	if codeStr != "" {
		return constants.SmsSendFrequentlyMsg, nil
	}
	if !isEmail {
		return "短信验证码功能暂未接入", nil
	}
	emailErr := emailutils.SendEmailCode(
		l.svcCtx.Config.Email,
		account,
		captcha,
	)
	common.ThrowIfCause(
		emailErr != nil,
		common.SystemError,
		emailErr,
	)
	// 邮件发送成功后，把验证码保存进Redis
	redisErr := l.svcCtx.Redis.SetexCtx(
		l.ctx,
		redisKey,
		captcha,
		constants.SmsExpireTime,
	)
	common.ThrowIfCause(
		redisErr != nil,
		common.SystemError,
		redisErr,
	)
	l.Infof("验证码发送成功，账号：%s", account)
	return constants.SmsSendSuccessMsg, nil
}
