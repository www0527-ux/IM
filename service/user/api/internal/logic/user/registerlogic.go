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
	"infinitechatgo/common/txctx"
	commonutils "infinitechatgo/common/utils"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"
	"infinitechatgo/service/user/api/internal/types/constants"
	userModel "infinitechatgo/service/user/model/user"
	userBalanceModel "infinitechatgo/service/user/model/user_balance"

	"github.com/zeromicro/go-zero/core/logx"
)

type RegisterLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewRegisterLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RegisterLogic {
	return &RegisterLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *RegisterLogic) Register(req *types.UserRegisterRequest) (resp *types.LoginResponse, err error) {
	// todo: add your logic here and delete this line
	//短信验证注册
	account := strings.TrimSpace(req.Account)

	var captchaKey string
	var isEmail bool
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
	if req.Password != req.ConfirmPassword {
		return nil, common.PasswordMatchError
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
	var findErr error
	if isEmail {
		_, findErr = l.svcCtx.UserModel.FindOneByEmail(
			l.ctx,
			sql.NullString{
				String: account,
				Valid:  true,
			},
		)
	} else {
		_, findErr = l.svcCtx.UserModel.FindOneByPhone(
			l.ctx,
			sql.NullString{
				String: account,
				Valid:  true,
			},
		)
	}
	switch {
		case findErr == nil:
			// 查到了用户，说明账号已经被注册。
			return nil, common.UserAlreadyExists

		case errors.Is(findErr, userModel.ErrNotFound):
			// 没查到用户，对注册来说是正常结果，继续执行。

		default:
			// 既不是“找到用户”，也不是“没有用户”，
			// 说明MySQL、缓存或网络出现异常。
			return nil, common.WrapError(
				common.SystemError,
				findErr,
			)
	}
	userID, err := commonutils.NextID()
	if err != nil {
		return nil, common.WrapError(
			common.SystemError,
			err,
		)
	}
	hashedPassword, err := commonutils.HashPassword(req.Password)
	if err != nil {
		return nil, common.WrapError(
			common.SystemError,
			err,
		)
	}
	now := time.Now()

	newUser := &userModel.User{
		UserId:      userID,
		Password:    hashedPassword,
		Nickname:    strings.TrimSpace(req.Nickname),
		Gender:      2,
		State:       0,
		Role:        0,
		CreatedTime: now,
		UpdatedTime: now,
		IsDelete:    0,
	}
	if isEmail {
		newUser.Email = sql.NullString{
			String: account,
			Valid:  true,
		}
	} else {
		newUser.Phone = sql.NullString{
			String: account,
			Valid:  true,
		}
	}
	newBalance := &userBalanceModel.UserBalance{
		UserId:      userID,
		Balance:     100000,
		CreatedTime: now,
		UpdatedTime: now,
	}
	//事务插入用户信息与账单信息
	err = txctx.WithTransaction(
		l.ctx,
		l.svcCtx.SqlConn,
		func(ctx context.Context) error {
			if _, insertErr := l.svcCtx.UserModel.InsertTx(
				ctx,
				newUser,
			); insertErr != nil {
				return common.WrapError(
					//?
					common.OperationError,
					insertErr,
				)
			}
			if _, balanceErr := l.svcCtx.UserBalanceModel.InsertTx(
				ctx,
				newBalance,
			); balanceErr != nil {
				return common.WrapError(
					common.OperationError,
					balanceErr,
				)
			}
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	if cacheErr := l.svcCtx.UserModel.ClearCache(
		l.ctx,
		newUser,
	); cacheErr != nil {
		l.Errorf(
			"清理用户缓存失败，userId=%d, err=%v",
			newUser.UserId,
			cacheErr,
		)
	}
	//删除验证码
	if _, deleteErr := l.svcCtx.Redis.DelCtx(
		l.ctx,
		captchaKey,
	); deleteErr != nil {
		l.Errorf(
			"删除注册验证码失败，account=%s, err=%v",
			account,
			deleteErr,
		)
	}
	//签发token
	tokenPair,err:=l.svcCtx.TokenService.Issue(
		l.ctx,
		newUser.UserId,
	)
	if err != nil {
		return nil, common.WrapError(
			common.SystemError,
			err,
		)
	}

	return &types.LoginResponse{
		UserId:      newUser.UserId,
		Account:     account,
		Nickname:    newUser.Nickname,
		Avatar:      newUser.Avatar.String,
		Gender:      int32(newUser.Gender),
		Description: newUser.Description.String,
		Token: tokenPair.AccessToken,
		RefreshToken: tokenPair.RefreshToken,
		WsServerUri: "",
	}, nil
}
