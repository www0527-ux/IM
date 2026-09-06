// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package user

import (
	"context"
	"errors"
	"infinitechatgo/common/common"
	userService "infinitechatgo/service/user/api/internal/service"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type RefreshTokenLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewRefreshTokenLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RefreshTokenLogic {
	return &RefreshTokenLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *RefreshTokenLogic) RefreshToken(req *types.RefreshTokenRequest) (resp *types.TokenPairResponse, err error) {
	tokenPair, err := l.svcCtx.TokenService.Rotate(
		l.ctx,
		req.RefreshToken,
	)
	if errors.Is(
		err,
		userService.ErrInvalidRefreshToken,
	) {
		return nil, common.NotLoginError
	}
	if err != nil {
		return nil, common.WrapError(
			common.SystemError,
			err,
		)
	}
	return &types.TokenPairResponse{
		Token:        tokenPair.AccessToken,
		RefreshToken: tokenPair.RefreshToken,
	}, nil
}
