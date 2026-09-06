// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package user

import (
	"context"
	"errors"

	"infinitechatgo/common/auth"
	"infinitechatgo/common/common"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"
	balanceModel "infinitechatgo/service/user/model/user_balance"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetUserBalanceLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetUserBalanceLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetUserBalanceLogic {
	return &GetUserBalanceLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetUserBalanceLogic) GetUserBalance() (resp *types.GetUserBalanceResponse, err error) {
	identity, ok := auth.IdentityFromContext(l.ctx)
	if !ok {
		return nil, common.NotLoginError
	}

	balance, err := l.svcCtx.UserBalanceModel.FindOne(
		l.ctx,
		identity.UserID,
	)
	if errors.Is(err, balanceModel.ErrNotFound) {
		return nil, common.NotFoundError
	}
	if err != nil {
		return nil, common.WrapError(
			common.SystemError,
			err,
		)
	}

	return &types.GetUserBalanceResponse{
		BalanceCents: balance.Balance,
	}, nil
}
