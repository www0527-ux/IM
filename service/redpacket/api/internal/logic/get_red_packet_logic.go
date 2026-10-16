// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"errors"
	"infinitechatgo/common/auth"
	"infinitechatgo/common/common"
	"infinitechatgo/service/redpacket/application"

	"infinitechatgo/service/redpacket/api/internal/svc"
	"infinitechatgo/service/redpacket/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetRedPacketLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetRedPacketLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetRedPacketLogic {
	return &GetRedPacketLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetRedPacketLogic) GetRedPacket(req *types.GetRedPacketRequest) (resp *types.RedPacketDetailResponse, err error) {
	identity, ok := auth.IdentityFromContext(l.ctx)
	if !ok {
		return nil, common.NotLoginError
	}
	if req == nil || req.RedPacketId <= 0 {
		return nil, common.ParamsError
	}
	detail, err := l.svcCtx.QueryService.Get(l.ctx, req.RedPacketId, identity.UserID)
	if err != nil {
		switch {
		case errors.Is(err, application.ErrReceiveNotFound):
			return nil, common.WrapError(common.NotFoundError, err)
		case errors.Is(err, application.ErrReceivePermission):
			return nil, common.WrapError(common.NoAuthError, err)
		default:
			return nil, common.WrapError(common.SystemBusy, err)
		}
	}
	value := detailResponse(detail)
	return &value, nil
}
