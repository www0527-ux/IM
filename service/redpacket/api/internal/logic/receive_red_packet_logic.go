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

type ReceiveRedPacketLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewReceiveRedPacketLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ReceiveRedPacketLogic {
	return &ReceiveRedPacketLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ReceiveRedPacketLogic) ReceiveRedPacket(req *types.ReceiveRedPacketRequest) (resp *types.ReceiveRedPacketResponse, err error) {
	identity, ok := auth.IdentityFromContext(l.ctx)
	if !ok {
		return nil, common.NotLoginError
	}
	if req == nil || req.RedPacketId <= 0 {
		return nil, common.ParamsError
	}
	result, err := l.svcCtx.ReceiveService.Receive(l.ctx, req.RedPacketId, identity.UserID)
	if err != nil {
		switch {
		case errors.Is(err, application.ErrReceiveNotFound):
			return nil, common.WrapError(common.NotFoundError, err)
		case errors.Is(err, application.ErrReceivePermission):
			return nil, common.WrapError(common.NoAuthError, err)
		case errors.Is(err, application.ErrReceiveExpired):
			return nil, common.WrapError(common.RedPacketExpired, err)
		case errors.Is(err, application.ErrReceiveSoldOut):
			return nil, common.WrapError(common.RedPacketSoldOut, err)
		case errors.Is(err, application.ErrReceiveInvariant):
			return nil, common.WrapError(common.SystemError, err)
		default:
			return nil, common.WrapError(common.SystemBusy, err)
		}
	}
	return &types.ReceiveRedPacketResponse{
		ReceiveId: result.ReceiveID, ShareId: result.ShareID,
		Amount: result.Amount, ReceivedAt: result.ReceivedAt.UnixMilli(),
	}, nil
}
