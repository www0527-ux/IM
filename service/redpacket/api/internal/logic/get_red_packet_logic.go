// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
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
	return nil, application.ErrStageNotImplemented
}
