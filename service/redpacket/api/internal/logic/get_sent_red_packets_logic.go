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

type GetSentRedPacketsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetSentRedPacketsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetSentRedPacketsLogic {
	return &GetSentRedPacketsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetSentRedPacketsLogic) GetSentRedPackets(req *types.GetSentRedPacketsRequest) (resp *types.GetSentRedPacketsResponse, err error) {
	return nil, application.ErrStageNotImplemented
}
