// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"infinitechatgo/common/auth"
	"infinitechatgo/common/common"

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
	identity, ok := auth.IdentityFromContext(l.ctx)
	if !ok {
		return nil, common.NotLoginError
	}
	if req == nil || req.PageNum < 1 || req.PageSize < 1 || req.PageSize > 100 {
		return nil, common.ParamsError
	}
	details, total, err := l.svcCtx.QueryService.ListSent(l.ctx,
		identity.UserID, req.PageNum, req.PageSize)
	if err != nil {
		return nil, common.WrapError(common.SystemBusy, err)
	}
	list := make([]types.SentRedPacketSummary, 0, len(details))
	for _, detail := range details {
		list = append(list, sentPacketSummary(detail))
	}
	return &types.GetSentRedPacketsResponse{
		List: list, Total: total, PageNum: req.PageNum, PageSize: req.PageSize,
	}, nil
}
