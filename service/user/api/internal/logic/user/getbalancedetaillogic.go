// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package user

import (
	"context"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"infinitechatgo/common/auth"
	"infinitechatgo/common/common"
	pageDto "infinitechatgo/common/model/dto"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"
	"infinitechatgo/service/user/model/balance_log"
)

type GetBalanceDetailLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetBalanceDetailLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetBalanceDetailLogic {
	return &GetBalanceDetailLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetBalanceDetailLogic) GetBalanceDetail(
	req *types.GetBalanceDetailRequest,
) (*types.GetBalanceDetailResponse, error) {
	identity, ok := auth.IdentityFromContext(l.ctx)
	if !ok {
		return nil, common.NotLoginError
	}

	pageNum := int(req.PageNum)
	pageSize := int(req.PageSize)

	total, err := l.svcCtx.BalanceLogModel.CountByUserId(
		l.ctx,
		identity.UserID,
	)
	if err != nil {
		return nil, common.WrapError(
			common.SystemError,
			err,
		)
	}

	var logs []*balance_log.BalanceLog

	if total > 0 {
		logs, err = l.svcCtx.BalanceLogModel.FindPageByUserId(
			l.ctx,
			identity.UserID,
			pageNum,
			pageSize,
		)
		if err != nil {
			return nil, common.WrapError(
				common.SystemError,
				err,
			)
		}
	}

	list := make(
		[]types.BalanceLogItem,
		0,
		len(logs),
	)

	for _, log := range logs {
		var relatedId int64
		if log.RelatedId.Valid {
			relatedId = log.RelatedId.Int64
		}

		list = append(list, types.BalanceLogItem{
			BalanceLogId: log.BalanceLogId,
			Amount:       log.Amount,
			Type:         log.Type,
			RelatedId:    relatedId,
			CreatedTime: log.CreatedTime.Format(
				time.RFC3339,
			),
		})
	}

	page := pageDto.NewPageResponse(
		list,
		total,
		pageNum,
		pageSize,
	)

	return &types.GetBalanceDetailResponse{
		List:        page.List,
		Total:       page.Total,
		PageSize:    page.PageSize,
		PageNum:     page.PageNum,
		Pages:       page.Pages,
		HasNext:     page.HasNext,
		HasPrevious: page.HasPrevious,
	}, nil
}