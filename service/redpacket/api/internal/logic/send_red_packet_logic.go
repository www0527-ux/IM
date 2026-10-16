// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package logic

import (
	"context"
	"errors"

	"infinitechatgo/common/auth"
	"infinitechatgo/common/common"
	"infinitechatgo/service/redpacket/api/internal/svc"
	"infinitechatgo/service/redpacket/api/internal/types"
	"infinitechatgo/service/redpacket/application"

	"github.com/zeromicro/go-zero/core/logx"
)

type SendRedPacketLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewSendRedPacketLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SendRedPacketLogic {
	return &SendRedPacketLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *SendRedPacketLogic) SendRedPacket(req *types.SendRedPacketRequest) (resp *types.SendRedPacketResponse, err error) {
	identity, ok := auth.IdentityFromContext(l.ctx)
	if !ok {
		return nil, common.NotLoginError
	}

	result, err := l.svcCtx.SendService.Send(
		l.ctx,
		application.SendCommand{
			SenderID:        identity.UserID,
			ClientMessageID: req.ClientMessageId,
			SessionID:       req.SessionId,
			SessionType:     req.SessionType,
			ReceiverID:      req.ReceiverId,
			RedPacketType:   req.RedPacketType,
			WrapperText:     req.WrapperText,
			TotalAmount:     req.TotalAmount,
			TotalCount:      req.TotalCount,
		},
	)
	if err != nil {
		var validationErr application.SendValidationError
		switch {
		case errors.As(err, &validationErr):
			return nil, common.WrapError(common.ParamsError, err)
		case errors.Is(err, application.ErrSendPermissionDenied):
			return nil, common.WrapError(common.NoAuthError, err)
		case errors.Is(err, application.ErrSendInsufficientBalance):
			return nil, common.WrapError(common.RedPacketInsufficientBalance, err)
		case errors.Is(err, application.ErrSendIdempotencyConflict):
			return nil, common.WrapError(common.RedPacketSendConflict, err)
		case errors.Is(err, application.ErrSendInvariantViolation),
			errors.Is(err, application.ErrSendDependenciesMissing):
			return nil, common.WrapError(common.SystemError, err)
		default:
			return nil, common.WrapError(common.SystemBusy, err)
		}
	}

	return &types.SendRedPacketResponse{
		RedPacketId: result.RedPacketID,
		MessageId:   result.MessageID,
		Status:      result.Status,
	}, nil
}
