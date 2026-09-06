package logic

import (
	"context"
	"errors"

	sessionModel "infinitechatgo/service/user/model/session"
	userModel "infinitechatgo/service/user/model/user"
	userSessionModel "infinitechatgo/service/user/model/user_session"
	"infinitechatgo/service/user/rpc/internal/svc"
	"infinitechatgo/service/user/rpc/user"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	groupSessionType            int64 = 1
	messageReasonNotGroupMember       = "NOT_GROUP_MEMBER"
)

type ValidateGroupMessageLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewValidateGroupMessageLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ValidateGroupMessageLogic {
	return &ValidateGroupMessageLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ValidateGroupMessageLogic) ValidateGroupMessage(in *user.ValidateGroupMessageReq) (*user.MessageValidateResp, error) {
	if in == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"request is required",
		)
	}

	if in.SenderId <= 0 || in.SessionId <= 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"sender_id and session_id must be positive",
		)
	}

	sender, err := l.svcCtx.UserModel.FindOne(
		l.ctx,
		in.SenderId,
	)
	if err != nil {
		if errors.Is(err, userModel.ErrNotFound) {
			return &user.MessageValidateResp{
				Allowed:      false,
				RejectReason: messageReasonSenderDisabled,
				ErrorCode:    messageCodeSenderDisabled,
			}, nil
		}

		l.Errorf(
			"查询群消息发送者失败，senderId=%d, err=%v",
			in.SenderId,
			err,
		)
		return nil, status.Error(
			codes.Internal,
			"query sender failed",
		)
	}

	if sender.State != 0 || sender.IsDelete != 0 {
		return &user.MessageValidateResp{
			Allowed:      false,
			RejectReason: messageReasonSenderDisabled,
			ErrorCode:    messageCodeSenderDisabled,
		}, nil
	}

	chatSession, err := l.svcCtx.SessionModel.FindOne(
		l.ctx,
		in.SessionId,
	)
	if err != nil {
		if errors.Is(err, sessionModel.ErrNotFound) {
			return &user.MessageValidateResp{
				Allowed:      false,
				RejectReason: messageReasonInvalidSession,
				ErrorCode:    messageCodeInvalidSession,
			}, nil
		}

		l.Errorf(
			"查询群聊会话失败，sessionId=%d, err=%v",
			in.SessionId,
			err,
		)
		return nil, status.Error(
			codes.Internal,
			"query group session failed",
		)
	}

	if chatSession.Type != groupSessionType ||
		chatSession.Status != sessionStatusNormal {
		return &user.MessageValidateResp{
			Allowed:      false,
			RejectReason: messageReasonInvalidSession,
			ErrorCode:    messageCodeInvalidSession,
		}, nil
	}

	member, err := l.svcCtx.UserSessionModel.FindOne(
		l.ctx,
		in.SenderId,
		in.SessionId,
	)
	if err != nil {
		if errors.Is(err, userSessionModel.ErrNotFound) {
			return &user.MessageValidateResp{
				Allowed:      false,
				RejectReason: messageReasonNotGroupMember,
				ErrorCode:    messageCodeNotSessionMember,
			}, nil
		}

		l.Errorf(
			"查询群成员关系失败，senderId=%d, sessionId=%d, err=%v",
			in.SenderId,
			in.SessionId,
			err,
		)
		return nil, status.Error(
			codes.Internal,
			"query group membership failed",
		)
	}

	if member.Status != userSessionStatusNormal {
		return &user.MessageValidateResp{
			Allowed:      false,
			RejectReason: messageReasonNotGroupMember,
			ErrorCode:    messageCodeNotSessionMember,
		}, nil
	}

	return &user.MessageValidateResp{
		Allowed: true,
	}, nil
}
