package logic

import (
	"context"
	"errors"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	friendModel "infinitechatgo/service/user/model/friend"
	sessionModel "infinitechatgo/service/user/model/session"
	userModel "infinitechatgo/service/user/model/user"
	userSessionModel "infinitechatgo/service/user/model/user_session"
	"infinitechatgo/service/user/rpc/internal/svc"
	"infinitechatgo/service/user/rpc/user"
)

const (
	messageCodeSenderDisabled   int32 = 91006
	messageCodeReceiverDisabled int32 = 91007
)
const (
	messageReasonSenderDisabled   = "SENDER_DISABLED"
	messageReasonReceiverDisabled = "RECEIVER_DISABLED"
)

// 业务数字
const (
	sessionTypeSingle       int64 = 0
	sessionStatusNormal     int64 = 0
	userSessionStatusNormal int64 = 0
	friendStatusNormal      int64 = 0
	friendStatusBlocked     int64 = 1
	friendStatusDeleted     int64 = 2
)
const (
	messageCodeInvalidSession    int32 = 91001
	messageCodeNotFriend         int32 = 91002
	messageCodeBlockedByReceiver int32 = 91003
	messageCodeFriendDeleted     int32 = 91004
	messageCodeNotSessionMember  int32 = 91005
	messageCodeBlockedBySender   int32 = 91008
)
const (
	messageReasonInvalidSession    = "INVALID_SESSION"
	messageReasonNotFriend         = "NOT_FRIEND"
	messageReasonBlockedByReceiver = "BLOCKED_BY_RECEIVER"
	messageReasonFriendDeleted     = "FRIEND_DELETED"
	messageReasonNotSessionMember  = "NOT_SESSION_MEMBER"
	messageReasonBlockedBySender   = "BLOCKED_BY_SENDER"
)

type ValidateSingleMessageLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewValidateSingleMessageLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ValidateSingleMessageLogic {
	return &ValidateSingleMessageLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ValidateSingleMessageLogic) ValidateSingleMessage(in *user.ValidateSingleMessageReq) (*user.MessageValidateResp, error) {
	if in == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"request is required",
		)
	}
	//基本参数检查
	if in.SenderId <= 0 ||
		in.ReceiverId <= 0 ||
		in.SessionId <= 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"sender_id, receiver_id and session_id must be positive",
		)
	}
	if in.SenderId == in.ReceiverId {
		return nil, status.Error(
			codes.InvalidArgument,
			"sender_id and receiver_id cannot be the same",
		)
	}
	//判断用户状态
	sender, err := l.svcCtx.UserModel.FindOne(
		l.ctx,
		in.SenderId,
	)
	//业务错误
	if err != nil {
		if errors.Is(err, userModel.ErrNotFound) {
			return &user.MessageValidateResp{
				Allowed:      false,
				RejectReason: messageReasonSenderDisabled,
				ErrorCode:    messageCodeSenderDisabled,
			}, nil
		}
		l.Errorf(
			"查询发送者失败，senderId=%d, err=%v",
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
	receiver, err := l.svcCtx.UserModel.FindOne(
		l.ctx,
		in.ReceiverId,
	)
	if err != nil {
		if errors.Is(err, userModel.ErrNotFound) {
			return &user.MessageValidateResp{
				Allowed:      false,
				RejectReason: messageReasonReceiverDisabled,
				ErrorCode:    messageCodeReceiverDisabled,
			}, nil
		}
		l.Errorf(
			"查询接收者失败，receiverId=%d, err=%v",
			in.ReceiverId,
			err,
		)
		return nil, status.Error(
			codes.Internal,
			"query receiver failed",
		)
	}
	if receiver.State != 0 || receiver.IsDelete != 0 {
		return &user.MessageValidateResp{
			Allowed:      false,
			RejectReason: messageReasonReceiverDisabled,
			ErrorCode:    messageCodeReceiverDisabled,
		}, nil
	}
	chatSession, err := l.svcCtx.SessionModel.FindOne(
		l.ctx,
		in.SessionId,
	)
	//验证session是否存在
	if err != nil {
		if errors.Is(err, sessionModel.ErrNotFound) {
			return rejectSingleMessage(
				messageReasonInvalidSession,
				messageCodeInvalidSession,
			)
		}

		l.Errorf(
			"查询单聊会话失败，sessionId=%d, err=%v",
			in.SessionId,
			err,
		)
		return nil, status.Error(
			codes.Internal,
			"query session failed",
		)
	}
	expectedSingleKey := sessionModel.BuildSingleKey(
		in.SenderId,
		in.ReceiverId,
	)
	//验证session状态
	if chatSession.Type != sessionTypeSingle ||
		chatSession.Status != sessionStatusNormal ||
		!chatSession.SingleKey.Valid ||
		chatSession.SingleKey.String != expectedSingleKey {
		return rejectSingleMessage(
			messageReasonInvalidSession,
			messageCodeInvalidSession,
		)
	}
	senderMember, err := l.svcCtx.UserSessionModel.FindOne(
		l.ctx,
		in.SenderId,
		in.SessionId,
	)
	if err != nil {
		if errors.Is(err, userSessionModel.ErrNotFound) {
			return rejectSingleMessage(
				messageReasonNotSessionMember,
				messageCodeNotSessionMember,
			)
		}

		l.Errorf(
			"查询发送者会话关系失败，senderId=%d, sessionId=%d, err=%v",
			in.SenderId,
			in.SessionId,
			err,
		)
		return nil, status.Error(
			codes.Internal,
			"query sender session membership failed",
		)
	}

	if senderMember.Status != userSessionStatusNormal {
		return rejectSingleMessage(
			messageReasonNotSessionMember,
			messageCodeNotSessionMember,
		)
	}

	receiverMember, err := l.svcCtx.UserSessionModel.FindOne(
		l.ctx,
		in.ReceiverId,
		in.SessionId,
	)
	if err != nil {
		if errors.Is(err, userSessionModel.ErrNotFound) {
			return rejectSingleMessage(
				messageReasonNotSessionMember,
				messageCodeNotSessionMember,
			)
		}

		l.Errorf(
			"查询接收者会话关系失败，receiverId=%d, sessionId=%d, err=%v",
			in.ReceiverId,
			in.SessionId,
			err,
		)
		return nil, status.Error(
			codes.Internal,
			"query receiver session membership failed",
		)
	}

	if receiverMember.Status != userSessionStatusNormal {
		return rejectSingleMessage(
			messageReasonNotSessionMember,
			messageCodeNotSessionMember,
		)
	}

	senderRelation, err :=
		l.svcCtx.FriendModel.FindOneByUserIdFriendId(
			l.ctx,
			in.SenderId,
			in.ReceiverId,
		)
	if err != nil {
		if errors.Is(err, friendModel.ErrNotFound) {
			return rejectSingleMessage(
				messageReasonNotFriend,
				messageCodeNotFriend,
			)
		}

		l.Errorf(
			"查询发送者好友关系失败，senderId=%d, receiverId=%d, err=%v",
			in.SenderId,
			in.ReceiverId,
			err,
		)
		return nil, status.Error(
			codes.Internal,
			"query sender friend relation failed",
		)
	}
	switch senderRelation.Status {
	case friendStatusNormal:
		// 继续校验反向好友关系。
	case friendStatusBlocked:
		return rejectSingleMessage(
			messageReasonBlockedBySender,
			messageCodeBlockedBySender,
		)
	case friendStatusDeleted:
		return rejectSingleMessage(
			messageReasonFriendDeleted,
			messageCodeFriendDeleted,
		)
	default:
		return rejectSingleMessage(
			messageReasonNotFriend,
			messageCodeNotFriend,
		)
	}
	receiverRelation, err :=
		l.svcCtx.FriendModel.FindOneByUserIdFriendId(
			l.ctx,
			in.ReceiverId,
			in.SenderId,
		)
	if err != nil {
		if errors.Is(err, friendModel.ErrNotFound) {
			return rejectSingleMessage(
				messageReasonNotFriend,
				messageCodeNotFriend,
			)
		}

		l.Errorf(
			"查询接收者好友关系失败，senderId=%d, receiverId=%d, err=%v",
			in.SenderId,
			in.ReceiverId,
			err,
		)
		return nil, status.Error(
			codes.Internal,
			"query receiver friend relation failed",
		)
	}

	switch receiverRelation.Status {
	case friendStatusNormal:
		// 所有校验通过。
	case friendStatusBlocked:
		return rejectSingleMessage(
			messageReasonBlockedByReceiver,
			messageCodeBlockedByReceiver,
		)
	case friendStatusDeleted:
		return rejectSingleMessage(
			messageReasonFriendDeleted,
			messageCodeFriendDeleted,
		)
	default:
		return rejectSingleMessage(
			messageReasonNotFriend,
			messageCodeNotFriend,
		)
	}

	return &user.MessageValidateResp{
		Allowed: true,
	}, nil
}
func rejectSingleMessage(
	reason string,
	code int32,
) (*user.MessageValidateResp, error) {
	return &user.MessageValidateResp{
		Allowed:      false,
		RejectReason: reason,
		ErrorCode:    code,
	}, nil
}
