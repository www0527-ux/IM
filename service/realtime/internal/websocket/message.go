package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	messageevent "infinitechatgo/common/model/event"
	"infinitechatgo/service/realtime/internal/messageid"
	"infinitechatgo/service/realtime/internal/protocol"
	"infinitechatgo/service/realtime/internal/svc"
	"infinitechatgo/service/user/rpc/userrpc"
)

const (
	dependencyTimeout = 3 * time.Second
	publishTimeout    = 10 * time.Second
)

func handleMessage(
	sender *Client,
	rawMessage []byte,
	svcCtx *svc.ServiceContext,
) {
	var request protocol.SendMessageRequest

	if err := json.Unmarshal(
		rawMessage,
		&request,
	); err != nil {
		sendJSON(
			sender,
			protocol.NewRejectedError(
				"",
				"invalid_json",
				"消息不是合法的JSON",
			),
		)
		return
	}

	if err := protocol.ValidateSendMessageRequest(
		&request,
		sender.userID,
	); err != nil {
		var validationErr protocol.ValidationError

		if errors.As(err, &validationErr) {
			sendJSON(
				sender,
				protocol.NewRejectedError(
					request.ClientMessageID,
					validationErr.Code,
					validationErr.Message,
				),
			)
			return
		}

		log.Printf(
			"validate message request failed: %v",
			err,
		)

		sendJSON(
			sender,
			protocol.NewRejectedError(
				request.ClientMessageID,
				"invalid_message",
				"消息参数不正确",
			),
		)
		return
	}

	allowed, rejectReason, err :=
		validateMessagePermission(
			sender.userID,
			&request,
			svcCtx.UserRPC,
		)
	if err != nil {
		log.Printf(
			"validate message permission failed: %v",
			err,
		)

		sendJSON(
			sender,
			protocol.NewUnavailableError(
				request.ClientMessageID,
				"validation_unavailable",
				"消息校验服务暂时不可用",
			),
		)
		return
	}

	if !allowed {
		if rejectReason == "" {
			rejectReason = "当前不允许发送这条消息"
		}

		sendJSON(
			sender,
			protocol.NewRejectedError(
				request.ClientMessageID,
				"message_rejected",
				rejectReason,
			),
		)
		return
	}

	fingerprint, err := protocol.FingerprintSendMessageRequest(
		request,
		sender.userID,
	)
	if err != nil {
		log.Printf("fingerprint message request failed: %v", err)
		sendJSON(
			sender,
			protocol.NewUnavailableError(
				request.ClientMessageID,
				"message_identity_unavailable",
				"消息身份服务暂时不可用",
			),
		)
		return
	}

	identityCtx, cancelIdentity :=
		context.WithTimeout(
			context.Background(),
			dependencyTimeout,
		)

	identity, err :=
		svcCtx.MessageIDResolver.GetOrCreate(
			identityCtx,
			sender.userID,
			request.ClientMessageID,
			fingerprint,
		)

	cancelIdentity()

	if err != nil {
		if errors.Is(err, messageid.ErrIdempotencyConflict) {
			sendJSON(
				sender,
				protocol.NewRejectedError(
					request.ClientMessageID,
					"idempotency_conflict",
					"同一个clientMessageId不能用于不同的消息内容",
				),
			)
			return
		}

		log.Printf(
			"get message identity failed: %v",
			err,
		)

		sendJSON(
			sender,
			protocol.NewUnavailableError(
				request.ClientMessageID,
				"idempotency_unavailable",
				"消息服务暂时不可用",
			),
		)
		return
	}

	message := protocol.BuildChatMessage(
		request,
		sender.userID,
		identity.MessageID,
		identity.CreatedAt,
	)

	event :=
		messageevent.NewChatMessageAcceptedEvent(
			message,
		)

	publishCtx, cancelPublish :=
		context.WithTimeout(
			context.Background(),
			publishTimeout,
		)

	err = svcCtx.AcceptedPublisher.Publish(
		publishCtx,
		event,
	)
	//释放定时器资源
	cancelPublish()

	if err != nil {
		log.Printf(
			"publish accepted message failed: messageId=%d, err=%v",
			message.MessageID,
			err,
		)
		sendJSON(
			sender,
			protocol.NewUnknownError(
				request.ClientMessageID,
				message.MessageID,
				"kafka_publish_unknown",
				"消息发送结果暂时无法确定，请使用相同ID重试",
			),
		)
		return
	}

	sendJSON(
		sender,
		protocol.NewAcceptedAck(
			request.ClientMessageID,
			message.MessageID,
		),
	)

	log.Printf(
		"message accepted: messageId=%d, sessionId=%d, senderId=%d",
		message.MessageID,
		message.SessionID,
		message.SenderID,
	)
}

func validateMessagePermission(
	senderID int64,
	request *protocol.SendMessageRequest,
	userRPC userrpc.UserRpc,
) (bool, string, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		dependencyTimeout,
	)
	defer cancel()

	switch request.SessionType {
	case messageevent.SessionTypeSingle:
		result, err := userRPC.ValidateSingleMessage(
			ctx,
			&userrpc.ValidateSingleMessageReq{
				SenderId:   senderID,
				ReceiverId: *request.ReceiverID,
				SessionId:  request.SessionID,
			},
		)
		if err != nil {
			return false, "", err
		}

		if result == nil {
			return false, "", fmt.Errorf(
				"single message validation returned nil",
			)
		}

		return result.Allowed,
			result.RejectReason,
			nil

	case messageevent.SessionTypeGroup:
		result, err := userRPC.ValidateGroupMessage(
			ctx,
			&userrpc.ValidateGroupMessageReq{
				SenderId:  senderID,
				SessionId: request.SessionID,
			},
		)
		if err != nil {
			return false, "", err
		}

		if result == nil {
			return false, "", fmt.Errorf(
				"group message validation returned nil",
			)
		}

		return result.Allowed,
			result.RejectReason,
			nil

	default:
		return false, "", fmt.Errorf(
			"unsupported session type: %d",
			request.SessionType,
		)
	}
}

func sendJSON(
	client *Client,
	value any,
) {
	data, err := json.Marshal(value)
	if err != nil {
		log.Printf(
			"marshal websocket response failed: %v",
			err,
		)
		return
	}

	client.sendMessage(data)
}
