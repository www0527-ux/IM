package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	messageevent "infinitechatgo/common/model/event"
	messageapplication "infinitechatgo/service/message/application"
	"infinitechatgo/service/realtime/internal/protocol"
	"infinitechatgo/service/realtime/internal/svc"
	"infinitechatgo/service/user/rpc/userrpc"
)

const (
	dependencyTimeout = 3 * time.Second
	writeTimeout      = 10 * time.Second
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

	writeCtx, cancelWrite := context.WithTimeout(context.Background(), writeTimeout)
	result, writeErr := svcCtx.MessageWriter.Send(
		writeCtx,
		messageapplication.Command{
			Source:      messageapplication.SourceRealtime,
			Message:     protocol.BuildChatMessage(request, sender.userID, 0, 0),
			Fingerprint: fingerprint,
		},
	)
	cancelWrite()
	if writeErr != nil {
		switch {
		case errors.Is(writeErr, messageapplication.ErrIdempotencyConflict):
			sendJSON(sender, protocol.NewRejectedError(
				request.ClientMessageID,
				"idempotency_conflict",
				"该clientMessageId已被当前账号用于另一条不同的消息请求",
			))
		case errors.Is(writeErr, messageapplication.ErrInvalidCommand):
			sendJSON(sender, protocol.NewRejectedError(
				request.ClientMessageID,
				"invalid_message",
				"消息参数不正确",
			))
		default:
			// TODO: distinguish retryable failures from unknown COMMIT results.
			log.Printf("write message through outbox pipeline failed: %v", writeErr)
			sendJSON(sender, protocol.NewUnavailableError(
				request.ClientMessageID,
				"message_write_unavailable",
				"消息持久化服务尚未就绪或暂时不可用",
			))
		}
		return
	}
	sendJSON(
		sender,
		protocol.NewStoredAck(
			request.ClientMessageID,
			result.Message.MessageID,
			result.Message.Seq,
		),
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
