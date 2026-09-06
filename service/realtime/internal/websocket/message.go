package websocket

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"infinitechatgo/service/user/rpc/user"
	"infinitechatgo/service/user/rpc/userrpc"
)

const (
	messageTypeGroup  = "group"
	messageTypeSingle = "single"
	messageTypeAck    = "ack"
	messageTypeError  = "error"
)

type MessageRequest struct {
	Type            string `json:"type"`
	ClientMessageID string `json:"clientMessageId"`
	SessionID       int64  `json:"sessionId"`
	ReceiverID      int64  `json:"receiverId"`
	Content         string `json:"content"`
}

type MessageResponse struct {
	Type            string `json:"type"`
	ClientMessageID string `json:"clientMessageId"`
	SessionID       int64  `json:"sessionId"`
	SenderID        int64  `json:"senderId"`
	ReceiverID      int64  `json:"receiverId"`
	Content         string `json:"content"`
	SentAt          int64  `json:"sentAt"`
}

type MessageAck struct {
	Type              string `json:"type"`
	ClientMessageID   string `json:"clientMessageId"`
	RoutedConnections int    `json:"routedConnections"`
	SentAt            int64  `json:"sentAt"`
}

type ErrorResponse struct {
	Type            string `json:"type"`
	ClientMessageID string `json:"clientMessageId,omitempty"`
	Code            string `json:"code"`
	Message         string `json:"message"`
}

func handleMessage(
	manager *Manager,
	sender *Client,
	message []byte,
	userRPC userrpc.UserRpc,
) {
	var request MessageRequest
	if err := json.Unmarshal(message, &request); err != nil {
		sendError(
			sender,
			"",
			"invalid_json",
			"消息不是合法的JSON",
		)
		return
	}

	if request.ClientMessageID == "" ||
		request.SessionID <= 0 ||
		request.Content == "" {
		sendError(
			sender,
			request.ClientMessageID,
			"invalid_message",
			"消息参数不完整",
		)
		return
	}

	switch request.Type {
	case messageTypeSingle:
		handleSingleMessage(
			manager,
			sender,
			&request,
			userRPC,
		)

	case messageTypeGroup:
		handleGroupMessage(
			manager,
			sender,
			&request,
			userRPC,
		)

	default:
		sendError(
			sender,
			request.ClientMessageID,
			"unsupported_type",
			"不支持的消息类型",
		)
	}
}

func handleSingleMessage(
	manager *Manager,
	sender *Client,
	request *MessageRequest,
	userRPC userrpc.UserRpc,
) {
	if request.ReceiverID <= 0 {
		sendError(
			sender,
			request.ClientMessageID,
			"invalid_receiver",
			"接收者ID不正确",
		)
		return
	}

	if request.ReceiverID == sender.userID {
		sendError(
			sender,
			request.ClientMessageID,
			"invalid_receiver",
			"不能给自己发送单聊消息",
		)
		return
	}
	//context注入的时机？
	validationCtx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)
	defer cancel()

	validation, err := userRPC.ValidateSingleMessage(
		validationCtx,
		&userrpc.ValidateSingleMessageReq{
			SenderId:   sender.userID,
			ReceiverId: request.ReceiverID,
			SessionId:  request.SessionID,
		},
	)
	if err != nil {
		log.Printf(
			"validate single message failed: %v",
			err,
		)
		sendError(
			sender,
			request.ClientMessageID,
			"validation_unavailable",
			"消息校验服务暂时不可用",
		)
		return
	}

	if validation == nil || !validation.Allowed {
		message := "当前不允许发送这条消息"
		if validation != nil && validation.RejectReason != "" {
			message = validation.RejectReason
		}
		sendError(
			sender,
			request.ClientMessageID,
			"message_rejected",
			message,
		)
		return
	}

	sentAt := time.Now().UnixMilli()
	response := MessageResponse{
		Type:            messageTypeSingle,
		ClientMessageID: request.ClientMessageID,
		SessionID:       request.SessionID,
		SenderID:        sender.userID,
		ReceiverID:      request.ReceiverID,
		Content:         request.Content,
		SentAt:          sentAt,
	}
	responseJSON, err := json.Marshal(response)
	if err != nil {
		log.Printf(
			"marshal message response failed: %v",
			err,
		)
		sendError(
			sender,
			request.ClientMessageID,
			"internal_error",
			"系统错误",
		)
		return
	}

	sentCount := manager.sendToUser(
		request.ReceiverID,
		responseJSON,
	)
	if sentCount == 0 {
		sendError(
			sender,
			request.ClientMessageID,
			"receiver_offline",
			"接收者当前不在线",
		)
		return
	}

	sendJSON(
		sender,
		MessageAck{
			Type:              messageTypeAck,
			ClientMessageID:   request.ClientMessageID,
			RoutedConnections: sentCount,
			SentAt:            sentAt,
		},
	)

	log.Printf(
		"message routed: senderId=%d, receiverId=%d, connections=%d",
		sender.userID,
		request.ReceiverID,
		sentCount,
	)
}
func handleGroupMessage(
	manager *Manager,
	sender *Client,
	request *MessageRequest,
	userRPC userrpc.UserRpc,
) {
	rpcCtx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)
	defer cancel()
	//校验发送者和群聊会话状态
	validation, err := userRPC.ValidateGroupMessage(
		rpcCtx,
		&userrpc.ValidateGroupMessageReq{
			SenderId:  sender.userID,
			SessionId: request.SessionID,
		},
	)
	if err != nil {
		log.Printf(
			"validate group message failed:%v",
			err,
		)
		sendError(
			sender,
			request.ClientMessageID,
			"validation_unavailable",
			"消息校验服务暂时不可用",
		)
		return
	}
	if validation == nil || !validation.Allowed {
		message := "当前不允许发送这条群聊消息"
		if validation != nil &&
			validation.RejectReason != "" {
			message = validation.RejectReason
		}

		sendError(
			sender,
			request.ClientMessageID,
			"message_rejected",
			message,
		)
		return
	}
	//查询该群聊当前所有的有效成员
	members, err := userRPC.GetGroupMemberIds(
		rpcCtx,
		&user.SessionIdReq{
			SessionId: request.SessionID,
		},
	)
	if err != nil {
		log.Printf(
			"get group member ids failed: %v",
			err,
		)
		sendError(
			sender,
			request.ClientMessageID,
			"member_service_unavailable",
			"暂时无法获取群成员",
		)
		return
	}
	if members == nil || len(members.UserIds) == 0 {
		sendError(
			sender,
			request.ClientMessageID,
			"empty_group",
			"群聊中没有有效成员",
		)
		return
	}
	// 3. 构造发给群成员的消息。
	sentAt := time.Now().UnixMilli()
	response := MessageResponse{
		Type:            messageTypeGroup,
		ClientMessageID: request.ClientMessageID,
		SessionID:       request.SessionID,
		SenderID:        sender.userID,
		Content:         request.Content,
		SentAt:          sentAt,
	}

	responseJSON, err := json.Marshal(response)
	if err != nil {
		log.Printf(
			"marshal group message failed: %v",
			err,
		)
		sendError(
			sender,
			request.ClientMessageID,
			"internal_error",
			"系统错误",
		)
		return
	}
	//向除了发送者以外的所有在线群成员转发
	routedConnections := 0
	for _, memberID := range members.UserIds {
		if memberID == sender.userID {
			continue
		}
		routedConnections += manager.sendToUser(
			memberID,
			responseJSON,
		)
	}
	// 5. 返回当前转发结果。
	sendJSON(
		sender,
		MessageAck{
			Type:              messageTypeAck,
			ClientMessageID:   request.ClientMessageID,
			RoutedConnections: routedConnections,
			SentAt:            sentAt,
		},
	)

	log.Printf(
		"group message routed: senderId=%d, sessionId=%d, connections=%d",
		sender.userID,
		request.SessionID,
		routedConnections,
	)
}
func sendError(
	client *Client,
	clientMessageID string,
	code string,
	message string,
) {
	sendJSON(
		client,
		ErrorResponse{
			Type:            messageTypeError,
			ClientMessageID: clientMessageID,
			Code:            code,
			Message:         message,
		},
	)
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
