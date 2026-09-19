package protocol

import (
	messageevent "infinitechatgo/common/model/event"
	"time"
)

// client<->realtime server
const (
	EventTypeMessageSend  = "message.send"
	EventTypeMessageAck   = "message.ack"
	EventTypeMessageError = "message.error"
	EventTypeMessageNew   = "message.new"
)

type MessageAckStatus string

const (
	MessageAckStatusAccepted MessageAckStatus = "accepted"
	MessageAckStatusStored   MessageAckStatus = "stored"
)

type MessageErrorStatus string

const (
	MessageErrorStatusRejected    MessageErrorStatus = "rejected"
	MessageErrorStatusUnknown     MessageErrorStatus = "unknown"
	MessageErrorStatusUnavailable MessageErrorStatus = "unavailable"
)

// 客户端发送的消息请求，需经过校验
type SendMessageRequest struct {
	EventType       string `json:"eventType"`
	ClientMessageID string `json:"clientMessageId"`

	SessionID   int64                    `json:"sessionId,string"`
	SessionType messageevent.SessionType `json:"sessionType"`

	ReceiverID *int64 `json:"receiverId,string,omitempty"`

	MessageType messageevent.MessageType `json:"messageType"`
	Body        messageevent.MessageBody `json:"body"`
}

type MessageAck struct {
	EventType string `json:"eventType"`

	ClientMessageID string `json:"clientMessageId"`
	MessageID       int64  `json:"messageId,string"`

	Status MessageAckStatus `json:"status"`

	ServerTime int64 `json:"serverTime"`
}

type MessageErrorResponse struct {
	EventType string `json:"eventType"`

	ClientMessageID string `json:"clientMessageId,omitempty"`  //防止客户端上传了非法的JSON，导致无法解析clientMessageId字段
	MessageID       *int64 `json:"messageId,string,omitempty"` //使用指针表达该字段存在与否

	Status MessageErrorStatus `json:"status"`

	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`

	ServerTime int64 `json:"serverTime"`
}
type NewMessageResponse struct {
	EventType string `json:"eventType"`

	Message messageevent.ChatMessage `json:"message"`
}

func NewAcceptedAck(
	clientMessageID string,
	messageID int64,
) MessageAck {
	return MessageAck{
		EventType:       EventTypeMessageAck,
		ClientMessageID: clientMessageID,
		MessageID:       messageID,
		Status:          MessageAckStatusAccepted,
		ServerTime:      time.Now().UnixMilli(),
	}
}
func NewStoredAck(
	clientMessageID string,
	messageID int64,
) MessageAck {
	return MessageAck{
		EventType:       EventTypeMessageAck,
		ClientMessageID: clientMessageID,
		MessageID:       messageID,
		Status:          MessageAckStatusStored,
		ServerTime:      time.Now().UnixMilli(),
	}
}
func NewRejectedError(
	clientMessageID string,
	code string,
	message string,
) MessageErrorResponse {
	return MessageErrorResponse{
		EventType:       EventTypeMessageError,
		ClientMessageID: clientMessageID,
		Status:          MessageErrorStatusRejected,
		Code:            code,
		Message:         message,
		Retryable:       false,
		ServerTime:      time.Now().UnixMilli(),
	}
}

func NewUnknownError(
	clientMessageID string,
	messageID int64,
	code string,
	message string,
) MessageErrorResponse {
	return MessageErrorResponse{
		EventType:       EventTypeMessageError,
		ClientMessageID: clientMessageID,
		MessageID:       &messageID,
		Status:          MessageErrorStatusUnknown,
		Code:            code,
		Message:         message,
		Retryable:       true,
		ServerTime:      time.Now().UnixMilli(),
	}
}

func NewUnavailableError(
	clientMessageID string,
	code string,
	message string,
) MessageErrorResponse {
	return MessageErrorResponse{
		EventType:       EventTypeMessageError,
		ClientMessageID: clientMessageID,
		Status:          MessageErrorStatusUnavailable,
		Code:            code,
		Message:         message,
		Retryable:       true,
		ServerTime:      time.Now().UnixMilli(),
	}
}

func NewMessage(
	message messageevent.ChatMessage,
) NewMessageResponse {
	return NewMessageResponse{
		EventType: EventTypeMessageNew,
		Message:   message,
	}
}
