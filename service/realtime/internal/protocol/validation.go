package protocol

import (
	"fmt"
	"strings"
	"unicode/utf8"

	messageevent "infinitechatgo/common/model/event"
)

const (
	maxClientMessageIDLength = 64
	maxTextMessageLength     = 4000
)

type ValidationError struct {
	Code    string
	Message string
}

func (e ValidationError) Error() string {
	return e.Message
}
func ValidateSendMessageRequest(
	request *SendMessageRequest,
	senderID int64,
) error {
	if request == nil {
		return newValidationError(
			"invalid_message",
			"消息请求不能为空",
		)
	}

	if request.EventType != EventTypeMessageSend {
		return newValidationError(
			"invalid_event_type",
			"消息事件类型不正确",
		)
	}
	clientMessageID := strings.TrimSpace(
		request.ClientMessageID,
	)
	if clientMessageID == "" ||
		clientMessageID != request.ClientMessageID ||
		len(clientMessageID) > maxClientMessageIDLength {
		return newValidationError(
			"invalid_client_message_id",
			"客户端消息ID不正确",
		)
	}
	if request.SessionID <= 0 {
		return newValidationError(
			"invalid_session",
			"会话ID不正确",
		)
	}
	switch request.SessionType {
	case messageevent.SessionTypeSingle:
		if request.ReceiverID == nil ||
			*request.ReceiverID <= 0 {
			return newValidationError(
				"invalid_receiver",
				"单聊消息必须指定接收者",
			)
		}

		if *request.ReceiverID == senderID {
			return newValidationError(
				"invalid_receiver",
				"不能给自己发送单聊消息",
			)
		}

	case messageevent.SessionTypeGroup:
		if request.ReceiverID != nil {
			return newValidationError(
				"invalid_receiver",
				"群聊消息不能指定单个接收者",
			)
		}

	default:
		return newValidationError(
			"unsupported_session_type",
			"暂不支持该会话类型",
		)
	}
	if request.MessageType != messageevent.MessageTypeText {
		return newValidationError(
			"unsupported_message_type",
			"当前只支持文本消息",
		)
	}
	content := strings.TrimSpace(request.Body.Content)
	if content == "" {
		return newValidationError(
			"invalid_content",
			"消息内容不能为空",
		)
	}
	if utf8.RuneCountInString(
		request.Body.Content,
	) > maxTextMessageLength {
		return newValidationError(
			"message_too_long",
			fmt.Sprintf(
				"文本消息不能超过%d个字符",
				maxTextMessageLength,
			),
		)
	}

	if request.Body.ReplyID != nil &&
		*request.Body.ReplyID <= 0 {
		return newValidationError(
			"invalid_reply_id",
			"引用消息ID不正确",
		)
	}

	return nil
}
func newValidationError(
	code string,
	message string,
) ValidationError {
	return ValidationError{
		Code:    code,
		Message: message,
	}
}
