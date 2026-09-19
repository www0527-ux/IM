package protocol

import (
	messageevent "infinitechatgo/common/model/event"
)

func BuildChatMessage(
	request SendMessageRequest,
	senderID int64,
	messageID int64,
	createdAt int64,
) messageevent.ChatMessage {
	return messageevent.ChatMessage{
		MessageID:       messageID,
		ClientMessageID: request.ClientMessageID,
		SessionID:       request.SessionID,
		SessionType:     request.SessionType,
		SenderID:        senderID,
		ReceiverID:      cloneInt64Pointer(request.ReceiverID),
		MessageType:     request.MessageType,
		Body: messageevent.MessageBody{
			Content: request.Body.Content,
			ReplyID: cloneInt64Pointer(
				request.Body.ReplyID,
			),
		},
		CreatedAt: createdAt,
	}
}

// 为ID创建独立的副本
func cloneInt64Pointer(value *int64) *int64 {
	if value == nil {
		return nil
	}

	copiedValue := *value
	return &copiedValue
}
