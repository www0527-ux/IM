package messageidentity

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	messageevent "infinitechatgo/common/model/event"
)

// payload contains only immutable business fields. MessageID, Seq,
// ClientMessageID and timestamps are deliberately excluded: they identify or
// order a request, but do not describe what the user intended to send.
type payload struct {
	SenderID             int64                    `json:"senderId,string"`
	SessionID            int64                    `json:"sessionId,string"`
	SessionType          messageevent.SessionType `json:"sessionType"`
	ReceiverID           *int64                   `json:"receiverId,string,omitempty"`
	MessageType          messageevent.MessageType `json:"messageType"`
	Content              string                   `json:"content"`
	ReplyID              *int64                   `json:"replyId,string,omitempty"`
	RedPacketID          *int64                   `json:"redPacketId,string,omitempty"`
	RedPacketWrapperText string                   `json:"redPacketWrapperText,omitempty"`
}

// Fingerprint returns a stable SHA-256 digest for semantic idempotency checks.
// It is not an authentication signature.
func Fingerprint(message messageevent.ChatMessage) (string, error) {
	data, err := json.Marshal(payload{
		SenderID:             message.SenderID,
		SessionID:            message.SessionID,
		SessionType:          message.SessionType,
		ReceiverID:           cloneInt64(message.ReceiverID),
		MessageType:          message.MessageType,
		Content:              message.Body.Content,
		ReplyID:              cloneInt64(message.Body.ReplyID),
		RedPacketID:          cloneInt64(message.Body.RedPacketID),
		RedPacketWrapperText: message.Body.RedPacketWrapperText,
	})
	if err != nil {
		return "", fmt.Errorf("marshal message fingerprint payload: %w", err)
	}

	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}
