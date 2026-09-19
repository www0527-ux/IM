package protocol

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	messageevent "infinitechatgo/common/model/event"
)

// fingerprintPayload contains normalized business fields. Hashing decoded
// fields avoids differences caused by whitespace or JSON field order.
type fingerprintPayload struct {
	SenderID    int64                    `json:"senderId,string"`
	SessionID   int64                    `json:"sessionId,string"`
	SessionType messageevent.SessionType `json:"sessionType"`
	ReceiverID  *int64                   `json:"receiverId,string,omitempty"`
	MessageType messageevent.MessageType `json:"messageType"`
	Content     string                   `json:"content"`
	ReplyID     *int64                   `json:"replyId,string,omitempty"`
}

func FingerprintSendMessageRequest(
	request SendMessageRequest,
	senderID int64,
) (string, error) {
	payload := fingerprintPayload{
		SenderID:    senderID,
		SessionID:   request.SessionID,
		SessionType: request.SessionType,
		ReceiverID:  cloneInt64Pointer(request.ReceiverID),
		MessageType: request.MessageType,
		Content:     request.Body.Content,
		ReplyID:     cloneInt64Pointer(request.Body.ReplyID),
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal message fingerprint payload: %w", err)
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}
