package messagecache

import (
	"fmt"
	"strconv"
)

const (
	contentKeyPrefix     = "im:message-cache:"
	idempotencyKeyPrefix = "im:message:idempotency:v3:"
)

type IdempotencyEntry struct {
	MessageID   int64  `json:"messageId,string"`
	Seq         int64  `json:"seq,string"`
	Fingerprint string `json:"fingerprint"`
}

func ContentKey(sessionID, seq int64) string {
	return fmt.Sprintf("%s{%d}:%d", contentKeyPrefix, sessionID, seq)
}

func IdempotencyKey(senderID int64, clientMessageID string) string {
	return idempotencyKeyPrefix +
		strconv.FormatInt(senderID, 10) + ":" + clientMessageID
}

func (entry IdempotencyEntry) Valid() bool {
	return entry.MessageID > 0 && entry.Seq > 0 && entry.Fingerprint != ""
}
