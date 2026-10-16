package consumer

import (
	"context"
	"encoding/json"
	"testing"

	"infinitechatgo/common/messagecache"
	"infinitechatgo/common/messageidentity"
	messageevent "infinitechatgo/common/model/event"
)

type recordingCache struct {
	values map[string]string
}

func (r *recordingCache) SetexCtx(
	_ context.Context,
	key string,
	value string,
	_ int,
) error {
	r.values[key] = value
	return nil
}

func TestMessageCacheConsumerWritesBothPersistedProjections(t *testing.T) {
	cache := &recordingCache{values: map[string]string{}}
	consumer, err := NewMessageCacheConsumer(cache, 60, 120)
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	event := messageevent.NewChatMessagePersistedEvent(
		messageevent.ChatMessage{
			MessageID:       10,
			ClientMessageID: "client-1",
			SessionID:       20,
			SessionType:     messageevent.SessionTypeGroup,
			Seq:             30,
			SenderID:        40,
			MessageType:     messageevent.MessageTypeText,
			Body:            messageevent.MessageBody{Content: "hello"},
		},
		50,
	)
	// The cache uses the committed message snapshot even when the legacy
	// fingerprint carried by the event is stale.
	event.Fingerprint = "stale"
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	if err := consumer.Consume(context.Background(), "20", string(payload)); err != nil {
		t.Fatalf("consume event: %v", err)
	}
	content := cache.values[messagecache.ContentKey(20, 30)]
	if content == "" {
		t.Fatal("content cache was not written")
	}
	var cachedMessage messageevent.ChatMessage
	if err := json.Unmarshal([]byte(content), &cachedMessage); err != nil {
		t.Fatalf("decode cached message: %v", err)
	}
	if cachedMessage.MessageID != event.Message.MessageID ||
		cachedMessage.SessionID != event.Message.SessionID ||
		cachedMessage.Seq != event.Message.Seq {
		t.Fatalf("cached message = %+v, want %+v", cachedMessage, event.Message)
	}
	var contentFields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(content), &contentFields); err != nil {
		t.Fatalf("decode cached message fields: %v", err)
	}
	if _, wrapped := contentFields["eventType"]; wrapped {
		t.Fatal("content cache still contains the event wrapper")
	}
	if cache.values[messagecache.IdempotencyKey(40, "client-1")] == "" {
		t.Fatal("idempotency cache was not written")
	}
	var entry messagecache.IdempotencyEntry
	if err := json.Unmarshal([]byte(cache.values[messagecache.IdempotencyKey(40, "client-1")]), &entry); err != nil {
		t.Fatalf("decode cached idempotency result: %v", err)
	}
	wantFingerprint, err := messageidentity.Fingerprint(event.Message)
	if err != nil {
		t.Fatalf("fingerprint message: %v", err)
	}
	if entry.Fingerprint != wantFingerprint {
		t.Fatalf("cached fingerprint = %q, want %q", entry.Fingerprint, wantFingerprint)
	}
}
