package consumer

import (
	"context"
	"encoding/json"
	"testing"

	"infinitechatgo/common/messagecache"
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
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	if err := consumer.Consume(context.Background(), "20", string(payload)); err != nil {
		t.Fatalf("consume event: %v", err)
	}
	if cache.values[messagecache.ContentKey(20, 30)] == "" {
		t.Fatal("content cache was not written")
	}
	if cache.values[messagecache.IdempotencyKey(40, "client-1")] == "" {
		t.Fatal("idempotency cache was not written")
	}
}
