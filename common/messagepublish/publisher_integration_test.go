//go:build integration

package messagepublish

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	messageevent "infinitechatgo/common/model/event"
)

func TestPublishCommittedMessageToIsolatedKafka(t *testing.T) {
	address := os.Getenv("IM_ISOLATED_KAFKA_ADDR")
	if address == "" {
		t.Skip("isolated Kafka not configured")
	}
	const topic = "im-best-effort-integration"
	p := New([]string{address}, topic)
	if !p.TryPublish(messageevent.ChatMessage{MessageID: 101, SessionID: 99, Seq: 7, SenderID: 8, ClientMessageID: "test", CreatedAt: 1}, "fingerprint") {
		t.Fatal("queue full")
	}
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{address}, Topic: topic, Partition: 0, StartOffset: kafka.FirstOffset})
	defer reader.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	record, err := reader.ReadMessage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var event messageevent.ChatMessagePersistedEvent
	if err := json.Unmarshal(record.Value, &event); err != nil {
		t.Fatal(err)
	}
	if string(record.Key) != "99" || event.Message.MessageID != 101 || event.Message.Seq != 7 {
		t.Fatalf("wrong event: %+v key=%s", event, record.Key)
	}
}
