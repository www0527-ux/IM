package messagepublish

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	messageevent "infinitechatgo/common/model/event"
)

func TestTryPublishDoesNotBlockWhenQueueIsFull(t *testing.T) {
	p := &Publisher{queue: make(chan record, 1)}
	if !p.TryPublish(messageevent.ChatMessage{MessageID: 1}, "a") {
		t.Fatal("first message was not queued")
	}
	if p.TryPublish(messageevent.ChatMessage{MessageID: 2}, "b") {
		t.Fatal("full queue accepted message")
	}
}

func TestPublishReturnsErrorWhenKafkaUnavailable(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	p := &Publisher{writer: &kafka.Writer{Addr: kafka.TCP(address), Topic: "unavailable", Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, BatchSize: 1, MaxAttempts: 1}}
	defer p.writer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.publish(ctx, messageevent.ChatMessage{MessageID: 1, SessionID: 2}, "fingerprint"); err == nil {
		t.Fatal("expected Kafka publish failure")
	}
}
