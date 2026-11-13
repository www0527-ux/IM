package messagepublish

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	event "infinitechatgo/common/model/event"
)

type batchWriterFake struct {
	mu      sync.Mutex
	batches [][]kafka.Message
	err     error
}

func (w *batchWriterFake) WriteMessages(_ context.Context, m ...kafka.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.batches = append(w.batches, append([]kafka.Message(nil), m...))
	return w.err
}
func (w *batchWriterFake) Close() error { return nil }
func TestPublisherBatchesAndPreservesEnqueueOrder(t *testing.T) {
	w := &batchWriterFake{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := &Publisher{writer: w, queue: make(chan record, 8), config: Config{BatchSize: 3, FlushMillis: 1}, done: make(chan struct{}), ctx: ctx, cancel: cancel}
	for i := int64(1); i <= 5; i++ {
		if !p.TryPublish(event.ChatMessage{MessageID: i, SessionID: 7, Seq: i, SenderID: 9, ClientMessageID: "test"}, "fingerprint") {
			t.Fatal("queue rejected")
		}
	}
	go p.run()
	closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
	defer closeCancel()
	if err := p.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	if len(w.batches) != 2 || len(w.batches[0]) != 3 || len(w.batches[1]) != 2 {
		t.Fatalf("batches=%v", w.batches)
	}
	var last int64
	for _, batch := range w.batches {
		for _, m := range batch {
			var value event.ChatMessagePersistedEvent
			if err := json.Unmarshal(m.Value, &value); err != nil {
				t.Fatal(err)
			}
			last++
			if value.Message.Seq != last || string(m.Key) != "7" {
				t.Fatalf("key=%s message=%+v", m.Key, value.Message)
			}
		}
	}
	if p.TryPublish(event.ChatMessage{}, "") {
		t.Fatal("closed publisher accepted message")
	}
}
func TestPublisherBatchFailureDoesNotReenqueueWholeBatch(t *testing.T) {
	w := &batchWriterFake{err: errors.New("partial or unknown broker result")}
	p := &Publisher{writer: w}
	err := p.publishBatch(context.Background(), []record{{message: event.ChatMessage{MessageID: 1, SessionID: 7}}, {message: event.ChatMessage{MessageID: 2, SessionID: 7}}})
	if err == nil || len(w.batches) != 1 || len(w.batches[0]) != 2 {
		t.Fatalf("err=%v batches=%v", err, w.batches)
	}
}
