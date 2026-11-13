//go:build integration

package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"infinitechatgo/common/kafkareader"
	"infinitechatgo/common/messagepublish"
	event "infinitechatgo/common/model/event"
)

type replayCommitReader struct {
	*kafkareader.Reader
	failOnce  bool
	committed chan int64
}

func (r *replayCommitReader) CommitMessages(ctx context.Context, messages ...kafka.Message) error {
	if r.failOnce {
		r.failOnce = false
		return errors.New("injected commit failure before broker call")
	}
	if err := r.Reader.CommitMessages(ctx, messages...); err != nil {
		return err
	}
	for _, m := range messages {
		var value event.ChatMessagePersistedEvent
		if json.Unmarshal(m.Value, &value) == nil {
			select {
			case r.committed <- value.Message.Seq:
			default:
			}
		}
	}
	return nil
}

func TestRealKafkaBatchRestartReplaysAndCommitRetryDoesNotRepeatBusiness(t *testing.T) {
	addr, topic := os.Getenv("IM_PIPELINE_TEST_KAFKA"), os.Getenv("IM_PIPELINE_TEST_TOPIC")
	if addr == "" || !strings.HasPrefix(topic, "im-push-opt-") {
		t.Skip("isolated Kafka topic is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	publisher, err := messagepublish.NewWithConfig([]string{addr}, topic, messagepublish.Config{BatchSize: 8})
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 6; i++ {
		if !publisher.TryPublish(event.ChatMessage{MessageID: i, SessionID: 77, SenderID: 11, Seq: i, ClientMessageID: "replay-" + strconv.FormatInt(i, 10)}, "fixture") {
			t.Fatal("publisher queue rejected")
		}
	}
	if err := publisher.Close(ctx); err != nil {
		t.Fatal(err)
	}
	group := "push-batch-replay-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	newReader := func() *kafkareader.Reader {
		return kafkareader.New(kafka.ReaderConfig{Brokers: []string{addr}, Topic: topic, GroupID: group, StartOffset: kafka.FirstOffset, CommitInterval: 0})
	}
	first := newReader()
	var mu sync.Mutex
	seen := map[int64]int{}
	decode := func(value string) int64 {
		var e event.ChatMessagePersistedEvent
		if err := json.Unmarshal([]byte(value), &e); err != nil {
			t.Error(err)
		}
		return e.Message.Seq
	}
	err = RunKafkaConsumerBatched(ctx, first, batchHandlerFunc(func(_ context.Context, _, value string) error {
		seq := decode(value)
		if seq == 2 {
			return &FatalError{Err: errors.New("simulate stop before pending batch commit")}
		}
		mu.Lock()
		seen[seq]++
		mu.Unlock()
		return nil
	}), group, &scriptedDLQ{}, BatchCommitConfig{BatchSize: 32, IntervalMillis: 1000})
	_ = first.Close()
	if err == nil {
		t.Fatal("first consumer did not stop at injected fault")
	}
	secondCtx, stopSecond := context.WithCancel(ctx)
	defer stopSecond()
	second := &replayCommitReader{Reader: newReader(), failOnce: true, committed: make(chan int64, 16)}
	defer second.Close()
	done := make(chan error, 1)
	go func() {
		done <- RunKafkaConsumerBatched(secondCtx, second, batchHandlerFunc(func(_ context.Context, _, value string) error {
			seq := decode(value)
			mu.Lock()
			seen[seq]++
			mu.Unlock()
			return nil
		}), group, &scriptedDLQ{}, BatchCommitConfig{BatchSize: 32, IntervalMillis: 10})
	}()
	for {
		select {
		case seq := <-second.committed:
			if seq != 6 {
				continue
			}
			stopSecond()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			if seen[1] != 2 {
				t.Fatalf("uncommitted successful prefix was not replayed: %v", seen)
			}
			for i := int64(2); i <= 6; i++ {
				if seen[i] != 1 {
					t.Fatalf("commit retry repeated or skipped business: %v", seen)
				}
			}
			return
		case err := <-done:
			t.Fatalf("second consumer stopped before committed tail: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}
