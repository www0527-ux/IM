package messagepublish

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"infinitechatgo/common/messagetrace"
	messageevent "infinitechatgo/common/model/event"
)

// Publisher sends committed message snapshots on a best-effort basis.
// A publish error never changes the already committed database result.
type Publisher struct {
	writer batchWriter
	queue  chan record
	config Config
	mu     sync.RWMutex
	closed bool
	done   chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
}

type batchWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
	Close() error
}
type record struct {
	message     messageevent.ChatMessage
	fingerprint string
}

func New(brokers []string, topic string) *Publisher {
	p, err := NewWithConfig(brokers, topic, Config{})
	if err != nil {
		panic(err)
	} // defaults are compile-time valid
	return p
}

func NewWithConfig(brokers []string, topic string, config Config) (*Publisher, error) {
	config, err := config.WithDefaults()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Publisher{writer: &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic,
		Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, BatchSize: config.BatchSize,
		BatchTimeout:           time.Duration(config.FlushMillis) * time.Millisecond,
		AllowAutoTopicCreation: false}, queue: make(chan record, config.QueueCapacity), config: config,
		done: make(chan struct{}), ctx: ctx, cancel: cancel}
	go p.run()
	return p, nil
}

// TryPublish never blocks a committed transaction or its response. A full
// queue or failed Kafka publish is recovered only by client synchronization.
func (p *Publisher) TryPublish(message messageevent.ChatMessage, fingerprint string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return false
	}
	messagetrace.Message(message, "publish_enqueue")
	select {
	case p.queue <- record{message, fingerprint}:
		return true
	default:
		return false
	}
}

func (p *Publisher) run() {
	defer close(p.done)
	defer p.writer.Close()
	for first := range p.queue {
		items := []record{first}
		messagetrace.Message(first.message, "publish_dequeue")
		timer := time.NewTimer(time.Duration(p.config.FlushMillis) * time.Millisecond)
	collect:
		for len(items) < p.config.BatchSize {
			select {
			case item, ok := <-p.queue:
				if !ok {
					break collect
				}
				items = append(items, item)
				messagetrace.Message(item.message, "publish_dequeue")
			case <-timer.C:
				break collect
			}
		}
		timer.Stop()
		ctx, cancel := context.WithTimeout(p.ctx, 3*time.Second)
		err := p.publishBatch(ctx, items)
		cancel()
		if err != nil {
			log.Printf("best-effort message publish failed batchSize=%d firstMessageId=%d: %v", len(items), first.message.MessageID, err)
		}
	}
}

func (p *Publisher) publish(ctx context.Context, message messageevent.ChatMessage, fingerprint string) error {
	return p.publishBatch(ctx, []record{{message: message, fingerprint: fingerprint}})
}

func (p *Publisher) publishBatch(ctx context.Context, items []record) error {
	messages := make([]kafka.Message, 0, len(items))
	for _, item := range items {
		message := item.message
		event := messageevent.NewChatMessagePersistedEventWithIdentity(message.MessageID, message, item.fingerprint, message.CreatedAt)
		payload, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("encode persisted message: %w", err)
		}
		messages = append(messages, kafka.Message{Key: []byte(strconv.FormatInt(message.SessionID, 10)), Value: payload})
	}
	for _, item := range items {
		messagetrace.Message(item.message, "kafka_write_start")
	}
	err := p.writer.WriteMessages(ctx, messages...)
	for _, item := range items {
		messagetrace.Message(item.message, "kafka_write_end")
	}
	return err
}

// Close stops admission and drains the bounded queue. On deadline, outstanding
// notifications are canceled; committed MySQL messages remain sync-readable.
func (p *Publisher) Close(ctx context.Context) error {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.queue)
	}
	p.mu.Unlock()
	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		p.cancel()
		return ctx.Err()
	}
}
