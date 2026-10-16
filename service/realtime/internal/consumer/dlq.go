package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// DeadLetter records the original Kafka location so a repeated DLQ publish
// can be recognized after an uncertain ACK or an offset commit failure.
type DeadLetter struct {
	ID            string    `json:"id"`
	ConsumerGroup string    `json:"consumerGroup"`
	Topic         string    `json:"topic"`
	Partition     int       `json:"partition"`
	Offset        int64     `json:"offset"`
	EventID       string    `json:"eventId,omitempty"`
	Key           []byte    `json:"key"`
	Value         []byte    `json:"value"`
	ErrorType     string    `json:"errorType"`
	ErrorMessage  string    `json:"errorMessage"`
	FailedAt      time.Time `json:"failedAt"`
}

type DeadLetterPublisher interface {
	Publish(ctx context.Context, record DeadLetter) error
}

type KafkaDeadLetterPublisher struct {
	writer *kafka.Writer
}

func NewKafkaDeadLetterPublisher(brokers []string, topic string) (*KafkaDeadLetterPublisher, error) {
	if len(brokers) == 0 || strings.TrimSpace(topic) == "" {
		return nil, fmt.Errorf("DLQ brokers and topic are required")
	}
	for _, broker := range brokers {
		if strings.TrimSpace(broker) == "" {
			return nil, fmt.Errorf("DLQ broker cannot be empty")
		}
	}
	return &KafkaDeadLetterPublisher{writer: &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		Async:                  false,
		MaxAttempts:            5,
		ReadTimeout:            10 * time.Second,
		WriteTimeout:           10 * time.Second,
		AllowAutoTopicCreation: false,
	}}, nil
}

func (p *KafkaDeadLetterPublisher) Publish(ctx context.Context, record DeadLetter) error {
	if p == nil || p.writer == nil {
		return fmt.Errorf("DLQ publisher is unavailable")
	}
	body, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode DLQ record: %w", err)
	}
	if err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(record.ID),
		Value: body,
	}); err != nil {
		return fmt.Errorf("publish DLQ record %s: %w", record.ID, err)
	}
	return nil
}

func (p *KafkaDeadLetterPublisher) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}

func newDeadLetter(group string, message kafka.Message, failure *PermanentError) DeadLetter {
	var eventID string
	var envelope struct {
		EventID json.RawMessage `json:"eventId"`
	}
	if json.Unmarshal(message.Value, &envelope) == nil && len(envelope.EventID) > 0 {
		if json.Unmarshal(envelope.EventID, &eventID) != nil {
			eventID = string(envelope.EventID)
		}
	}
	return DeadLetter{
		ID:            fmt.Sprintf("%s/%s/%d/%d", group, message.Topic, message.Partition, message.Offset),
		ConsumerGroup: group,
		Topic:         message.Topic,
		Partition:     message.Partition,
		Offset:        message.Offset,
		EventID:       eventID,
		Key:           append([]byte(nil), message.Key...),
		Value:         append([]byte(nil), message.Value...),
		ErrorType:     failure.Reason,
		ErrorMessage:  failure.Error(),
		FailedAt:      time.Now().UTC(),
	}
}
