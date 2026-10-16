package mq

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// Publisher sends the payload already serialized in the Outbox row.
// A successful synchronous WriteMessages call means the broker acknowledged it.
type Publisher struct {
	writer *kafka.Writer
}

func NewPublisher(brokers []string) (*Publisher, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka brokers cannot be empty")
	}
	for _, broker := range brokers {
		if strings.TrimSpace(broker) == "" {
			return nil, fmt.Errorf("kafka broker cannot be empty")
		}
	}
	return &Publisher{writer: &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		// Worker synchronously publishes one event at a time; waiting for a
		// larger batch would add the default one-second delay to every event.
		BatchSize:              1,
		MaxAttempts:            5,
		ReadTimeout:            10 * time.Second,
		WriteTimeout:           10 * time.Second,
		AllowAutoTopicCreation: false,
	}}, nil
}

func (p *Publisher) Publish(ctx context.Context, topic, messageKey, payload string) error {
	if strings.TrimSpace(topic) == "" || strings.TrimSpace(messageKey) == "" {
		return fmt.Errorf("outbox topic and message key are required")
	}
	if err := p.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(messageKey),
		Value: []byte(payload),
	}); err != nil {
		return fmt.Errorf("publish outbox event to %s: %w", topic, err)
	}
	return nil
}

func (p *Publisher) Close() error {
	return p.writer.Close()
}
