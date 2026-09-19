package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	messageevent "infinitechatgo/common/model/event"
	"infinitechatgo/service/transfer/internal/config"

	"github.com/segmentio/kafka-go"
)

type SequencedMessagePublisher struct {
	writer *kafka.Writer
}

func NewSequencedMessagePublisher(
	c config.KafkaConfig,
) (*SequencedMessagePublisher, error) {
	if len(c.Brokers) == 0 {
		return nil, fmt.Errorf("kafka brokers cannot be empty")
	}
	if strings.TrimSpace(c.SequencedTopic) == "" {
		return nil, fmt.Errorf("kafka sequenced topic cannot be empty")
	}

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(c.Brokers...),
		Topic:                  c.SequencedTopic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		Async:                  false,
		MaxAttempts:            5,
		ReadTimeout:            10 * time.Second,
		WriteTimeout:           10 * time.Second,
		AllowAutoTopicCreation: false,
	}

	return &SequencedMessagePublisher{writer: writer}, nil
}

func (p *SequencedMessagePublisher) Publish(
	ctx context.Context,
	event messageevent.ChatMessageSequencedEvent,
) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal sequenced message event: %w", err)
	}

	sessionKey := strconv.FormatInt(event.Message.SessionID, 10)
	if err := p.writer.WriteMessages(
		ctx,
		kafka.Message{
			Key:   []byte(sessionKey),
			Value: payload,
		},
	); err != nil {
		return fmt.Errorf("publish sequenced message event: %w", err)
	}

	return nil
}

func (p *SequencedMessagePublisher) Close() error {
	return p.writer.Close()
}
