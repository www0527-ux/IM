package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	messageevent "infinitechatgo/common/model/event"
	"infinitechatgo/service/realtime/internal/config"

	"github.com/segmentio/kafka-go"
)

type AcceptedMessagePublisher struct {
	writer *kafka.Writer
}

func NewAcceptedMessagePublisher(
	c config.KafkaConfig,
) (*AcceptedMessagePublisher, error) {
	if len(c.Brokers) == 0 {
		return nil, fmt.Errorf(
			"kafka brokers cannot be empty",
		)
	}

	if strings.TrimSpace(c.AcceptedTopic) == "" {
		return nil, fmt.Errorf(
			"kafka accepted topic cannot be empty",
		)
	}

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(c.Brokers...),
		Topic:                  c.AcceptedTopic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		Async:                  false,
		MaxAttempts:            5,
		ReadTimeout:            10 * time.Second,
		WriteTimeout:           10 * time.Second,
		AllowAutoTopicCreation: false,
	}
	return &AcceptedMessagePublisher{
		writer: writer,
	}, nil
}

func (p *AcceptedMessagePublisher) Publish(
	ctx context.Context,
	event messageevent.ChatMessageAcceptedEvent,
) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf(
			"marshal accepted message event: %w",
			err,
		)
	}
	sessionKey := strconv.FormatInt(
		event.Message.SessionID,
		10,
	)

	if err := p.writer.WriteMessages(
		ctx,
		kafka.Message{
			Key:   []byte(sessionKey),
			Value: payload,
		},
	); err != nil {
		return fmt.Errorf(
			"publish accepted message event: %w",
			err,
		)
	}

	return nil
}
func (p *AcceptedMessagePublisher) Close() error {
	return p.writer.Close()
}
