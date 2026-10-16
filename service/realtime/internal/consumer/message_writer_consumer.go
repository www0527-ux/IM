package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	messageevent "infinitechatgo/common/model/event"
	messageapplication "infinitechatgo/service/message/application"
)

type messageWriter interface {
	Send(
		ctx context.Context,
		command messageapplication.Command,
	) (messageapplication.Result, error)
}

type MessageWriterConsumer struct {
	writer messageWriter
}

func NewMessageWriterConsumer(writer messageWriter) *MessageWriterConsumer {
	return &MessageWriterConsumer{writer: writer}
}

// 消息落库业务：requestedEvent->落库(permanent 永久的)
func (c *MessageWriterConsumer) Consume(
	ctx context.Context,
	key string,
	value string,
) error {
	if c == nil || c.writer == nil {
		return &FatalError{Err: messageapplication.ErrPersistenceDependencyMissing}
	}
	var event messageevent.ChatMessageCreateRequestedEvent
	if err := json.Unmarshal([]byte(value), &event); err != nil {
		return permanent("invalid_json",
			fmt.Errorf("decode message create-requested event key=%q: %w", key, err))
	}
	if event.EventType != messageevent.ChatMessageCreateRequestedEventType ||
		event.Version != messageevent.ChatMessageEventVersion {
		return permanent("unsupported_event", fmt.Errorf(
			"unsupported message create-requested event key=%q type=%q version=%d",
			key,
			event.EventType,
			event.Version,
		))
	}
	//实现落库
	_, err := c.writer.Send(ctx, messageapplication.Command{
		Source:      messageapplication.SourceRedPacket,
		Message:     event.Message,
		Fingerprint: event.Fingerprint,
	})
	switch {
	case errors.Is(err, messageapplication.ErrInvalidCommand):
		return permanent("invalid_command", err)
	case errors.Is(err, messageapplication.ErrIdempotencyConflict):
		return permanent("idempotency_conflict", err)
	case errors.Is(err, messageapplication.ErrUniqueConstraintViolation):
		return &FatalError{Err: err}
	case errors.Is(err, messageapplication.ErrPersistenceDependencyMissing):
		return &FatalError{Err: err}
	default:
		return err
	}
}
