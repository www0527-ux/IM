package consumer

import (
	"context"
	"encoding/json"
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

func (c *MessageWriterConsumer) Consume(
	ctx context.Context,
	key string,
	value string,
) error {
	var event messageevent.ChatMessageCreateRequestedEvent
	if err := json.Unmarshal([]byte(value), &event); err != nil {
		// TODO(user): route permanent protocol failures to DLQ before returning
		// nil. Until that policy is implemented, returning an error prevents an
		// accidental offset commit.
		return fmt.Errorf("decode message create-requested event key=%q: %w", key, err)
	}
	if event.EventType != messageevent.ChatMessageCreateRequestedEventType ||
		event.Version != messageevent.ChatMessageEventVersion {
		return fmt.Errorf(
			"unsupported message create-requested event key=%q type=%q version=%d",
			key,
			event.EventType,
			event.Version,
		)
	}
	if c == nil || c.writer == nil {
		return messageapplication.ErrCoreNotImplemented
	}

	_, err := c.writer.Send(ctx, messageapplication.Command{
		Source:      messageapplication.SourceRedPacket,
		Message:     event.Message,
		Fingerprint: event.Fingerprint,
	})
	return err
}
