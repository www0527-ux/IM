package consumer

import (
	"context"
	"encoding/json"
	"fmt"

	"infinitechatgo/common/messagecache"
	"infinitechatgo/common/messageidentity"
	messageevent "infinitechatgo/common/model/event"
)

type cacheWriter interface {
	SetexCtx(ctx context.Context, key string, value string, seconds int) error
}

type MessageCacheConsumer struct {
	cache                 cacheWriter
	contentTTLSeconds     int
	idempotencyTTLSeconds int
}

func NewMessageCacheConsumer(
	cache cacheWriter,
	contentTTLSeconds int,
	idempotencyTTLSeconds int,
) (*MessageCacheConsumer, error) {
	if cache == nil || contentTTLSeconds <= 0 || idempotencyTTLSeconds <= 0 {
		return nil, fmt.Errorf("invalid message cache consumer dependencies")
	}
	return &MessageCacheConsumer{
		cache:                 cache,
		contentTTLSeconds:     contentTTLSeconds,
		idempotencyTTLSeconds: idempotencyTTLSeconds,
	}, nil
}

func (c *MessageCacheConsumer) Consume(
	ctx context.Context,
	key string,
	value string,
) error {
	var event messageevent.ChatMessagePersistedEvent
	if err := json.Unmarshal([]byte(value), &event); err != nil {
		return fmt.Errorf("decode persisted message event key=%q: %w", key, err)
	}
	if event.EventType != messageevent.ChatMessagePersistedEventType ||
		event.Version != messageevent.ChatMessageEventVersion {
		return fmt.Errorf(
			"unsupported persisted message event key=%q type=%q version=%d",
			key,
			event.EventType,
			event.Version,
		)
	}
	message := event.Message
	if message.MessageID <= 0 || message.Seq <= 0 || message.SenderID <= 0 ||
		message.SessionID <= 0 || message.ClientMessageID == "" {
		return fmt.Errorf("invalid persisted message event key=%q", key)
	}

	fingerprint, err := messageidentity.Fingerprint(message)
	if err != nil {
		return fmt.Errorf("fingerprint persisted message %d: %w", message.MessageID, err)
	}
	if event.Fingerprint != "" && event.Fingerprint != fingerprint {
		return fmt.Errorf("persisted message %d fingerprint mismatch", message.MessageID)
	}
	event.Fingerprint = fingerprint

	contentValue, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode persisted message cache: %w", err)
	}
	entryValue, err := json.Marshal(messagecache.IdempotencyEntry{
		MessageID:   message.MessageID,
		Seq:         message.Seq,
		Fingerprint: fingerprint,
	})
	if err != nil {
		return fmt.Errorf("encode message idempotency cache: %w", err)
	}

	if err := c.cache.SetexCtx(
		ctx,
		messagecache.ContentKey(message.SessionID, message.Seq),
		string(contentValue),
		c.contentTTLSeconds,
	); err != nil {
		return fmt.Errorf("write persisted message content cache: %w", err)
	}
	if err := c.cache.SetexCtx(
		ctx,
		messagecache.IdempotencyKey(message.SenderID, message.ClientMessageID),
		string(entryValue),
		c.idempotencyTTLSeconds,
	); err != nil {
		return fmt.Errorf("write persisted message idempotency cache: %w", err)
	}
	return nil
}
