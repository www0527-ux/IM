package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	messageevent "infinitechatgo/common/model/event"
	"infinitechatgo/service/transfer/internal/svc"
)

const (
	initialRetryBackoff = 100 * time.Millisecond
	maxRetryBackoff     = 5 * time.Second
)

type MessageTransferConsumer struct {
	svcCtx *svc.ServiceContext
}

func NewMessageTransferConsumer(
	svcCtx *svc.ServiceContext,
) *MessageTransferConsumer {
	return &MessageTransferConsumer{svcCtx: svcCtx}
}

// 解析、过滤、重试策略
func (c *MessageTransferConsumer) Consume(
	ctx context.Context,
	key string,
	value string,
) error {
	var acceptedEvent messageevent.ChatMessageAcceptedEvent
	if err := json.Unmarshal([]byte(value), &acceptedEvent); err != nil {
		log.Printf(
			"discard invalid accepted event: key=%s, err=%v",
			key,
			err,
		)
		return nil
	}

	if acceptedEvent.EventType != messageevent.ChatMessageAcceptedEventType {
		log.Printf(
			"discard unexpected accepted event type: key=%s, type=%s",
			key,
			acceptedEvent.EventType,
		)
		return nil
	}
	if acceptedEvent.Version != messageevent.ChatMessageEventVersion {
		log.Printf(
			"discard unsupported accepted event version: key=%s, version=%d",
			key,
			acceptedEvent.Version,
		)
		return nil
	}
	//退避时间
	backoff := initialRetryBackoff
	for {
		if err := c.transfer(ctx, acceptedEvent); err == nil {
			return nil
		} else {
			log.Printf(
				"transfer message failed, retrying: messageId=%d, err=%v",
				acceptedEvent.Message.MessageID,
				err,
			)
		}

		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		}

		backoff *= 2
		if backoff > maxRetryBackoff {
			backoff = maxRetryBackoff
		}
	}
}

func (c *MessageTransferConsumer) transfer(
	ctx context.Context,
	acceptedEvent messageevent.ChatMessageAcceptedEvent,
) error {
	message := acceptedEvent.Message
	seq, err := c.svcCtx.SequenceResolver.Resolve(
		ctx,
		message.SessionID,
		message.MessageID,
	)
	if err != nil {
		return err
	}

	message.Seq = seq
	sequencedEvent := messageevent.NewChatMessageSequencedEvent(message)

	if err := c.svcCtx.SequenceResolver.Cache(
		ctx,
		sequencedEvent,
	); err != nil {
		return err
	}

	if err := c.svcCtx.SequencedPublisher.Publish(
		ctx,
		sequencedEvent,
	); err != nil {
		return err
	}

	log.Printf(
		"message sequenced: messageId=%d, sessionId=%d, seq=%d",
		message.MessageID,
		message.SessionID,
		message.Seq,
	)
	return nil
}

func ValidateKafkaConfig(c *svc.ServiceContext) error {
	if c.Config.Kafka.AcceptedTopic == "" {
		return fmt.Errorf("kafka accepted topic cannot be empty")
	}
	if c.Config.Kafka.AcceptedGroup == "" {
		return fmt.Errorf("kafka accepted group cannot be empty")
	}
	return nil
}
