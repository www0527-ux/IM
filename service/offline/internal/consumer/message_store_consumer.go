package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"infinitechatgo/common/common"
	messageevent "infinitechatgo/common/model/event"
	"infinitechatgo/service/offline/internal/svc"
	"infinitechatgo/service/offline/model/message"
)

const (
	initialStoreRetryBackoff = 100 * time.Millisecond
	maxStoreRetryBackoff     = 5 * time.Second
)

type MessageStoreConsumer struct {
	svcCtx *svc.ServiceContext
}

func NewMessageStoreConsumer(
	svcCtx *svc.ServiceContext,
) *MessageStoreConsumer {
	return &MessageStoreConsumer{svcCtx: svcCtx}
}

func (c *MessageStoreConsumer) Consume(
	ctx context.Context,
	key string,
	value string,
) error {
	var sequencedEvent messageevent.ChatMessageSequencedEvent
	if err := json.Unmarshal([]byte(value), &sequencedEvent); err != nil {
		common.Errorf("discard invalid kafka message: key=%s, err=%v", key, err)
		return nil
	}
	if sequencedEvent.EventType != messageevent.ChatMessageSequencedEventType {
		common.Errorf("discard unexpected event type: %s", sequencedEvent.EventType)
		return nil
	}
	if sequencedEvent.Version != messageevent.ChatMessageEventVersion {
		common.Errorf("discard unsupported event version: %d", sequencedEvent.Version)
		return nil
	}

	data := toMessageModel(sequencedEvent.Message)
	backoff := initialStoreRetryBackoff
	for {
		storedMessage, inserted, err :=
			c.svcCtx.MessageModel.InsertIdempotently(ctx, data)
		if err == nil {
			common.Infof(
				"message stored: messageId=%d, sessionId=%d, seq=%d, inserted=%t",
				storedMessage.MessageId,
				storedMessage.SessionId,
				storedMessage.Seq,
				inserted,
			)
			return nil
		}

		if errors.Is(err, message.ErrIdempotencyConflict) {
			common.Errorf(
				"permanent message idempotency conflict: messageId=%d, sessionId=%d, seq=%d, err=%v",
				data.MessageId,
				data.SessionId,
				data.Seq,
				err,
			)
			return nil
		}

		common.Errorf(
			"store message failed, retrying: messageId=%d, sessionId=%d, seq=%d, backoff=%s, err=%v",
			data.MessageId,
			data.SessionId,
			data.Seq,
			backoff,
			err,
		)

		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return fmt.Errorf("store message %d: %w", data.MessageId, ctx.Err())
		}

		backoff *= 2
		if backoff > maxStoreRetryBackoff {
			backoff = maxStoreRetryBackoff
		}
	}
}

func toMessageModel(chatMessage messageevent.ChatMessage) *message.Message {
	data := &message.Message{
		MessageId:       chatMessage.MessageID,
		ClientMessageId: chatMessage.ClientMessageID,
		SenderId:        chatMessage.SenderID,
		SessionId:       chatMessage.SessionID,
		Seq:             chatMessage.Seq,
		SessionType:     int64(chatMessage.SessionType),
		MessageType:     int64(chatMessage.MessageType),
		Content:         chatMessage.Body.Content,
		CreatedTime:     time.UnixMilli(chatMessage.CreatedAt),
	}

	if chatMessage.Body.ReplyID != nil {
		data.ReplyId = sql.NullInt64{
			Int64: *chatMessage.Body.ReplyID,
			Valid: true,
		}
	}

	return data
}
