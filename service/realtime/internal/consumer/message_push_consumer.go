package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	messageevent "infinitechatgo/common/model/event"
	"infinitechatgo/service/realtime/internal/protocol"
	realtimews "infinitechatgo/service/realtime/internal/websocket"
	"infinitechatgo/service/user/rpc/userrpc"

	"google.golang.org/grpc"
)

const groupMemberLookupTimeout = 3 * time.Second

type messageSender interface {
	SendToUser(userID int64, payload []byte) int
}

type groupMemberProvider interface {
	GetGroupMemberIds(
		ctx context.Context,
		in *userrpc.SessionIdReq,
		opts ...grpc.CallOption,
	) (*userrpc.GroupMemberIdsResp, error)
}

type MessagePushConsumer struct {
	manager      messageSender
	groupMembers groupMemberProvider
	expectedType string
}

func NewMessagePushConsumer(
	manager *realtimews.Manager,
	groupMembers userrpc.UserRpc,
) *MessagePushConsumer {
	return &MessagePushConsumer{
		manager:      manager,
		groupMembers: groupMembers,
		expectedType: messageevent.ChatMessageSequencedEventType,
	}
}

func NewPersistedMessagePushConsumer(
	manager *realtimews.Manager,
	groupMembers userrpc.UserRpc,
) *MessagePushConsumer {
	return &MessagePushConsumer{
		manager:      manager,
		groupMembers: groupMembers,
		expectedType: messageevent.ChatMessagePersistedEventType,
	}
}

func (c *MessagePushConsumer) Consume(
	ctx context.Context,
	key string,
	value string,
) error {
	var event struct {
		EventType string                   `json:"eventType"`
		Version   int                      `json:"version"`
		Message   messageevent.ChatMessage `json:"message"`
	}
	if err := json.Unmarshal([]byte(value), &event); err != nil {
		log.Printf("discard invalid message event: key=%s, err=%v", key, err)
		return nil
	}
	expectedType := c.expectedType
	if expectedType == "" {
		expectedType = messageevent.ChatMessageSequencedEventType
	}
	if event.EventType != expectedType {
		log.Printf("discard unexpected push event type: key=%s, type=%s", key, event.EventType)
		return nil
	}
	if event.Version != messageevent.ChatMessageEventVersion {
		log.Printf("discard unsupported push event version: key=%s, version=%d", key, event.Version)
		return nil
	}

	message := event.Message
	payload, err := json.Marshal(protocol.NewMessage(message))
	if err != nil {
		log.Printf("marshal realtime message failed: messageId=%d, err=%v", message.MessageID, err)
		return nil
	}

	switch message.SessionType {
	case messageevent.SessionTypeSingle:
		return c.pushSingle(message, payload)
	case messageevent.SessionTypeGroup:
		return c.pushGroup(ctx, message, payload)
	default:
		log.Printf(
			"discard unsupported realtime session type: messageId=%d, sessionId=%d, type=%d",
			message.MessageID,
			message.SessionID,
			message.SessionType,
		)
		return nil
	}
}

func (c *MessagePushConsumer) pushSingle(
	message messageevent.ChatMessage,
	payload []byte,
) error {
	if message.ReceiverID == nil || *message.ReceiverID <= 0 {
		log.Printf(
			"discard single message without receiver: messageId=%d, sessionId=%d",
			message.MessageID,
			message.SessionID,
		)
		return nil
	}

	senderQueued := c.manager.SendToUser(message.SenderID, payload)
	receiverQueued := 0
	if *message.ReceiverID != message.SenderID {
		receiverQueued = c.manager.SendToUser(*message.ReceiverID, payload)
	}

	log.Printf(
		"realtime push attempted: messageId=%d, sessionId=%d, seq=%d, senderQueued=%d, receiverQueued=%d",
		message.MessageID,
		message.SessionID,
		message.Seq,
		senderQueued,
		receiverQueued,
	)
	return nil
}

func (c *MessagePushConsumer) pushGroup(
	ctx context.Context,
	message messageevent.ChatMessage,
	payload []byte,
) error {
	lookupCtx, cancel := context.WithTimeout(ctx, groupMemberLookupTimeout)
	defer cancel()

	result, err := c.groupMembers.GetGroupMemberIds(
		lookupCtx,
		&userrpc.SessionIdReq{SessionId: message.SessionID},
	)
	if err != nil {
		return fmt.Errorf(
			"load group members for message %d: %w",
			message.MessageID,
			err,
		)
	}
	if result == nil {
		return fmt.Errorf(
			"load group members for message %d: nil response",
			message.MessageID,
		)
	}

	seen := make(map[int64]struct{}, len(result.UserIds))
	memberCount := 0
	queued := 0
	for _, userID := range result.UserIds {
		if userID <= 0 {
			continue
		}
		if _, exists := seen[userID]; exists {
			continue
		}
		seen[userID] = struct{}{}
		memberCount++
		queued += c.manager.SendToUser(userID, payload)
	}

	log.Printf(
		"group realtime push attempted: messageId=%d, sessionId=%d, seq=%d, members=%d, queued=%d",
		message.MessageID,
		message.SessionID,
		message.Seq,
		memberCount,
		queued,
	)
	return nil
}
