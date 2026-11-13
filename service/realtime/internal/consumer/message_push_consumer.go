package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"infinitechatgo/common/messagetrace"

	messageevent "infinitechatgo/common/model/event"
	"infinitechatgo/service/realtime/internal/protocol"
	realtimews "infinitechatgo/service/realtime/internal/websocket"
	"infinitechatgo/service/user/rpc/userrpc"

	"github.com/zeromicro/go-zero/core/logx"
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
	groupMembers groupMemberProvider //群成员查询依赖
}

func NewMessagePushConsumer(
	manager *realtimews.Manager,
	groupMembers groupMemberProvider,
) *MessagePushConsumer {
	return &MessagePushConsumer{
		manager:      manager,
		groupMembers: groupMembers,
	}
}

func (c *MessagePushConsumer) Consume(
	ctx context.Context,
	key string,
	value string,
) error {
	started := messagetrace.Stamp()
	if c == nil || c.manager == nil || c.groupMembers == nil {
		return &FatalError{Err: fmt.Errorf("push consumer dependencies are required")}
	}
	var event struct {
		EventType string                   `json:"eventType"`
		Version   int                      `json:"version"`
		Message   messageevent.ChatMessage `json:"message"`
	}
	if err := json.Unmarshal([]byte(value), &event); err != nil {
		return permanent("invalid_json", fmt.Errorf("decode persisted message event key=%q: %w", key, err))
	}
	if event.EventType != messageevent.ChatMessagePersistedEventType {
		return permanent("unsupported_event", fmt.Errorf("unexpected push event type key=%q type=%q", key, event.EventType))
	}
	if event.Version != messageevent.ChatMessageEventVersion {
		return permanent("unsupported_event", fmt.Errorf("unsupported push event version key=%q version=%d", key, event.Version))
	}

	message := event.Message
	if messagetrace.Enabled() {
		messagetrace.MessageAt(message, "push_handler_start", started)
		defer func() { messagetrace.Message(message, "push_handler_end") }()
	}
	if message.MessageID <= 0 || message.Seq <= 0 || message.SenderID <= 0 ||
		message.SessionID <= 0 || strings.TrimSpace(message.ClientMessageID) == "" {
		return permanent("invalid_message", fmt.Errorf(
			"invalid persisted message for push: key=%q messageId=%d", key, message.MessageID,
		))
	}
	payload, err := json.Marshal(protocol.NewMessage(message))
	if err != nil {
		return fmt.Errorf("marshal realtime message %d: %w", message.MessageID, err)
	}

	switch message.SessionType {
	case messageevent.SessionTypeSingle:
		return c.pushSingle(message, payload)
	case messageevent.SessionTypeGroup:
		//message用来提供结构化数据
		return c.pushGroup(ctx, message, payload)
	default:
		return permanent("invalid_message", fmt.Errorf(
			"unsupported realtime session type: messageId=%d, sessionId=%d, type=%d",
			message.MessageID,
			message.SessionID,
			message.SessionType,
		))
	}
}

func (c *MessagePushConsumer) pushSingle(
	message messageevent.ChatMessage,
	payload []byte,
) error {
	if message.ReceiverID == nil || *message.ReceiverID <= 0 {
		return permanent("invalid_message", fmt.Errorf(
			"single message without receiver: messageId=%d, sessionId=%d",
			message.MessageID,
			message.SessionID,
		))
	}
	if *message.ReceiverID == message.SenderID {
		return permanent("invalid_message", fmt.Errorf(
			"single message receiver equals sender: messageId=%d, sessionId=%d",
			message.MessageID, message.SessionID,
		))
	}
	//记录输入的数目
	receiverQueued := c.manager.SendToUser(*message.ReceiverID, payload)

	logx.Debugf(
		"realtime push attempted: messageId=%d, sessionId=%d, seq=%d, receiverQueued=%d",
		message.MessageID,
		message.SessionID,
		message.Seq,
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

	messagetrace.Message(message, "group_rpc_start")
	result, err := c.groupMembers.GetGroupMemberIds(
		lookupCtx,
		&userrpc.SessionIdReq{SessionId: message.SessionID},
	)
	messagetrace.Message(message, "group_rpc_end")
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

	messagetrace.Message(message, "fanout_start")
	seen := make(map[int64]struct{}, len(result.UserIds))
	memberCount := 0
	queued := 0
	for _, userID := range result.UserIds {
		if userID <= 0 || userID == message.SenderID {
			continue
		}
		if _, exists := seen[userID]; exists {
			continue
		}
		seen[userID] = struct{}{}
		memberCount++
		queued += c.manager.SendToUser(userID, payload)
	}
	messagetrace.Message(message, "fanout_end")

	logx.Debugf(
		"group realtime push attempted: messageId=%d, sessionId=%d, seq=%d, members=%d, queued=%d",
		message.MessageID,
		message.SessionID,
		message.Seq,
		memberCount,
		queued,
	)
	return nil
}
