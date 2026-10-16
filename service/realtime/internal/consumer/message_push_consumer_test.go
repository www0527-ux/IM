package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	messageevent "infinitechatgo/common/model/event"
	"infinitechatgo/service/user/rpc/userrpc"

	"google.golang.org/grpc"
)

type recordingSender struct {
	userIDs []int64
}

func (s *recordingSender) SendToUser(userID int64, _ []byte) int {
	s.userIDs = append(s.userIDs, userID)
	return 1
}

type fakeGroupMemberProvider struct {
	sessionID int64
	response  *userrpc.GroupMemberIdsResp
	err       error
}

func (p *fakeGroupMemberProvider) GetGroupMemberIds(
	_ context.Context,
	in *userrpc.SessionIdReq,
	_ ...grpc.CallOption,
) (*userrpc.GroupMemberIdsResp, error) {
	p.sessionID = in.SessionId
	return p.response, p.err
}

func TestMessagePushConsumerPushesGroupToEveryUniqueOtherMember(t *testing.T) {
	sender := &recordingSender{}
	members := &fakeGroupMemberProvider{
		response: &userrpc.GroupMemberIdsResp{
			UserIds: []int64{11, 22, 22, 0, -1},
		},
	}
	consumer := &MessagePushConsumer{
		manager:      sender,
		groupMembers: members,
	}

	if err := consumer.Consume(
		context.Background(),
		"99",
		persistedEventJSON(t, messageevent.ChatMessage{
			MessageID:       100,
			ClientMessageID: "client-100",
			SessionID:       99,
			SessionType:     messageevent.SessionTypeGroup,
			SenderID:        11,
			Seq:             7,
		}),
	); err != nil {
		t.Fatalf("Consume() error = %v", err)
	}

	if members.sessionID != 99 {
		t.Fatalf("GetGroupMemberIds() sessionID = %d, want 99", members.sessionID)
	}
	if !reflect.DeepEqual(sender.userIDs, []int64{22}) {
		t.Fatalf("pushed users = %v, want [22]", sender.userIDs)
	}
}

func TestMessagePushConsumerRetriesGroupMemberLookupFailure(t *testing.T) {
	sender := &recordingSender{}
	members := &fakeGroupMemberProvider{err: errors.New("rpc unavailable")}
	consumer := &MessagePushConsumer{
		manager:      sender,
		groupMembers: members,
	}

	err := consumer.Consume(
		context.Background(),
		"99",
		persistedEventJSON(t, messageevent.ChatMessage{
			MessageID:       100,
			ClientMessageID: "client-100",
			SessionID:       99,
			SessionType:     messageevent.SessionTypeGroup,
			SenderID:        11,
			Seq:             7,
		}),
	)
	if err == nil {
		t.Fatal("Consume() error = nil, want group member lookup error")
	}
	if len(sender.userIDs) != 0 {
		t.Fatalf("pushed users = %v, want none", sender.userIDs)
	}
}

func TestMessagePushConsumerOnlyQueuesSingleRecipient(t *testing.T) {
	receiverID := int64(22)
	sender := &recordingSender{}
	consumer := &MessagePushConsumer{
		manager:      sender,
		groupMembers: &fakeGroupMemberProvider{},
	}
	err := consumer.Consume(context.Background(), "99", persistedEventJSON(t, messageevent.ChatMessage{
		MessageID:       100,
		ClientMessageID: "client-100",
		SessionID:       99,
		SessionType:     messageevent.SessionTypeSingle,
		SenderID:        11,
		ReceiverID:      &receiverID,
		Seq:             7,
	}))
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	if !reflect.DeepEqual(sender.userIDs, []int64{22}) {
		t.Fatalf("pushed users = %v, want only receiver 22", sender.userIDs)
	}
}

func persistedEventJSON(
	t *testing.T,
	message messageevent.ChatMessage,
) string {
	t.Helper()
	payload, err := json.Marshal(
		messageevent.NewChatMessagePersistedEvent(message, 0),
	)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return string(payload)
}
