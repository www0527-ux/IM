package consumer

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"google.golang.org/grpc"
	"infinitechatgo/common/groupmembercache"
	messageevent "infinitechatgo/common/model/event"
	"infinitechatgo/service/user/rpc/userrpc"
)

type countingMembers struct {
	calls int
	ids   []int64
	err   error
}

func (c *countingMembers) GetGroupMemberIds(context.Context, *userrpc.SessionIdReq, ...grpc.CallOption) (*userrpc.GroupMemberIdsResp, error) {
	c.calls++
	if c.err != nil {
		return nil, c.err
	}
	return &userrpc.GroupMemberIdsResp{UserIds: c.ids}, nil
}

func TestPushCacheReusesRPCAndRefreshesRecipientsAfterInvalidation(t *testing.T) {
	local, err := groupmembercache.NewLocal(groupmembercache.Config{})
	if err != nil {
		t.Fatal(err)
	}
	members := &countingMembers{ids: []int64{11, 22}}
	sender := &recordingSender{}
	c := &MessagePushConsumer{manager: sender, groupMembers: NewCachedGroupMembers(members, local)}
	push := func(id, seq int64) {
		t.Helper()
		err := c.Consume(context.Background(), "99", persistedEventJSON(t, messageevent.ChatMessage{
			MessageID: id, ClientMessageID: "cache-test", SessionID: 99, SessionType: messageevent.SessionTypeGroup, SenderID: 11, Seq: seq}))
		if err != nil {
			t.Fatal(err)
		}
	}
	push(100, 1)
	push(101, 2)
	if members.calls != 1 || !reflect.DeepEqual(sender.userIDs, []int64{22, 22}) {
		t.Fatalf("calls=%d recipients=%v", members.calls, sender.userIDs)
	}
	members.ids = []int64{11, 33}
	local.Invalidate(99)
	push(102, 3)
	if members.calls != 2 || !reflect.DeepEqual(sender.userIDs, []int64{22, 22, 33}) {
		t.Fatalf("calls=%d recipients=%v", members.calls, sender.userIDs)
	}
	members.err = errors.New("RPC unavailable")
	local.Invalidate(99)
	err = c.Consume(context.Background(), "99", persistedEventJSON(t, messageevent.ChatMessage{
		MessageID: 103, ClientMessageID: "retry", SessionID: 99, SessionType: messageevent.SessionTypeGroup, SenderID: 11, Seq: 4}))
	if err == nil || len(sender.userIDs) != 3 {
		t.Fatalf("lookup failure did not stop push: err=%v recipients=%v", err, sender.userIDs)
	}
}
