package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	messageevent "infinitechatgo/common/model/event"
	"infinitechatgo/service/offline/model/message"
)

type fakeCache struct {
	values []string
	err    error
}

func (f fakeCache) MgetCtx(_ context.Context, _ ...string) ([]string, error) {
	return f.values, f.err
}

type fakeMessageStore struct {
	maxSeq           int64
	messages         map[int64]*message.Message
	requestedAfter   int64
	requestedThrough int64
}

func (f *fakeMessageStore) MaxSeq(context.Context, int64) (int64, error) {
	return f.maxSeq, nil
}

func (f *fakeMessageStore) FindBySessionSeqRange(
	_ context.Context,
	_ int64,
	afterSeq int64,
	throughSeq int64,
) ([]*message.Message, error) {
	f.requestedAfter = afterSeq
	f.requestedThrough = throughSeq
	rows := make([]*message.Message, 0, int(throughSeq-afterSeq))
	for seq := afterSeq + 1; seq <= throughSeq; seq++ {
		if row := f.messages[seq]; row != nil {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

type fakeSessionAccess struct {
	allowed bool
}

func (f fakeSessionAccess) IsActiveMember(
	context.Context,
	int64,
	int64,
) (bool, error) {
	return f.allowed, nil
}

func TestReaderSyncMergesCacheAndDatabaseAndSkipsSeqHole(t *testing.T) {
	const sessionID int64 = 20
	cachedMessage := messageevent.ChatMessage{
		MessageID:       101,
		ClientMessageID: "client-1",
		SessionID:       sessionID,
		Seq:             1,
		SenderID:        7,
		MessageType:     messageevent.MessageTypeText,
		Body:            messageevent.MessageBody{Content: "cached"},
		CreatedAt:       1_700_000_000_000,
	}
	payload, err := json.Marshal(cachedMessage)
	if err != nil {
		t.Fatal(err)
	}

	store := &fakeMessageStore{
		maxSeq: 3,
		messages: map[int64]*message.Message{
			2: {
				MessageId:       102,
				ClientMessageId: "client-2",
				SenderId:        8,
				SessionId:       sessionID,
				Seq:             2,
				MessageType:     int64(messageevent.MessageTypeText),
				Content:         "database",
				CreatedTime:     time.UnixMilli(1_700_000_001_000),
			},
		},
	}
	reader := NewReader(
		fakeCache{values: []string{string(payload), "", ""}},
		store,
		fakeSessionAccess{allowed: true},
	)

	result, err := reader.Sync(context.Background(), 7, sessionID, 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	if store.requestedAfter != 0 || store.requestedThrough != 3 {
		t.Fatalf(
			"database fallback range = (%d, %d], want (0, 3]",
			store.requestedAfter,
			store.requestedThrough,
		)
	}
	if result.NextSeq != 3 || result.ServerMaxSeq != 3 || result.HasMore {
		t.Fatalf("unexpected cursor result: %+v", result)
	}
	if len(result.Messages) != 2 ||
		result.Messages[0].Seq != 1 ||
		result.Messages[1].Seq != 2 {
		t.Fatalf("unexpected messages: %+v", result.Messages)
	}
}

func TestReaderSyncFallsBackWhenCacheIsUnavailable(t *testing.T) {
	store := &fakeMessageStore{
		maxSeq:   2,
		messages: map[int64]*message.Message{},
	}
	reader := NewReader(
		fakeCache{err: errors.New("redis unavailable")},
		store,
		fakeSessionAccess{allowed: true},
	)

	if _, err := reader.Sync(context.Background(), 1, 2, 0, 2); err != nil {
		t.Fatal(err)
	}
	if store.requestedAfter != 0 || store.requestedThrough != 2 {
		t.Fatalf(
			"database fallback range = (%d, %d], want (0, 2]",
			store.requestedAfter,
			store.requestedThrough,
		)
	}
}

func TestDecodeCachedMessageRejectsOldEventWrapper(t *testing.T) {
	message := messageevent.ChatMessage{
		MessageID:       101,
		ClientMessageID: "client-1",
		SessionID:       20,
		Seq:             1,
		SenderID:        7,
	}
	payload, err := json.Marshal(messageevent.NewChatMessagePersistedEvent(message, 0))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := decodeCachedMessage(string(payload), 20, 1); ok {
		t.Fatal("old event wrapper must fall back to the message store")
	}
}

func TestReaderSyncRejectsNonMember(t *testing.T) {
	reader := NewReader(
		fakeCache{},
		&fakeMessageStore{},
		fakeSessionAccess{allowed: false},
	)
	_, err := reader.Sync(context.Background(), 1, 2, 0, 10)
	if !errors.Is(err, ErrSessionAccessDenied) {
		t.Fatalf("error = %v, want ErrSessionAccessDenied", err)
	}
}
