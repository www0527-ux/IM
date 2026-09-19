package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
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
	maxSeq        int64
	messages      map[int64]*message.Message
	requestedSeqs []int64
}

func (f *fakeMessageStore) MaxSeq(context.Context, int64) (int64, error) {
	return f.maxSeq, nil
}

func (f *fakeMessageStore) FindBySessionSeqs(
	_ context.Context,
	_ int64,
	seqs []int64,
) ([]*message.Message, error) {
	f.requestedSeqs = append([]int64(nil), seqs...)
	rows := make([]*message.Message, 0, len(seqs))
	for _, seq := range seqs {
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
	payload, err := json.Marshal(
		messageevent.NewChatMessageSequencedEvent(cachedMessage),
	)
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
	if !reflect.DeepEqual(store.requestedSeqs, []int64{2, 3}) {
		t.Fatalf("database fallback seqs = %v, want [2 3]", store.requestedSeqs)
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
	if !reflect.DeepEqual(store.requestedSeqs, []int64{1, 2}) {
		t.Fatalf("database fallback seqs = %v, want [1 2]", store.requestedSeqs)
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
