package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	messageevent "infinitechatgo/common/model/event"
)

type recordingCandidateBuilder struct{ calls *[]string }

func (b recordingCandidateBuilder) Rebuild(context.Context, int64) error {
	*b.calls = append(*b.calls, "candidates")
	return nil
}

type recordingSendResultBuilder struct{ calls *[]string }

func (b recordingSendResultBuilder) Build(context.Context, int64) error {
	*b.calls = append(*b.calls, "send-result")
	return nil
}

func TestCacheBuildRejectsUnusableEvents(t *testing.T) {
	for _, value := range []string{
		"not-json",
		`{"eventType":"redpacket.cache.build_requested","version":1,"redPacketId":"0"}`,
		`{"eventType":"unknown","version":1,"eventId":"1","redPacketId":"2"}`,
	} {
		err := rebuildFromEvent(context.Background(), nil, nil, []byte(value))
		var invalid invalidCacheEvent
		if !errors.As(err, &invalid) {
			t.Fatalf("event %q returned %v, want permanent validation failure", value, err)
		}
	}
}

func TestCacheBuildProjectsBothRedisViews(t *testing.T) {
	var calls []string
	event := messageevent.NewRedPacketCacheBuildEvent(1, 2)
	value, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	err = rebuildFromEvent(context.Background(),
		recordingCandidateBuilder{calls: &calls},
		recordingSendResultBuilder{calls: &calls}, value)
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 || calls[0] != "send-result" || calls[1] != "candidates" {
		t.Fatalf("cache build calls = %v", calls)
	}
}
