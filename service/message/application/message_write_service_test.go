package application

import (
	"context"
	"errors"
	"testing"

	"infinitechatgo/common/messagecache"
	"infinitechatgo/common/messageidentity"
	messageevent "infinitechatgo/common/model/event"
)

type fakeCache struct {
	entry messagecache.IdempotencyEntry
	found bool
	err   error
}

func (f fakeCache) Lookup(
	context.Context,
	int64,
	string,
) (messagecache.IdempotencyEntry, bool, error) {
	return f.entry, f.found, f.err
}

type fakeWriter struct {
	called bool
	result Result
}

func (f *fakeWriter) Persist(
	context.Context,
	Command,
) (Result, error) {
	f.called = true
	return f.result, nil
}

func validRealtimeCommand(t *testing.T) Command {
	t.Helper()
	message := messageevent.ChatMessage{
		ClientMessageID: "client-1",
		SessionID:       20,
		SessionType:     messageevent.SessionTypeGroup,
		SenderID:        10,
		MessageType:     messageevent.MessageTypeText,
		Body:            messageevent.MessageBody{Content: "hello"},
	}
	fingerprint, err := messageidentity.Fingerprint(message)
	if err != nil {
		t.Fatalf("fingerprint message: %v", err)
	}
	return Command{
		Source:      SourceRealtime,
		Message:     message,
		Fingerprint: fingerprint,
	}
}

func TestMessageWriteServiceReturnsPersistedCacheHit(t *testing.T) {
	command := validRealtimeCommand(t)
	writer := &fakeWriter{}
	service := NewMessageWriteService(fakeCache{
		entry: messagecache.IdempotencyEntry{
			MessageID:   30,
			Seq:         40,
			Fingerprint: command.Fingerprint,
		},
		found: true,
	}, writer)

	result, err := service.Send(context.Background(), command)
	if err != nil {
		t.Fatalf("send message: %v", err)
	}
	if writer.called {
		t.Fatal("persistent writer called on valid cache hit")
	}
	if result.Message.MessageID != 30 || result.Message.Seq != 40 || result.Created {
		t.Fatalf("unexpected cache result: %+v", result)
	}
}

func TestMessageWriteServiceFallsBackWhenCacheFails(t *testing.T) {
	command := validRealtimeCommand(t)
	writer := &fakeWriter{result: Result{Message: messageevent.ChatMessage{MessageID: 30}, Created: true}}
	service := NewMessageWriteService(
		fakeCache{err: errors.New("redis unavailable")},
		writer,
	)

	result, err := service.Send(context.Background(), command)
	if err != nil {
		t.Fatalf("send message: %v", err)
	}
	if !writer.called || result.Message.MessageID != 30 {
		t.Fatalf("persistent fallback not used: called=%t result=%+v", writer.called, result)
	}
}

func TestMessageWriteServiceRejectsFingerprintConflict(t *testing.T) {
	command := validRealtimeCommand(t)
	service := NewMessageWriteService(fakeCache{
		entry: messagecache.IdempotencyEntry{
			MessageID:   30,
			Seq:         40,
			Fingerprint: "different",
		},
		found: true,
	}, &fakeWriter{})

	_, err := service.Send(context.Background(), command)
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("expected idempotency conflict, got %v", err)
	}
}
