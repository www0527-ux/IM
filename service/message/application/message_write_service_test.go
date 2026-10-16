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
	err    error
}

func (f *fakeWriter) Persist(
	context.Context,
	Command,
) (Result, error) {
	f.called = true
	return f.result, f.err
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

func TestMessageWriteServiceChecksMySQLOnCacheFingerprintConflict(t *testing.T) {
	command := validRealtimeCommand(t)
	writer := &fakeWriter{result: Result{
		Message: messageevent.ChatMessage{MessageID: 30, Seq: 40},
		Created: false,
	}}
	service := NewMessageWriteService(fakeCache{
		entry: messagecache.IdempotencyEntry{
			MessageID:   30,
			Seq:         40,
			Fingerprint: "different",
		},
		found: true,
	}, writer)

	result, err := service.Send(context.Background(), command)
	if err != nil || !writer.called || result.Message.MessageID != 30 {
		t.Fatalf("MySQL fallback not used: result=%+v err=%v called=%t", result, err, writer.called)
	}
}

func TestMessageWriteServiceReturnsConfirmedMySQLConflict(t *testing.T) {
	command := validRealtimeCommand(t)
	writer := &fakeWriter{err: ErrIdempotencyConflict}
	service := NewMessageWriteService(fakeCache{
		entry: messagecache.IdempotencyEntry{Fingerprint: "different"},
		found: true,
	}, writer)

	_, err := service.Send(context.Background(), command)
	if !writer.called || !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("confirmed MySQL conflict: called=%t err=%v", writer.called, err)
	}
}
