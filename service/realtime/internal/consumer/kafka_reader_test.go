package consumer

import (
	"context"
	"errors"
	"io"
	"testing"

	"infinitechatgo/common/kafkareader"

	"github.com/segmentio/kafka-go"
)

type scriptedReader struct {
	message      kafka.Message
	cancel       context.CancelFunc
	fetches      int
	commits      int
	commitErrors int
	commitResult error
	fetchEOFs    int
	commitEOFs   int
	delivered    bool
	onCommit     func()
}

func (r *scriptedReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	r.fetches++
	if r.fetchEOFs > 0 {
		r.fetchEOFs--
		return kafka.Message{}, io.EOF
	}
	if !r.delivered {
		r.delivered = true
		return r.message, nil
	}
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (r *scriptedReader) CommitMessages(_ context.Context, _ ...kafka.Message) error {
	r.commits++
	if r.commitEOFs > 0 {
		r.commitEOFs--
		return io.EOF
	}
	if r.onCommit != nil {
		r.onCommit()
	}
	if r.commitErrors > 0 {
		r.commitErrors--
		return errors.New("commit unavailable")
	}
	if r.commitResult != nil {
		return r.commitResult
	}
	r.cancel()
	return nil
}

func (r *scriptedReader) Close() error { return nil }

func TestKafkaConsumerRetriesNetworkEOFDuringFetchAndCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &scriptedReader{message: testRecord(), cancel: cancel, fetchEOFs: 1, commitEOFs: 1}
	client.onCommit = func() {
		if client.fetches != 2 {
			t.Fatalf("next record fetched before current commit completed: fetches=%d", client.fetches)
		}
	}
	reader := kafkareader.Wrap(client)
	defer reader.Close()
	handler, dlq := &scriptedHandler{}, &scriptedDLQ{}
	if err := RunKafkaConsumer(ctx, reader, handler, "writer", dlq); err != nil {
		t.Fatalf("transient network EOF stopped consumer: %v", err)
	}
	if handler.calls != 1 || client.commits != 2 || len(dlq.records) != 0 {
		t.Fatalf("fetches=%d business=%d commits=%d DLQ=%d", client.fetches, handler.calls, client.commits, len(dlq.records))
	}
}

func TestKafkaConsumerStopsForExplicitReaderClose(t *testing.T) {
	client := &scriptedReader{fetchEOFs: 1}
	reader := kafkareader.Wrap(client)
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	handler, dlq := &scriptedHandler{}, &scriptedDLQ{}
	err := RunKafkaConsumer(context.Background(), reader, handler, "writer", dlq)
	if !errors.Is(err, io.ErrClosedPipe) || handler.calls != 0 || client.commits != 0 {
		t.Fatalf("closed Reader: err=%v business=%d commits=%d", err, handler.calls, client.commits)
	}
}

type scriptedHandler struct {
	responses []error
	calls     int
}

func (h *scriptedHandler) Consume(context.Context, string, string) error {
	h.calls++
	if h.calls <= len(h.responses) {
		return h.responses[h.calls-1]
	}
	return nil
}

type scriptedDLQ struct {
	records   []DeadLetter
	errors    int
	successes int
}

func (d *scriptedDLQ) Publish(_ context.Context, record DeadLetter) error {
	d.records = append(d.records, record)
	if d.errors > 0 {
		d.errors--
		return errors.New("DLQ unavailable")
	}
	d.successes++
	return nil
}

func testRecord() kafka.Message {
	return kafka.Message{
		Topic:     "messages",
		Partition: 2,
		Offset:    7,
		Key:       []byte("session-1"),
		Value:     []byte{0xff, 0x00},
	}
}

func TestKafkaConsumerRetriesCurrentRecordBeforeCommitting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := &scriptedReader{message: testRecord(), cancel: cancel}
	handler := &scriptedHandler{responses: []error{errors.New("database unavailable"), nil}}
	dlq := &scriptedDLQ{}

	if err := RunKafkaConsumer(ctx, reader, handler, "writer", dlq); err != nil {
		t.Fatalf("RunKafkaConsumer: %v", err)
	}
	if handler.calls != 2 || reader.commits != 1 || len(dlq.records) != 0 {
		t.Fatalf("calls=%d commits=%d DLQ=%d", handler.calls, reader.commits, len(dlq.records))
	}
}

func TestKafkaConsumerCommitsOnlyAfterDLQAcknowledged(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := &scriptedReader{message: testRecord(), cancel: cancel}
	handler := &scriptedHandler{responses: []error{
		permanent("invalid_json", errors.New("decode failed")),
	}}
	dlq := &scriptedDLQ{errors: 1}
	prematureCommit := false
	reader.onCommit = func() {
		if dlq.successes == 0 {
			prematureCommit = true
		}
	}

	if err := RunKafkaConsumer(ctx, reader, handler, "writer", dlq); err != nil {
		t.Fatalf("RunKafkaConsumer: %v", err)
	}
	if handler.calls != 1 || reader.commits != 1 || len(dlq.records) != 2 {
		t.Fatalf("calls=%d commits=%d DLQ attempts=%d", handler.calls, reader.commits, len(dlq.records))
	}
	if prematureCommit {
		t.Fatal("original offset committed before DLQ ACK")
	}
	if dlq.records[0].ID != "writer/messages/2/7" ||
		string(dlq.records[0].Value) != string(testRecord().Value) {
		t.Fatalf("DLQ original record identity or bytes changed: %+v", dlq.records[0])
	}
}

func TestKafkaConsumerRetriesCommitWithoutRepeatingBusiness(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := &scriptedReader{message: testRecord(), cancel: cancel, commitErrors: 1}
	handler := &scriptedHandler{}
	dlq := &scriptedDLQ{}

	if err := RunKafkaConsumer(ctx, reader, handler, "writer", dlq); err != nil {
		t.Fatalf("RunKafkaConsumer: %v", err)
	}
	if handler.calls != 1 || reader.commits != 2 || len(dlq.records) != 0 {
		t.Fatalf("calls=%d commits=%d DLQ=%d", handler.calls, reader.commits, len(dlq.records))
	}
}

func TestKafkaConsumerStopsAfterGroupGenerationChanges(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := &scriptedReader{
		message:      testRecord(),
		cancel:       cancel,
		commitResult: kafka.IllegalGeneration,
	}
	handler := &scriptedHandler{}
	dlq := &scriptedDLQ{}

	err := RunKafkaConsumer(ctx, reader, handler, "writer", dlq)
	if !errors.Is(err, kafka.IllegalGeneration) {
		t.Fatalf("RunKafkaConsumer error = %v, want IllegalGeneration", err)
	}
	if handler.calls != 1 || reader.commits != 1 || reader.fetches != 1 {
		t.Fatalf("calls=%d commits=%d fetches=%d", handler.calls, reader.commits, reader.fetches)
	}
}

func TestKafkaConsumerDoesNotSendBrokenHandlerToDLQ(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := &scriptedReader{message: testRecord(), cancel: cancel}
	handler := &scriptedHandler{responses: []error{
		&FatalError{Err: errors.New("handler dependency missing")},
	}}
	dlq := &scriptedDLQ{}

	if err := RunKafkaConsumer(ctx, reader, handler, "writer", dlq); err == nil {
		t.Fatal("broken handler was accepted")
	}
	if reader.commits != 0 || len(dlq.records) != 0 {
		t.Fatalf("commits=%d DLQ=%d, want neither", reader.commits, len(dlq.records))
	}
}
