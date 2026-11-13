package consumer

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

type batchReaderFake struct {
	mu           sync.Mutex
	records      []kafka.Message
	next         int
	commits      [][]kafka.Message
	commitErrors []error
	onCommit     func([]kafka.Message)
}

func (r *batchReaderFake) FetchMessage(ctx context.Context) (kafka.Message, error) {
	r.mu.Lock()
	if r.next < len(r.records) {
		m := r.records[r.next]
		r.next++
		r.mu.Unlock()
		return m, nil
	}
	r.mu.Unlock()
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}
func (r *batchReaderFake) CommitMessages(_ context.Context, messages ...kafka.Message) error {
	r.mu.Lock()
	r.commits = append(r.commits, append([]kafka.Message(nil), messages...))
	index := len(r.commits) - 1
	var err error
	if index < len(r.commitErrors) {
		err = r.commitErrors[index]
	}
	r.mu.Unlock()
	if r.onCommit != nil {
		r.onCommit(messages)
	}
	return err
}

type batchHandlerFunc func(context.Context, string, string) error

func (f batchHandlerFunc) Consume(ctx context.Context, key, value string) error {
	return f(ctx, key, value)
}
func batchRecords(n int) []kafka.Message {
	rows := make([]kafka.Message, n)
	for i := range rows {
		rows[i] = kafka.Message{Topic: "messages", Partition: 0, Offset: int64(i), Value: []byte(strconv.Itoa(i))}
	}
	return rows
}

func TestBatchCommitOnlyHighestCompletedOffsetInEachPartition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r := &batchReaderFake{records: batchRecords(3)}
	r.records[1].Partition = 1
	r.records[1].Offset = 7
	var handled atomic.Int32
	r.onCommit = func(ms []kafka.Message) {
		if handled.Load() != 3 {
			t.Errorf("committed before successful handling: %d", handled.Load())
		}
		got := map[int]int64{}
		for _, m := range ms {
			got[m.Partition] = m.Offset
		}
		if !reflect.DeepEqual(got, map[int]int64{0: 2, 1: 7}) {
			t.Errorf("committed=%v", got)
		}
		cancel()
	}
	err := RunKafkaConsumerBatched(ctx, r, batchHandlerFunc(func(context.Context, string, string) error { handled.Add(1); return nil }), "push", &scriptedDLQ{}, BatchCommitConfig{BatchSize: 3, IntervalMillis: 1000})
	if err != nil || len(r.commits) != 1 {
		t.Fatalf("err=%v commits=%v", err, r.commits)
	}
}

func TestBatchCommitFlushesIdleTailWithoutAnotherMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r := &batchReaderFake{records: batchRecords(1)}
	r.onCommit = func([]kafka.Message) { cancel() }
	err := RunKafkaConsumerBatched(ctx, r, batchHandlerFunc(func(context.Context, string, string) error { return nil }), "push", &scriptedDLQ{}, BatchCommitConfig{IntervalMillis: 5})
	if err != nil || len(r.commits) != 1 || r.commits[0][0].Offset != 0 {
		t.Fatalf("err=%v commits=%v", err, r.commits)
	}
}

func TestBatchCommitRetriesHandlingAndCommitWithoutCrossingFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r := &batchReaderFake{records: batchRecords(3), commitErrors: []error{errors.New("temporary commit failure")}}
	var seen []string
	failed := false
	h := batchHandlerFunc(func(_ context.Context, _, value string) error {
		seen = append(seen, value)
		if value == "1" && !failed {
			failed = true
			return errors.New("temporary handler failure")
		}
		return nil
	})
	r.onCommit = func(ms []kafka.Message) {
		for _, m := range ms {
			if m.Offset >= 1 && (len(seen) < 3 || seen[2] != "1") {
				t.Errorf("crossed pending failure: %v", seen)
			}
			if m.Offset == 2 {
				if !reflect.DeepEqual(seen, []string{"0", "1", "1", "2"}) {
					t.Errorf("business repeated after commit error: %v", seen)
				}
				cancel()
			}
		}
	}
	err := RunKafkaConsumerBatched(ctx, r, h, "push", &scriptedDLQ{}, BatchCommitConfig{BatchSize: 3, IntervalMillis: 1000})
	// The one-second retry may trigger an earlier timer flush of the completed
	// prefix. Every committed offset must still refer only to a completed record.
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(seen, []string{"0", "1", "1", "2"}) {
		t.Fatalf("business attempts=%v", seen)
	}
}

func TestBatchCommitStopsAtFatalRecordAndDoesNotHandlePrefetchedSuccessor(t *testing.T) {
	r := &batchReaderFake{records: batchRecords(3)}
	var seen []string
	err := RunKafkaConsumerBatched(context.Background(), r, batchHandlerFunc(func(_ context.Context, _, value string) error {
		seen = append(seen, value)
		if value == "1" {
			return &FatalError{Err: errors.New("invariant failed")}
		}
		return nil
	}), "push", &scriptedDLQ{}, BatchCommitConfig{IntervalMillis: 1000})
	if err == nil || !reflect.DeepEqual(seen, []string{"0", "1"}) || len(r.commits) != 0 {
		t.Fatalf("err=%v seen=%v commits=%v", err, seen, r.commits)
	}
}

func TestBatchCommitWaitsForDLQAcknowledgement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	r := &batchReaderFake{records: batchRecords(1)}
	dlq := &scriptedDLQ{errors: 1}
	r.onCommit = func([]kafka.Message) {
		if dlq.successes != 1 {
			t.Error("offset committed before DLQ ACK")
		}
		cancel()
	}
	err := RunKafkaConsumerBatched(ctx, r, batchHandlerFunc(func(context.Context, string, string) error { return permanent("bad_record", errors.New("bad record")) }), "push", dlq, BatchCommitConfig{BatchSize: 1})
	if err != nil || len(r.commits) != 1 || len(dlq.records) != 2 {
		t.Fatalf("err=%v commits=%d DLQ=%d", err, len(r.commits), len(dlq.records))
	}
}

func TestBatchCommitStopsOnGenerationChange(t *testing.T) {
	r := &batchReaderFake{records: batchRecords(3), commitErrors: []error{kafka.RebalanceInProgress}}
	var seen int
	err := RunKafkaConsumerBatched(context.Background(), r, batchHandlerFunc(func(context.Context, string, string) error { seen++; return nil }), "push", &scriptedDLQ{}, BatchCommitConfig{BatchSize: 2, IntervalMillis: 1000})
	if !errors.Is(err, kafka.RebalanceInProgress) || seen != 2 || len(r.commits) != 1 {
		t.Fatalf("err=%v handled=%d commits=%d", err, seen, len(r.commits))
	}
}
