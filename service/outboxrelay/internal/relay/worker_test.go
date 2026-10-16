package relay

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"
)

type recoveryStore struct {
	states            map[int64]int // 0 pending, 1 leased, 2 published
	retrying          map[int64]bool
	markFailure       bool
	commitBeforeError bool
	releaseFailure    bool
	releases          int
	checkpoints       [][]int64
}

func (s *recoveryStore) ClaimBatch(context.Context, string, int, time.Time) ([]Event, error) {
	var events []Event
	for id := int64(1); id <= int64(len(s.states)); id++ {
		if s.states[id] == 0 {
			s.states[id] = 1
			events = append(events, Event{EventID: id, Payload: strconv.FormatInt(id, 10)})
		}
	}
	return events, nil
}
func (s *recoveryStore) MarkPublishedBatch(_ context.Context, ids []int64, _ string, _ time.Time) error {
	s.checkpoints = append(s.checkpoints, append([]int64(nil), ids...))
	if s.markFailure && !s.commitBeforeError {
		s.markFailure = false
		return errors.New("checkpoint failed")
	}
	for _, id := range ids {
		s.states[id] = 2
	}
	if s.markFailure {
		s.markFailure = false
		return errors.New("COMMIT response lost")
	}
	return nil
}
func (s *recoveryStore) Reschedule(_ context.Context, id int64, _ string, _ time.Time, _ string) error {
	s.states[id] = 0
	s.retrying[id] = true
	return nil
}
func (s *recoveryStore) ReleaseBatch(ctx context.Context, ids []int64, _ string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	s.releases++
	if s.releaseFailure {
		return errors.New("release unavailable")
	}
	for _, id := range ids {
		if s.states[id] == 1 {
			s.states[id] = 0
		}
	}
	return nil
}

type recoveryPublisher struct {
	received []int64
	failAt   int64
	cancel   context.CancelFunc
}

func (p *recoveryPublisher) Publish(_ context.Context, _, _, payload string) error {
	id, _ := strconv.ParseInt(payload, 10, 64)
	p.received = append(p.received, id)
	if p.cancel != nil {
		p.cancel()
	}
	if id == p.failAt {
		return errors.New("broker accepted but ACK unknown")
	}
	return nil
}
func fixtureWorker(n int, checkpoint int) (*Worker, *recoveryStore, *recoveryPublisher) {
	s := &recoveryStore{states: map[int64]int{}, retrying: map[int64]bool{}}
	for id := int64(1); id <= int64(n); id++ {
		s.states[id] = 0
	}
	p := &recoveryPublisher{}
	return NewWorker(s, p, Options{WorkerID: "test", BatchSize: 500, CheckpointSize: checkpoint, LeaseDuration: time.Minute, MaxRetryBackoff: time.Minute}), s, p
}

func TestPublishFailureConfirmsOnlyACKedPrefixAndStopsSuccessors(t *testing.T) {
	w, s, p := fixtureWorker(6, 4)
	p.failAt = 3
	if n, err := w.RunBatch(context.Background()); n != 6 || err == nil {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if !reflect.DeepEqual(p.received, []int64{1, 2, 3}) || !reflect.DeepEqual(s.checkpoints, [][]int64{{1, 2}}) {
		t.Fatalf("received=%v checkpoints=%v", p.received, s.checkpoints)
	}
	for id := int64(1); id <= 6; id++ {
		want := 0
		if id < 3 {
			want = 2
		}
		if s.states[id] != want {
			t.Fatal(s.states)
		}
	}
	if !s.retrying[3] || s.releases != 1 {
		t.Fatal("failed event must retain retry and successors must release")
	}
}

func TestCheckpointFailureAndUnknownCommitRecoverWithoutSkipping(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(strconv.FormatBool(committed), func(t *testing.T) {
			w, s, p := fixtureWorker(4, 2)
			s.markFailure = true
			s.commitBeforeError = committed
			if _, err := w.RunBatch(context.Background()); err == nil {
				t.Fatal("missing checkpoint error")
			}
			if !reflect.DeepEqual(p.received, []int64{1, 2}) {
				t.Fatal("published beyond unconfirmed checkpoint")
			}
			if _, err := w.RunBatch(context.Background()); err != nil {
				t.Fatal(err)
			}
			for _, state := range s.states {
				if state != 2 {
					t.Fatal(s.states)
				}
			}
			want := []int64{1, 2, 1, 2, 3, 4}
			if committed {
				want = []int64{1, 2, 3, 4}
			}
			if !reflect.DeepEqual(p.received, want) {
				t.Fatal(p.received)
			}
		})
	}
}

func TestCancellationReleaseFailureAndLeaseExpiryKeepRecoveryRecords(t *testing.T) {
	w, s, p := fixtureWorker(3, 20)
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	if _, err := w.RunBatch(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if s.releases != 1 || len(s.checkpoints) != 0 || len(p.received) != 1 {
		t.Fatal("cancelled worker must release using independent context")
	}
	for _, state := range s.states {
		if state != 0 {
			t.Fatal(s.states)
		}
	}
	w, s, p = fixtureWorker(3, 20)
	s.releaseFailure = true
	p.failAt = 1
	if _, err := w.RunBatch(context.Background()); err == nil {
		t.Fatal("missing release error")
	}
	if s.states[1] != 0 || s.states[2] != 1 || s.states[3] != 1 {
		t.Fatal("unreleased events must retain persistent leases")
	}
	w, s, p = fixtureWorker(3, 20)
	w.options.LeaseDuration = -time.Second
	if _, err := w.RunBatch(context.Background()); err == nil || len(p.received) != 0 {
		t.Fatal("expired lease must prevent any publish")
	}
}
