package relay

import (
	"context"
	"errors"
	"testing"
	"time"
)

type batchFunc func(context.Context) (int, error)

func (f batchFunc) RunBatch(ctx context.Context) (int, error) { return f(ctx) }

func TestProductiveBatchContinuesWithoutPollWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	calls := 0
	err := Run(ctx, batchFunc(func(context.Context) (int, error) {
		calls++
		if calls == 3 {
			cancel()
		}
		return 1, nil
	}), time.Hour, nil)
	if err != nil || calls != 3 {
		t.Fatalf("productive work should continue immediately: calls=%d err=%v", calls, err)
	}
}

func TestEmptyAndFailedBatchesWaitAndCancellationStopsLoop(t *testing.T) {
	for _, failure := range []error{nil, errors.New("database unavailable")} {
		t.Run(fmtErrorName(failure), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			calls, reports := 0, 0
			err := Run(ctx, batchFunc(func(context.Context) (int, error) {
				calls++
				if failure != nil {
					return 10, failure
				}
				return 0, nil
			}), time.Hour, func(err error) {
				if !errors.Is(err, failure) {
					t.Errorf("wrong reported error: %v", err)
				}
				reports++
			})
			if err != nil || calls != 1 {
				t.Fatalf("empty/error loop must wait, then honor cancellation: calls=%d err=%v", calls, err)
			}
			if (failure != nil && reports != 1) || (failure == nil && reports != 0) {
				t.Fatalf("unexpected error reports: %d", reports)
			}
		})
	}
}

func TestAlreadyCancelledLoopDoesNotClaim(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Run(ctx, batchFunc(func(context.Context) (int, error) {
		t.Fatal("must not claim on cancelled context")
		return 0, nil
	}), time.Second, nil); err != nil {
		t.Fatal(err)
	}
}

func fmtErrorName(err error) string {
	if err == nil {
		return "empty"
	}
	return "failed"
}
