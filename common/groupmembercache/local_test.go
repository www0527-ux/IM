package groupmembercache

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestLocal(t *testing.T, c Config) *Local {
	t.Helper()
	local, err := NewLocal(c)
	if err != nil {
		t.Fatal(err)
	}
	return local
}

func TestLocalAbsoluteTTLAndIndependentResults(t *testing.T) {
	c := newTestLocal(t, Config{TTLSeconds: 5})
	now := time.Now()
	c.now = func() time.Time { return now }
	calls := 0
	load := func(context.Context, int64) ([]int64, error) { calls++; return []int64{int64(calls)}, nil }
	first, err := c.Get(context.Background(), 9, load)
	if err != nil {
		t.Fatal(err)
	}
	first[0] = 100
	now = now.Add(4 * time.Second)
	second, _ := c.Get(context.Background(), 9, load)
	if !reflect.DeepEqual(second, []int64{1}) || calls != 1 {
		t.Fatalf("ids=%v calls=%d", second, calls)
	}
	now = now.Add(time.Second)
	third, _ := c.Get(context.Background(), 9, load)
	if !reflect.DeepEqual(third, []int64{2}) || calls != 2 {
		t.Fatalf("TTL slid on access: ids=%v calls=%d", third, calls)
	}
}

func TestLocalInvalidationFencesInFlightQuery(t *testing.T) {
	c := newTestLocal(t, Config{})
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	load := func(context.Context, int64) ([]int64, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-release
			return []int64{1, 2}, nil
		}
		return []int64{1, 3}, nil
	}
	result := make(chan []int64, 1)
	go func() {
		ids, err := c.Get(context.Background(), 9, load)
		if err != nil {
			result <- nil
		} else {
			result <- ids
		}
	}()
	<-started
	c.Invalidate(9)
	close(release)
	select {
	case ids := <-result:
		if !reflect.DeepEqual(ids, []int64{1, 3}) {
			t.Fatalf("returned stale in-flight result %v", ids)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("query did not finish")
	}
	ids, _ := c.Get(context.Background(), 9, load)
	if !reflect.DeepEqual(ids, []int64{1, 3}) || calls.Load() != 2 || c.Stats().InvalidatedLoads != 1 {
		t.Fatalf("ids=%v stats=%+v", ids, c.Stats())
	}
}

func TestLocalConcurrentMissesCoalesceAndWaiterCanCancel(t *testing.T) {
	c := newTestLocal(t, Config{})
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	load := func(context.Context, int64) ([]int64, error) {
		calls.Add(1)
		close(started)
		<-release
		return []int64{3}, nil
	}
	var wg sync.WaitGroup
	wg.Go(func() { _, _ = c.Get(context.Background(), 9, load) })
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := c.Get(ctx, 9, load); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		wg.Go(func() {
			ids, err := c.Get(context.Background(), 9, load)
			if err != nil || !reflect.DeepEqual(ids, []int64{3}) {
				t.Errorf("ids=%v err=%v", ids, err)
			}
		})
	}
	close(release)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("RPC calls=%d", calls.Load())
	}
}

func TestLocalFailuresAreNotCachedAndEmptySuccessIsCached(t *testing.T) {
	c := newTestLocal(t, Config{})
	calls := 0
	unavailable := errors.New("RPC unavailable")
	load := func(context.Context, int64) ([]int64, error) {
		calls++
		if calls == 1 {
			return nil, unavailable
		}
		return []int64{}, nil
	}
	if _, err := c.Get(context.Background(), 9, load); !errors.Is(err, unavailable) {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		ids, err := c.Get(context.Background(), 9, load)
		if err != nil || len(ids) != 0 {
			t.Fatalf("ids=%v err=%v", ids, err)
		}
	}
	if calls != 2 {
		t.Fatalf("calls=%d", calls)
	}
}

func TestLocalLRUBoundsAndUnavailableBypassesCache(t *testing.T) {
	c := newTestLocal(t, Config{Capacity: 2})
	calls := map[int64]int{}
	load := func(_ context.Context, id int64) ([]int64, error) { calls[id]++; return []int64{id}, nil }
	for _, id := range []int64{1, 2, 1, 3, 2} {
		_, _ = c.Get(context.Background(), id, load)
	}
	if calls[1] != 1 || calls[2] != 2 || c.Stats().Entries != 2 {
		t.Fatalf("calls=%v stats=%+v", calls, c.Stats())
	}
	c.SetAvailable(false)
	for i := 0; i < 2; i++ {
		_, _ = c.Get(context.Background(), 1, load)
	}
	if calls[1] != 3 || c.Stats().Entries != 0 {
		t.Fatalf("calls=%v stats=%+v", calls, c.Stats())
	}
	c.SetAvailable(true)
	_, _ = c.Get(context.Background(), 1, load)
	_, _ = c.Get(context.Background(), 1, load)
	if calls[1] != 4 {
		t.Fatalf("calls=%v", calls)
	}
}

func TestSlowQueryDoesNotExtendSnapshotTTL(t *testing.T) {
	c := newTestLocal(t, Config{TTLSeconds: 1})
	now := time.Now()
	c.now = func() time.Time { return now }
	_, err := c.Get(context.Background(), 9, func(context.Context, int64) ([]int64, error) { now = now.Add(2 * time.Second); return []int64{1}, nil })
	if err != nil || c.Stats().Entries != 0 {
		t.Fatalf("err=%v stats=%+v", err, c.Stats())
	}
}
