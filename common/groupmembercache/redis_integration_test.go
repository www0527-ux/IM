//go:build integration

package groupmembercache

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func await(t *testing.T, test func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if test() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}

func TestRedisBroadcastInvalidatesTwoLocalCachesAndReconnectClears(t *testing.T) {
	addr := os.Getenv("IM_GROUP_CACHE_TEST_REDIS")
	if addr == "" {
		t.Skip("set IM_GROUP_CACHE_TEST_REDIS for disposable-channel Redis tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	channel := fmt.Sprintf("im-group-cache-test:%d", time.Now().UnixNano())
	name := fmt.Sprintf("im-group-cache-test-client-%d", time.Now().UnixNano())
	clients := []*redis.Client{
		redis.NewClient(&redis.Options{Addr: addr, Password: os.Getenv("IM_GROUP_CACHE_TEST_REDIS_PASSWORD"), ClientName: name + "-1"}),
		redis.NewClient(&redis.Options{Addr: addr, Password: os.Getenv("IM_GROUP_CACHE_TEST_REDIS_PASSWORD"), ClientName: name + "-2"}),
	}
	for _, client := range clients {
		defer client.Close()
	}
	locals := []*Local{newTestLocal(t, Config{TTLSeconds: 1}), newTestLocal(t, Config{TTLSeconds: 1})}
	subs := []*Subscription{Subscribe(ctx, clients[0], channel, locals[0]), Subscribe(ctx, clients[1], channel, locals[1])}
	for _, sub := range subs {
		defer sub.Close()
		select {
		case <-sub.Ready():
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	ids := []int64{11, 22}
	calls := 0
	load := func(context.Context, int64) ([]int64, error) { calls++; return ids, nil }
	for _, local := range locals {
		_, _ = local.Get(ctx, 99, load)
		_, _ = local.Get(ctx, 99, load)
	}
	if calls != 2 {
		t.Fatalf("warm RPC calls=%d", calls)
	}
	ids = []int64{11, 33}
	notifier := NewNotifier(clients[0], InvalidationConfig{Channel: channel})
	if err := notifier.Invalidate(ctx, 99); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return locals[0].Stats().Invalidations > 0 && locals[1].Stats().Invalidations > 0 })
	for _, local := range locals {
		got, err := local.Get(ctx, 99, load)
		if err != nil || !reflect.DeepEqual(got, ids) {
			t.Fatalf("ids=%v err=%v", got, err)
		}
	}
	if calls != 4 {
		t.Fatalf("refreshed RPC calls=%d", calls)
	}
	// Remove only this test's named subscriber TCP connection. The server and
	// other Redis clients remain running; automatic resubscription must reset L1.
	before := locals[0].Stats().Resets
	list, err := clients[1].ClientList(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	killed := false
	for _, line := range strings.Split(list, "\n") {
		if !strings.Contains(line, "name="+name+"-1 ") || !strings.Contains(line, "flags=P") {
			continue
		}
		for _, field := range strings.Fields(line) {
			if strings.HasPrefix(field, "id=") {
				if _, err := clients[1].ClientKillByFilter(ctx, "ID", strings.TrimPrefix(field, "id=")).Result(); err != nil {
					t.Fatal(err)
				}
				killed = true
			}
		}
	}
	if !killed {
		t.Fatal("owned subscriber connection not found")
	}
	await(t, func() bool { return locals[0].Stats().Resets > before })
	if locals[0].Stats().Entries != 0 {
		t.Fatal("reconnect retained old entries")
	}
	// A deliberately missed invalidation still converges after absolute TTL.
	_, _ = locals[0].Get(ctx, 99, load)
	ids = []int64{11, 44}
	time.Sleep(1100 * time.Millisecond)
	got, err := locals[0].Get(ctx, 99, load)
	if err != nil || !reflect.DeepEqual(got, ids) {
		t.Fatalf("missed notification not recovered: ids=%v err=%v", got, err)
	}
}
