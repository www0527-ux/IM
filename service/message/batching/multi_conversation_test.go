package batching

import (
	"context"
	"infinitechatgo/service/message/application"
	"testing"
)

func TestShardHashUsesTimeBitsOfSnowflakeIDs(t *testing.T) {
	seen := map[uint64]bool{}
	for i := int64(0); i < 64; i++ {
		id := (int64(1) << 50) + i*(int64(1)<<22)
		seen[conversationShard(id, 8)] = true
	}
	if len(seen) < 6 {
		t.Fatalf("Snowflake-shaped IDs concentrated in %d shards", len(seen))
	}
}

func TestWorkerSharesOneCommitAcrossCollectedOrdinaryConversations(t *testing.T) {
	calls := 0
	store := testStore{batch: func(_ context.Context, commands []application.Command) ([]application.Result, error) {
		calls++
		if len(commands) != 4 {
			t.Errorf("batch has %d requests", len(commands))
		}
		return committed(commands), nil
	}}
	queue := make(chan *request, 4)
	requests := []*request{testRequest(t, 1, "a"), testRequest(t, 2, "x"), testRequest(t, 1, "b"), testRequest(t, 2, "y")}
	for _, r := range requests {
		queue <- r
	}
	close(queue)
	w := &Writer{store: store, config: Config{BatchSize: 4, FlushMillis: 2, TimeoutMillis: 1000}}
	w.workers.Add(1)
	go w.run(queue)
	for _, r := range requests {
		if out := receive(t, r.reply); out.err != nil {
			t.Fatal(out.err)
		}
	}
	w.workers.Wait()
	if calls != 1 {
		t.Fatalf("batch transactions=%d", calls)
	}
}
