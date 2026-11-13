package groupmembercache

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"infinitechatgo/common/txctx"
)

type Invalidator interface {
	Invalidate(context.Context, int64) error
}

type Notifier struct {
	client  redis.UniversalClient
	channel string
}

func NewNotifier(client redis.UniversalClient, c InvalidationConfig) *Notifier {
	return &Notifier{client: client, channel: c.ChannelName()}
}

// Invalidate must run after the membership transaction commits. A failed
// notification cannot roll back MySQL; readers converge through absolute TTL.
func (n *Notifier) Invalidate(ctx context.Context, id int64) error {
	if n == nil || n.client == nil || id <= 0 {
		return fmt.Errorf("group member notifier dependencies are required")
	}
	if txctx.GetSession(ctx) != nil {
		return fmt.Errorf("group member invalidation must run outside the transaction")
	}
	return n.client.Publish(ctx, n.channel, strconv.FormatInt(id, 10)).Err()
}

type Subscription struct {
	pubsub    *redis.PubSub
	done      chan struct{}
	ready     chan struct{}
	readyOnce sync.Once
}

// Subscribe uses Redis Pub/Sub for prompt eviction, not durable delivery. Each
// (re)subscription clears all local entries; missed events are bounded by TTL.
func Subscribe(ctx context.Context, client redis.UniversalClient, channel string, local *Local) *Subscription {
	local.SetAvailable(false)
	s := &Subscription{pubsub: client.Subscribe(ctx, channel), done: make(chan struct{}), ready: make(chan struct{})}
	stream := s.pubsub.ChannelWithSubscriptions(redis.WithChannelHealthCheckInterval(5*time.Second), redis.WithChannelSendTimeout(time.Second))
	go func() {
		defer close(s.done)
		defer local.SetAvailable(false)
		defer s.pubsub.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-stream:
				if !ok {
					return
				}
				switch event := event.(type) {
				case *redis.Subscription:
					if event.Kind == "subscribe" && event.Channel == channel {
						local.SetAvailable(true)
						s.readyOnce.Do(func() { close(s.ready) })
					}
				case *redis.Message:
					id, err := strconv.ParseInt(event.Payload, 10, 64)
					if err != nil || id <= 0 {
						log.Printf("invalid group member cache notification")
						continue
					}
					local.Invalidate(id)
				}
			}
		}
	}()
	return s
}
func (s *Subscription) Ready() <-chan struct{} { return s.ready }
func (s *Subscription) Close() {
	_ = s.pubsub.Close()
	<-s.done
}
