package groupmembercache

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
)

type Loader func(context.Context, int64) ([]int64, error)

type entry struct {
	id      int64
	ids     []int64
	expires time.Time
}
type flight struct {
	done        chan struct{}
	ids         []int64
	err         error
	invalidated bool
}
type Stats struct {
	Hits             uint64 `json:"hits"`
	Loads            uint64 `json:"loads"`
	Invalidations    uint64 `json:"invalidations"`
	Resets           uint64 `json:"resets"`
	InvalidatedLoads uint64 `json:"invalidatedLoads"`
	Entries          int    `json:"entries"`
}
type Local struct {
	mu        sync.Mutex
	ttl       time.Duration
	capacity  int
	available bool
	entries   map[int64]*list.Element
	lru       *list.List
	loading   map[int64]*flight
	now       func() time.Time
	stats     Stats
}

var errInvalidated = errors.New("group members changed while loading")

func NewLocal(c Config) (*Local, error) {
	c, err := c.WithDefaults()
	if err != nil {
		return nil, err
	}
	return &Local{ttl: time.Duration(c.TTLSeconds) * time.Second, capacity: c.Capacity,
		available: !c.Disabled, entries: make(map[int64]*list.Element), lru: list.New(),
		loading: make(map[int64]*flight), now: time.Now}, nil
}

// Get coalesces concurrent misses. An invalidation also fences an in-flight
// loader: its old result is discarded and callers fetch again within their
// original deadline. Returned slices never alias the cache or another caller.
func (c *Local) Get(ctx context.Context, id int64, load Loader) ([]int64, error) {
	if id <= 0 || load == nil {
		return nil, fmt.Errorf("valid session ID and group member loader are required")
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		c.mu.Lock()
		now := c.now()
		if item := c.entries[id]; item != nil {
			if c.available && now.Before(item.Value.(*entry).expires) {
				c.lru.MoveToFront(item)
				c.stats.Hits++
				ids := slices.Clone(item.Value.(*entry).ids)
				c.mu.Unlock()
				return ids, nil
			}
			c.remove(item)
		}
		if pending := c.loading[id]; pending != nil {
			c.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-pending.done:
				if errors.Is(pending.err, errInvalidated) {
					continue
				}
				return slices.Clone(pending.ids), pending.err
			}
		}
		pending := &flight{done: make(chan struct{})}
		c.loading[id] = pending
		c.stats.Loads++
		c.mu.Unlock()
		ids, err := load(ctx, id)
		if err == nil {
			err = ctx.Err()
		}
		c.mu.Lock()
		delete(c.loading, id)
		if pending.invalidated {
			pending.err = errInvalidated
			c.stats.InvalidatedLoads++
		} else {
			pending.err = err
			if err == nil {
				pending.ids = slices.Clone(ids)
				// Anchor TTL to query start, so a delayed response cannot extend
				// the life of a snapshot obtained before a missed notification.
				expires := now.Add(c.ttl)
				if c.available && c.now().Before(expires) && len(ids) <= 10000 {
					c.entries[id] = c.lru.PushFront(&entry{id: id, ids: slices.Clone(ids), expires: expires})
					if c.lru.Len() > c.capacity {
						c.remove(c.lru.Back())
					}
				}
			}
		}
		close(pending.done)
		c.mu.Unlock()
		if errors.Is(pending.err, errInvalidated) {
			continue
		}
		return slices.Clone(pending.ids), pending.err
	}
}

func (c *Local) remove(item *list.Element) {
	delete(c.entries, item.Value.(*entry).id)
	c.lru.Remove(item)
}
func (c *Local) Invalidate(id int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if item := c.entries[id]; item != nil {
		c.remove(item)
	}
	if pending := c.loading[id]; pending != nil {
		pending.invalidated = true
	}
	c.stats.Invalidations++
}

// SetAvailable clears old snapshots on subscription establishment/reconnection
// and prevents cache population before the initial subscription is ready.
func (c *Local) SetAvailable(available bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.available = available
	c.entries = make(map[int64]*list.Element)
	c.lru.Init()
	for _, pending := range c.loading {
		pending.invalidated = true
	}
	c.stats.Resets++
}
func (c *Local) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	stats := c.stats
	stats.Entries = len(c.entries)
	return stats
}
