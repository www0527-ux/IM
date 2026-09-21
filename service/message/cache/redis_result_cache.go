package cache

import (
	"context"
	"encoding/json"
	"fmt"

	"infinitechatgo/common/messagecache"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

type RedisResultCache struct {
	redis *redis.Redis
}

func NewRedisResultCache(rds *redis.Redis) *RedisResultCache {
	return &RedisResultCache{redis: rds}
}

func (c *RedisResultCache) Lookup(
	ctx context.Context,
	senderID int64,
	clientMessageID string,
) (messagecache.IdempotencyEntry, bool, error) {
	if c == nil || c.redis == nil {
		return messagecache.IdempotencyEntry{}, false, nil
	}

	value, err := c.redis.GetCtx(
		ctx,
		messagecache.IdempotencyKey(senderID, clientMessageID),
	)
	if err != nil || value == "" {
		return messagecache.IdempotencyEntry{}, false, err
	}

	var entry messagecache.IdempotencyEntry
	if err := json.Unmarshal([]byte(value), &entry); err != nil {
		return messagecache.IdempotencyEntry{}, false,
			fmt.Errorf("decode message idempotency cache: %w", err)
	}
	if !entry.Valid() {
		return messagecache.IdempotencyEntry{}, false,
			fmt.Errorf("decode message idempotency cache: invalid entry")
	}
	return entry, true, nil
}
