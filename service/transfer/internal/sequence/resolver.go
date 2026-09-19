package sequence

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	messageevent "infinitechatgo/common/model/event"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

const retryRefillInterval = 25 * time.Millisecond

// This script atomically reuses or allocates a Seq. Zero means the local
// segment is absent or exhausted and Go must reserve another MySQL segment.
const allocateSeqLua = `
local existing = redis.call("GET", KEYS[1])
if existing then
    return tonumber(existing)
end

local current = tonumber(redis.call("HGET", KEYS[2], "CURR"))
local last = tonumber(redis.call("HGET", KEYS[2], "LAST"))
if (not current) or (not last) or current >= last then
    return 0
end

local seq = redis.call("HINCRBY", KEYS[2], "CURR", 1)
redis.call("SETEX", KEYS[1], ARGV[1], seq)
return seq
`

// A late refill worker must never overwrite a newer segment with an older one.
const installSegmentLua = `
local currentLast = tonumber(redis.call("HGET", KEYS[1], "LAST"))
local newLast = tonumber(ARGV[2])
if currentLast and currentLast >= newLast then
    return 0
end

redis.call("HSET", KEYS[1], "CURR", ARGV[1], "LAST", ARGV[2])
return 1
`

const segmentHasCapacityLua = `
local current = tonumber(redis.call("HGET", KEYS[1], "CURR"))
local last = tonumber(redis.call("HGET", KEYS[1], "LAST"))
if current and last and current < last then
    return 1
end
return 0
`

const releaseLockLua = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
end
return 0
`

var (
	allocateSeqScript        = redis.NewScript(allocateSeqLua)
	installSegmentScript     = redis.NewScript(installSegmentLua)
	segmentHasCapacityScript = redis.NewScript(segmentHasCapacityLua)
	releaseLockScript        = redis.NewScript(releaseLockLua)
)

type Resolver struct {
	redis                  *redis.Redis
	store                  SegmentStore
	blockSize              int64
	idempotencyTTLSeconds  int
	messageCacheTTLSeconds int
	refillLockTTLSeconds   int
}

func NewResolver(
	rds *redis.Redis,
	store SegmentStore,
	blockSize int64,
	idempotencyTTLSeconds int,
	messageCacheTTLSeconds int,
	refillLockTTLSeconds int,
) (*Resolver, error) {
	if rds == nil {
		return nil, fmt.Errorf("redis cannot be nil")
	}
	if store == nil {
		return nil, fmt.Errorf("sequence segment store cannot be nil")
	}
	if blockSize <= 0 || idempotencyTTLSeconds <= 0 ||
		messageCacheTTLSeconds <= 0 || refillLockTTLSeconds <= 0 {
		return nil, fmt.Errorf("sequence configuration values must be positive")
	}

	return &Resolver{
		redis:                  rds,
		store:                  store,
		blockSize:              blockSize,
		idempotencyTTLSeconds:  idempotencyTTLSeconds,
		messageCacheTTLSeconds: messageCacheTTLSeconds,
		refillLockTTLSeconds:   refillLockTTLSeconds,
	}, nil
}

// Resolve returns a stable conversation Seq for the same MessageID while its
// idempotency key is alive. It refills exhausted Redis segments from MySQL.
func (r *Resolver) Resolve(
	ctx context.Context,
	sessionID int64,
	messageID int64,
) (int64, error) {
	for {
		seq, err := r.allocate(ctx, sessionID, messageID)
		if err != nil {
			return 0, err
		}
		if seq > 0 {
			return seq, nil
		}

		refilled, err := r.tryRefill(ctx, sessionID)
		if err != nil {
			return 0, err
		}
		if refilled {
			continue
		}

		timer := time.NewTimer(retryRefillInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return 0, ctx.Err()
		case <-timer.C:
		}
	}
}

func (r *Resolver) allocate(
	ctx context.Context,
	sessionID int64,
	messageID int64,
) (int64, error) {
	result, err := r.redis.ScriptRunCtx(
		ctx,
		allocateSeqScript,
		[]string{
			messageSeqKey(sessionID, messageID),
			segmentKey(sessionID),
		},
		strconv.Itoa(r.idempotencyTTLSeconds),
	)
	if err != nil {
		return 0, fmt.Errorf("allocate message seq: %w", err)
	}

	seq, ok := result.(int64)
	if !ok || seq < 0 {
		return 0, fmt.Errorf("allocate message seq returned invalid value: %v", result)
	}
	return seq, nil
}

func (r *Resolver) tryRefill(
	ctx context.Context,
	sessionID int64,
) (bool, error) {
	token, err := randomToken()
	if err != nil {
		return false, fmt.Errorf("create sequence refill lock token: %w", err)
	}

	lockKey := refillLockKey(sessionID)
	locked, err := r.redis.SetnxExCtx(
		ctx,
		lockKey,
		token,
		r.refillLockTTLSeconds,
	)
	if err != nil {
		return false, fmt.Errorf("acquire sequence refill lock: %w", err)
	}
	if !locked {
		return false, nil
	}
	defer func() {
		_, _ = r.redis.ScriptRunCtx(
			context.Background(),
			releaseLockScript,
			[]string{lockKey},
			token,
		)
	}()

	// Another worker may have installed a segment just before this lock was
	// acquired, so check again before touching MySQL.
	available, err := r.segmentHasCapacity(ctx, sessionID)
	if err != nil {
		return false, err
	}
	if available {
		return true, nil
	}

	segment, err := r.store.Reserve(ctx, sessionID, r.blockSize)
	if err != nil {
		return false, fmt.Errorf("reserve sequence segment: %w", err)
	}

	_, err = r.redis.ScriptRunCtx(
		ctx,
		installSegmentScript,
		[]string{segmentKey(sessionID)},
		strconv.FormatInt(segment.StartExclusive, 10),
		strconv.FormatInt(segment.EndInclusive, 10),
	)
	if err != nil {
		return false, fmt.Errorf("install sequence segment: %w", err)
	}
	return true, nil
}

func (r *Resolver) segmentHasCapacity(
	ctx context.Context,
	sessionID int64,
) (bool, error) {
	result, err := r.redis.ScriptRunCtx(
		ctx,
		segmentHasCapacityScript,
		[]string{segmentKey(sessionID)},
	)
	if err != nil {
		return false, fmt.Errorf("check sequence segment: %w", err)
	}
	value, ok := result.(int64)
	if !ok {
		return false, fmt.Errorf("check sequence segment returned invalid value: %v", result)
	}
	return value == 1, nil
}

// Cache stores one complete event under one key, giving each message its own
// TTL. Offline sync can later fetch a Seq range with MGET and fall back to DB.
func (r *Resolver) Cache(
	ctx context.Context,
	event messageevent.ChatMessageSequencedEvent,
) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal sequenced message for cache: %w", err)
	}

	if err := r.redis.SetexCtx(
		ctx,
		messageCacheKey(event.Message.SessionID, event.Message.Seq),
		string(payload),
		r.messageCacheTTLSeconds,
	); err != nil {
		return fmt.Errorf("cache sequenced message: %w", err)
	}
	return nil
}

func randomToken() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func segmentKey(sessionID int64) string {
	return fmt.Sprintf("im:seq-segment:{%d}", sessionID)
}

func refillLockKey(sessionID int64) string {
	return fmt.Sprintf("im:seq-segment-lock:{%d}", sessionID)
}

func messageSeqKey(sessionID, messageID int64) string {
	return fmt.Sprintf("im:message-seq:{%d}:%d", sessionID, messageID)
}

func messageCacheKey(sessionID, seq int64) string {
	return fmt.Sprintf("im:message-cache:{%d}:%d", sessionID, seq)
}
