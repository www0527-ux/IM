package application

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	zeroredis "github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

// CandidatePool is a disposable projection of unclaimed share IDs. MySQL
// validates every candidate before it changes any balance.
type CandidatePool interface {
	Pop(ctx context.Context, redPacketID int64) (int64, error)
	Rebuild(ctx context.Context, redPacketID int64) error
	Remove(ctx context.Context, redPacketID int64) error
}

type RedisCandidatePool struct {
	redis *zeroredis.Redis
	db    sqlx.SqlConn
}

func NewRedisCandidatePool(redis *zeroredis.Redis, db sqlx.SqlConn) *RedisCandidatePool {
	return &RedisCandidatePool{redis: redis, db: db}
}

func candidateKey(redPacketID int64) string {
	return "redpacket:{" + strconv.FormatInt(redPacketID, 10) + "}:available-share-ids"
}

const popCandidateScript = `return redis.call('RPOP', KEYS[1])`

func (p *RedisCandidatePool) Pop(ctx context.Context, redPacketID int64) (int64, error) {
	if p == nil || p.redis == nil || redPacketID <= 0 {
		return 0, fmt.Errorf("candidate pool is unavailable")
	}
	result, err := p.redis.EvalCtx(ctx, popCandidateScript, []string{candidateKey(redPacketID)})
	if errors.Is(err, redis.Nil) || result == nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	value, ok := result.(string)
	if !ok {
		return 0, fmt.Errorf("candidate pool returned %T", result)
	}
	id, err := strconv.ParseInt(value, 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("candidate pool returned invalid share ID %q", value)
	}
	return id, nil
}

const replaceCandidateScript = `
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
redis.call('RENAME', KEYS[1], KEYS[2])
redis.call('EXPIRE', KEYS[2], ARGV[1])
return 1`

func (p *RedisCandidatePool) Rebuild(ctx context.Context, redPacketID int64) error {
	if p == nil || p.redis == nil || p.db == nil || redPacketID <= 0 {
		return fmt.Errorf("candidate pool dependencies are missing")
	}
	var packet struct {
		Status   int64     `db:"status"`
		Version  int64     `db:"version"`
		ExpireAt time.Time `db:"expire_at"`
	}
	const packetQuery = `SELECT status, version, expire_at FROM red_packet WHERE red_packet_id = ?`
	if err := p.db.QueryRowCtx(ctx, &packet, packetQuery, redPacketID); err != nil {
		return fmt.Errorf("load packet for cache rebuild: %w", err)
	}
	if packet.Status != redPacketActiveStatus || !packet.ExpireAt.After(time.Now()) {
		return p.Remove(ctx, redPacketID)
	}
	var shares []struct {
		ShareID int64 `db:"share_id"`
	}
	const shareQuery = `SELECT share_id FROM red_packet_share WHERE red_packet_id = ? AND status = 0 ORDER BY share_id`
	if err := p.db.QueryRowsCtx(ctx, &shares, shareQuery, redPacketID); err != nil {
		return fmt.Errorf("load unclaimed shares: %w", err)
	}
	suffix := make([]byte, 12)
	if _, err := rand.Read(suffix); err != nil {
		return fmt.Errorf("generate candidate temporary key: %w", err)
	}
	key := candidateKey(redPacketID)
	temporary := key + ":build:" + hex.EncodeToString(suffix)
	defer func() { _, _ = p.redis.DelCtx(context.Background(), temporary) }()
	if len(shares) > 0 {
		values := make([]any, 0, len(shares))
		for _, share := range shares {
			values = append(values, strconv.FormatInt(share.ShareID, 10))
		}
		if _, err := p.redis.RpushCtx(ctx, temporary, values...); err != nil {
			return fmt.Errorf("write candidate temporary list: %w", err)
		}
		if err := p.redis.ExpireCtx(ctx, temporary, 60); err != nil {
			return fmt.Errorf("expire candidate temporary list: %w", err)
		}
	}
	var latest struct {
		Status   int64     `db:"status"`
		Version  int64     `db:"version"`
		ExpireAt time.Time `db:"expire_at"`
	}
	if err := p.db.QueryRowCtx(ctx, &latest, packetQuery, redPacketID); err != nil {
		return fmt.Errorf("recheck packet cache version: %w", err)
	}
	if latest.Status != redPacketActiveStatus || latest.Version != packet.Version ||
		!latest.ExpireAt.After(time.Now()) {
		return fmt.Errorf("packet %d changed during candidate rebuild", redPacketID)
	}
	if len(shares) == 0 {
		return p.Remove(ctx, redPacketID)
	}
	ttl := int(time.Until(latest.ExpireAt).Seconds())
	if ttl < 1 {
		ttl = 1
	}
	if _, err := p.redis.EvalCtx(ctx, replaceCandidateScript, []string{temporary, key}, ttl); err != nil {
		return fmt.Errorf("replace candidate list: %w", err)
	}
	return nil
}

func (p *RedisCandidatePool) Remove(ctx context.Context, redPacketID int64) error {
	if p == nil || p.redis == nil || redPacketID <= 0 {
		return fmt.Errorf("candidate pool is unavailable")
	}
	_, err := p.redis.DelCtx(ctx, candidateKey(redPacketID))
	return err
}

// RebuildIfMismatch is a low-frequency repair path for candidates lost after
// a Redis pop but before a MySQL claim commit.
func (p *RedisCandidatePool) RebuildIfMismatch(ctx context.Context, redPacketID int64) error {
	var unclaimed int
	if err := p.db.QueryRowCtx(ctx, &unclaimed,
		`SELECT COUNT(1) FROM red_packet_share WHERE red_packet_id = ? AND status = 0`, redPacketID); err != nil {
		return err
	}
	count, err := p.redis.LlenCtx(ctx, candidateKey(redPacketID))
	if err != nil {
		return err
	}
	if count != unclaimed {
		return p.Rebuild(ctx, redPacketID)
	}
	return nil
}
