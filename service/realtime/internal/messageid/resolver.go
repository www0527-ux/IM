package messageid

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	commonutils "infinitechatgo/common/utils"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

const (
	idempotencyKeyPrefix  = "im:message:idempotency:v2:"
	idempotencyTTLSeconds = 7 * 24 * 60 * 60
)

var ErrIdempotencyConflict = errors.New("message idempotency conflict")

type Identity struct {
	MessageID   int64  `json:"messageId,string"`
	CreatedAt   int64  `json:"createdAt"`
	Fingerprint string `json:"fingerprint"`
}

type Resolver struct {
	rds *redis.Redis
}

func NewResolver(rds *redis.Redis) *Resolver {
	return &Resolver{rds: rds}
}

func (r *Resolver) GetOrCreate(
	ctx context.Context,
	senderID int64,
	clientMessageID string,
	fingerprint string,
) (Identity, error) {
	if fingerprint == "" {
		return Identity{}, fmt.Errorf("message fingerprint cannot be empty")
	}

	messageID, err := commonutils.NextID()
	if err != nil {
		return Identity{}, fmt.Errorf("generate message id: %w", err)
	}
	candidate := Identity{
		MessageID:   messageID,
		CreatedAt:   time.Now().UnixMilli(),
		Fingerprint: fingerprint,
	}

	value, err := json.Marshal(candidate)
	if err != nil {
		return Identity{}, fmt.Errorf("marshal message identity: %w", err)
	}

	key := buildKey(senderID, clientMessageID)
	created, err := r.rds.SetnxExCtx(
		ctx,
		key,
		string(value),
		idempotencyTTLSeconds,
	)
	if err != nil {
		return Identity{}, fmt.Errorf("reserve message identity: %w", err)
	}
	if created {
		return candidate, nil
	}

	storedValue, err := r.rds.GetCtx(ctx, key)
	if err != nil {
		return Identity{}, fmt.Errorf("get existing message identity: %w", err)
	}

	var existing Identity
	if err := json.Unmarshal([]byte(storedValue), &existing); err != nil {
		return Identity{}, fmt.Errorf("unmarshal existing message identity: %w", err)
	}
	if existing.MessageID <= 0 || existing.CreatedAt <= 0 ||
		existing.Fingerprint == "" {
		return Identity{}, fmt.Errorf("invalid stored message identity")
	}
	if existing.Fingerprint != fingerprint {
		return Identity{}, fmt.Errorf(
			"%w: sender_id=%d client_message_id=%q",
			ErrIdempotencyConflict,
			senderID,
			clientMessageID,
		)
	}

	return existing, nil
}

func buildKey(senderID int64, clientMessageID string) string {
	return idempotencyKeyPrefix +
		strconv.FormatInt(senderID, 10) +
		":" +
		clientMessageID
}
