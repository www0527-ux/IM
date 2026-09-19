package svc

import (
	"fmt"

	"infinitechatgo/service/transfer/internal/config"
	"infinitechatgo/service/transfer/internal/mq"
	"infinitechatgo/service/transfer/internal/sequence"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type ServiceContext struct {
	Config config.Config

	Redis              *redis.Redis
	SequenceResolver   *sequence.Resolver
	SequencedPublisher *mq.SequencedMessagePublisher
}

func NewServiceContext(c config.Config) (*ServiceContext, error) {
	rds := redis.MustNewRedis(c.Redis)
	conn := sqlx.NewMysql(c.DataSource)
	segmentStore := sequence.NewMySQLSegmentStore(conn)
	resolver, err := sequence.NewResolver(
		rds,
		segmentStore,
		c.Sequence.BlockSize,
		c.Sequence.IdempotencyTTLSeconds,
		c.Sequence.MessageCacheTTLSeconds,
		c.Sequence.RefillLockTTLSeconds,
	)
	if err != nil {
		return nil, fmt.Errorf("create sequence resolver: %w", err)
	}

	publisher, err := mq.NewSequencedMessagePublisher(c.Kafka)
	if err != nil {
		return nil, fmt.Errorf("create sequenced message publisher: %w", err)
	}

	return &ServiceContext{
		Config:             c,
		Redis:              rds,
		SequenceResolver:   resolver,
		SequencedPublisher: publisher,
	}, nil
}

func (s *ServiceContext) Close() error {
	return s.SequencedPublisher.Close()
}
