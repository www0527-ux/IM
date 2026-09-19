package config

import (
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

type KafkaConfig struct {
	Brokers        []string
	AcceptedTopic  string
	AcceptedGroup  string
	SequencedTopic string
}

type SequenceConfig struct {
	BlockSize              int64
	IdempotencyTTLSeconds  int
	MessageCacheTTLSeconds int
	RefillLockTTLSeconds   int
}

type Config struct {
	service.ServiceConf

	DataSource string
	Redis      redis.RedisConf
	Kafka      KafkaConfig
	Sequence   SequenceConfig
}
