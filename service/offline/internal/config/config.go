package config

import (
	"infinitechatgo/common/auth"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/rest"
)

type KafkaConfig struct {
	Brokers        []string
	SequencedTopic string
	StoreGroup     string
}

type SyncConfig struct {
	DefaultLimit     int `json:",default=100"`
	MaxLimit         int `json:",default=200"`
	MaxConversations int `json:",default=50"`
}

type Config struct {
	rest.RestConf

	DataSource string
	Redis      redis.RedisConf
	Auth       auth.Config
	Kafka      KafkaConfig
	Sync       SyncConfig
}
