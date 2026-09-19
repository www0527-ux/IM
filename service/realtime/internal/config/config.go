package config

import (
	"infinitechatgo/common/auth"

	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type KafkaConfig struct {
	Brokers        []string
	AcceptedTopic  string
	SequencedTopic string
	PushGroup      string
}

type Config struct {
	service.ServiceConf

	ListenOn string
	Redis    redis.RedisConf
	Auth     auth.Config
	UserRpc  zrpc.RpcClientConf
	Kafka    KafkaConfig
}
