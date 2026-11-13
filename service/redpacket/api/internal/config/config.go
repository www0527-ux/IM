// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package config

import (
	"infinitechatgo/common/auth"
	"infinitechatgo/common/idgen"
	"infinitechatgo/common/mysqlpool"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type KafkaConfig struct {
	Brokers                     []string
	PersistedTopic              string `json:",default=message-persisted-topic"`
	MessageCreateRequestedTopic string
	CacheBuildTopic             string
}

type Config struct {
	rest.RestConf
	DataSource string
	MySQLPool  mysqlpool.Config
	Redis      redis.RedisConf
	Auth       auth.Config
	Snowflake  idgen.Config
	UserRpc    zrpc.RpcClientConf
	Kafka      KafkaConfig
}
