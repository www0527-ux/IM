package config

import (
	"infinitechatgo/common/auth"
	"infinitechatgo/common/mysqlpool"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/rest"
)

type SyncConfig struct {
	DefaultLimit     int `json:",default=100"`
	MaxLimit         int `json:",default=200"`
	MaxConversations int `json:",default=50"`
}

type Config struct {
	rest.RestConf

	DataSource string
	MySQLPool  mysqlpool.Config
	Redis      redis.RedisConf
	Auth       auth.Config
	Sync       SyncConfig
}
