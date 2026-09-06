package config

import (
	"infinitechatgo/common/auth"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	ListenOn string
	Redis    redis.RedisConf
	Auth     auth.Config
	UserRpc  zrpc.RpcClientConf
}
