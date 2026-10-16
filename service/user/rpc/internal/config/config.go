package config

import (
	"github.com/zeromicro/go-zero/core/stores/cache"
	"github.com/zeromicro/go-zero/zrpc"
	"infinitechatgo/common/mysqlpool"
)

type Config struct {
	zrpc.RpcServerConf //包含etcd的配置
	DataSource         string
	MySQLPool          mysqlpool.Config
	Cache              cache.CacheConf
}
