// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package config

import (
	"infinitechatgo/common/auth"
	"infinitechatgo/common/idgen"

	"github.com/zeromicro/go-zero/core/stores/cache"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/rest"
)

type EmailConf struct {
	Host     string
	Port     int
	Username string
	Password string
	FromName string
	Subject  string
}
type MinioConf struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	UseSSL    bool
	Bucket    string
}
type Config struct {
	rest.RestConf
	DataSource string
	Cache      cache.CacheConf
	Redis      redis.RedisConf
	Email      EmailConf
	Auth       auth.Config
	Snowflake  idgen.Config
	Minio      MinioConf
}
