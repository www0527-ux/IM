package svc

import (
	"infinitechatgo/common/auth"
	"infinitechatgo/common/mysqlpool"
	"infinitechatgo/service/offline/internal/config"
	"infinitechatgo/service/offline/internal/syncer"
	"infinitechatgo/service/offline/model/message"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

type ServiceContext struct {
	Config        config.Config
	Authenticator *auth.Authenticator
	SyncReader    *syncer.Reader
}

func NewServiceContext(c config.Config) *ServiceContext {
	conn := mysqlpool.MustNewMysql(c.DataSource, c.MySQLPool)
	rds := redis.MustNewRedis(c.Redis)
	messageModel := message.NewMessageModel(conn)
	sessionAccess := syncer.NewMySQLSessionAccessStore(conn)

	return &ServiceContext{
		Config:        c,
		Authenticator: auth.NewAuthenticator(rds, c.Auth),
		SyncReader:    syncer.NewReader(rds, messageModel, sessionAccess),
	}
}
