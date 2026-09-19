package svc

import (
	"infinitechatgo/common/auth"
	"infinitechatgo/service/offline/internal/config"
	"infinitechatgo/service/offline/internal/syncer"
	"infinitechatgo/service/offline/model/message"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type ServiceContext struct {
	Config        config.Config
	MessageModel  message.MessageModel
	Authenticator *auth.Authenticator
	SyncReader    *syncer.Reader
}

func NewServiceContext(c config.Config) *ServiceContext {
	conn := sqlx.NewMysql(c.DataSource)
	rds := redis.MustNewRedis(c.Redis)
	messageModel := message.NewMessageModel(conn)
	sessionAccess := syncer.NewMySQLSessionAccessStore(conn)

	return &ServiceContext{
		Config:        c,
		MessageModel:  messageModel,
		Authenticator: auth.NewAuthenticator(rds, c.Auth),
		SyncReader:    syncer.NewReader(rds, messageModel, sessionAccess),
	}
}
