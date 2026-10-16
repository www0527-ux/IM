package svc

import (
	"infinitechatgo/common/mysqlpool"
	friendModel "infinitechatgo/service/user/model/friend"
	sessionModel "infinitechatgo/service/user/model/session"
	userModel "infinitechatgo/service/user/model/user"
	userSessionModel "infinitechatgo/service/user/model/user_session"
	"infinitechatgo/service/user/rpc/internal/config"
)

type ServiceContext struct {
	Config           config.Config
	UserModel        userModel.UserModel
	UserSessionModel userSessionModel.UserSessionModel
	SessionModel     sessionModel.SessionModel
	FriendModel      friendModel.FriendModel
}

//创建连接
func NewServiceContext(c config.Config) *ServiceContext {
	conn := mysqlpool.MustNewMysql(c.DataSource, c.MySQLPool)
	return &ServiceContext{
		Config: c,
		UserModel: userModel.NewUserModel(
			conn,
			c.Cache,
		),
		UserSessionModel: userSessionModel.NewUserSessionModel(
			conn,
			c.Cache,
		),
		SessionModel: sessionModel.NewSessionModel(
			conn,
			c.Cache,
		),
		FriendModel: friendModel.NewFriendModel(
			conn,
			c.Cache,
		),
	}
}
