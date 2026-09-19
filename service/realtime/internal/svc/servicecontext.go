package svc

import (
	"fmt"

	"infinitechatgo/common/auth"
	"infinitechatgo/service/realtime/internal/config"
	"infinitechatgo/service/realtime/internal/messageid"
	"infinitechatgo/service/realtime/internal/mq"
	"infinitechatgo/service/user/rpc/userrpc"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config config.Config

	Redis         *redis.Redis
	Authenticator *auth.Authenticator
	UserRPC       userrpc.UserRpc

	MessageIDResolver *messageid.Resolver
	AcceptedPublisher *mq.AcceptedMessagePublisher
}

func NewServiceContext(
	c config.Config,
) (*ServiceContext, error) {
	rds := redis.MustNewRedis(c.Redis)

	publisher, err :=
		mq.NewAcceptedMessagePublisher(c.Kafka)
	if err != nil {
		return nil, fmt.Errorf(
			"create accepted message publisher: %w",
			err,
		)
	}
	return &ServiceContext{
		Config: c,

		Redis:         rds,
		Authenticator: auth.NewAuthenticator(rds, c.Auth),
		UserRPC: userrpc.NewUserRpc(
			zrpc.MustNewClient(c.UserRpc),
		),

		MessageIDResolver: messageid.NewResolver(rds),
		AcceptedPublisher: publisher,
	}, nil
}
func (s *ServiceContext) Close() error {
	return s.AcceptedPublisher.Close()
}
