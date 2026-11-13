package svc

import (
	"fmt"

	"infinitechatgo/common/auth"
	"infinitechatgo/common/idgen"
	"infinitechatgo/common/messagepublish"
	"infinitechatgo/common/mysqlpool"
	messageapplication "infinitechatgo/service/message/application"
	"infinitechatgo/service/message/batching"
	messagecache "infinitechatgo/service/message/cache"
	sequencemodel "infinitechatgo/service/message/model/conversation_sequence"
	messagemodel "infinitechatgo/service/message/model/message"
	messagepersistence "infinitechatgo/service/message/persistence"
	"infinitechatgo/service/realtime/internal/config"
	"infinitechatgo/service/user/rpc/userrpc"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config config.Config

	Redis         *redis.Redis
	SqlConn       sqlx.SqlConn
	Authenticator *auth.Authenticator
	UserRPC       userrpc.UserRpc

	MessageWriter *messageapplication.MessageWriteService
	BatchWriter   *batching.Writer
	Publisher     *messagepublish.Publisher
}

func NewServiceContext(
	c config.Config,
) (*ServiceContext, error) {
	if err := c.ValidatePipeline(); err != nil {
		return nil, err
	}
	rds := redis.MustNewRedis(c.Redis)
	idGenerator, err := idgen.New(c.Snowflake)
	if err != nil {
		return nil, fmt.Errorf("create id generator: %w", err)
	}

	ctx := &ServiceContext{
		Config: c,

		Redis:         rds,
		Authenticator: auth.NewAuthenticator(rds, c.Auth),
		UserRPC: userrpc.NewUserRpc(
			zrpc.MustNewClient(c.UserRpc),
		),
	}

	ctx.SqlConn, err = mysqlpool.NewMysql(c.DataSource, c.MySQLPool)
	if err != nil {
		return nil, err
	}
	persistentWriter, err := messagepersistence.NewWriter(
		messagepersistence.Dependencies{
			SqlConn:     ctx.SqlConn,
			IDGenerator: idGenerator,
			Messages:    messagemodel.NewModel(ctx.SqlConn),
			Sequences:   sequencemodel.NewModel(ctx.SqlConn),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("create message persistent writer: %w", err)
	}
	ctx.Publisher, err = messagepublish.NewWithConfig(c.Kafka.Brokers, c.Kafka.PersistedTopic, c.MessagePipeline.Publishing)
	if err != nil {
		return nil, fmt.Errorf("create message publisher: %w", err)
	}
	ctx.BatchWriter, err = batching.New(persistentWriter, ctx.Publisher, c.MessagePipeline.Batching)
	if err != nil {
		return nil, fmt.Errorf("create message batch writer: %w", err)
	}
	ctx.MessageWriter = messageapplication.NewMessageWriteService(
		messagecache.NewRedisResultCache(rds),
		ctx.BatchWriter,
	)
	return ctx, nil
}
