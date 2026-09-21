package svc

import (
	"fmt"

	"infinitechatgo/common/auth"
	"infinitechatgo/common/idgen"
	messageapplication "infinitechatgo/service/message/application"
	messagecache "infinitechatgo/service/message/cache"
	sequencemodel "infinitechatgo/service/message/model/conversation_sequence"
	messagemodel "infinitechatgo/service/message/model/message"
	outboxmodel "infinitechatgo/service/message/model/outbox_event"
	messagepersistence "infinitechatgo/service/message/persistence"
	"infinitechatgo/service/realtime/internal/config"
	"infinitechatgo/service/realtime/internal/messageid"
	"infinitechatgo/service/realtime/internal/mq"
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

	MessageIDResolver *messageid.Resolver
	AcceptedPublisher *mq.AcceptedMessagePublisher
	MessageWriter     *messageapplication.MessageWriteService
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

	if c.MessagePipeline.NormalizedMode() == config.PipelineModeLegacy {
		publisher, err := mq.NewAcceptedMessagePublisher(c.Kafka)
		if err != nil {
			return nil, fmt.Errorf(
				"create accepted message publisher: %w",
				err,
			)
		}
		ctx.MessageIDResolver = messageid.NewResolver(rds, idGenerator)
		ctx.AcceptedPublisher = publisher
		return ctx, nil
	}

	ctx.SqlConn = sqlx.NewMysql(c.DataSource)
	persistentWriter, err := messagepersistence.NewWriter(
		messagepersistence.Dependencies{
			SqlConn:        ctx.SqlConn,
			IDGenerator:    idGenerator,
			Messages:       messagemodel.NewModel(ctx.SqlConn),
			Sequences:      sequencemodel.NewModel(ctx.SqlConn),
			Outbox:         outboxmodel.NewModel(ctx.SqlConn),
			PersistedTopic: c.Kafka.PersistedTopic,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("create message persistent writer: %w", err)
	}
	ctx.MessageWriter = messageapplication.NewMessageWriteService(
		messagecache.NewRedisResultCache(rds),
		persistentWriter,
	)
	return ctx, nil
}

func (s *ServiceContext) UsesOutboxPipeline() bool {
	return s != nil &&
		s.Config.MessagePipeline.NormalizedMode() == config.PipelineModeOutbox
}

func (s *ServiceContext) Close() error {
	if s == nil || s.AcceptedPublisher == nil {
		return nil
	}
	return s.AcceptedPublisher.Close()
}
