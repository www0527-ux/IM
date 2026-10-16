// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package svc

import (
	"github.com/zeromicro/go-zero/rest"
	"infinitechatgo/common/auth"
	"infinitechatgo/common/idgen"
	"infinitechatgo/common/mysqlpool"
	"infinitechatgo/service/redpacket/api/internal/config"
	"infinitechatgo/service/redpacket/api/internal/middleware"
	"infinitechatgo/service/redpacket/application"
	balancelogmodel "infinitechatgo/service/redpacket/model/balance_log"
	outboxmodel "infinitechatgo/service/redpacket/model/outbox_event"
	redpacketmodel "infinitechatgo/service/redpacket/model/red_packet"
	receivemodel "infinitechatgo/service/redpacket/model/red_packet_receive"
	sharemodel "infinitechatgo/service/redpacket/model/red_packet_share"
	balancemodel "infinitechatgo/service/redpacket/model/user_balance"
	"infinitechatgo/service/user/rpc/userrpc"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config config.Config

	SqlConn     sqlx.SqlConn
	Redis       *redis.Redis
	IDGenerator *idgen.Generator
	UserRPC     userrpc.UserRpc
	AccessAuth  rest.Middleware

	RedPacketModel        redpacketmodel.RedPacketModel
	RedPacketShareModel   sharemodel.RedPacketShareModel
	RedPacketReceiveModel receivemodel.RedPacketReceiveModel
	UserBalanceModel      balancemodel.UserBalanceModel
	BalanceLogModel       balancelogmodel.BalanceLogModel
	OutboxEventModel      outboxmodel.OutboxEventModel

	SendService    *application.SendService
	ReceiveService *application.ReceiveService
	QueryService   *application.QueryService
}

func NewServiceContext(c config.Config) *ServiceContext {
	conn := mysqlpool.MustNewMysql(c.DataSource, c.MySQLPool)
	rds := redis.MustNewRedis(c.Redis)
	idGenerator := idgen.MustNew(c.Snowflake)
	userRPC := userrpc.NewUserRpc(zrpc.MustNewClient(c.UserRpc))
	authenticator := auth.NewAuthenticator(rds, c.Auth)

	redPacketModel := redpacketmodel.NewRedPacketModel(conn)
	shareModel := sharemodel.NewRedPacketShareModel(conn)
	receiveModel := receivemodel.NewRedPacketReceiveModel(conn)
	userBalanceModel := balancemodel.NewUserBalanceModel(conn)
	balanceLogModel := balancelogmodel.NewBalanceLogModel(conn)
	outboxEventModel := outboxmodel.NewOutboxEventModel(conn)

	sendService := application.NewSendService(application.SendDependencies{
		SqlConn:          conn,
		Redis:            rds,
		IDGenerator:      idGenerator,
		UserRPC:          userRPC,
		RedPacketModel:   redPacketModel,
		ShareModel:       shareModel,
		UserBalanceModel: userBalanceModel,
		BalanceLogModel:  balanceLogModel,
		OutboxEventModel: outboxEventModel,
		Topics: application.OutboxTopics{
			MessageCreateRequested: c.Kafka.MessageCreateRequestedTopic,
			CacheBuild:             c.Kafka.CacheBuildTopic,
		},
	})
	candidatePool := application.NewRedisCandidatePool(rds, conn)
	receiveService := application.NewReceiveService(application.ReceiveDependencies{
		SqlConn: conn, IDGenerator: idGenerator, UserRPC: userRPC,
		CandidatePool: candidatePool, RedPacketModel: redPacketModel,
		ReceiveModel: receiveModel, BalanceLogModel: balanceLogModel,
	})
	queryService := application.NewQueryService(conn, redPacketModel, userRPC)

	return &ServiceContext{
		Config:                c,
		SqlConn:               conn,
		Redis:                 rds,
		IDGenerator:           idGenerator,
		UserRPC:               userRPC,
		AccessAuth:            middleware.NewAccessAuthMiddleware(authenticator).Handle,
		RedPacketModel:        redPacketModel,
		RedPacketShareModel:   shareModel,
		RedPacketReceiveModel: receiveModel,
		UserBalanceModel:      userBalanceModel,
		BalanceLogModel:       balanceLogModel,
		OutboxEventModel:      outboxEventModel,
		SendService:           sendService,
		ReceiveService:        receiveService,
		QueryService:          queryService,
	}
}
