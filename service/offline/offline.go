package main

import (
	"flag"
	"log"

	"infinitechatgo/common/middleware"
	"infinitechatgo/service/offline/internal/config"
	"infinitechatgo/service/offline/internal/consumer"
	"infinitechatgo/service/offline/internal/httpapi"
	"infinitechatgo/service/offline/internal/svc"

	"github.com/zeromicro/go-queue/kq"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/rest"
)

var configFile = flag.String(
	"f",
	"etc/offline.yaml",
	"the config file",
)

func main() {
	flag.Parse()
	var c config.Config
	conf.MustLoad(
		*configFile,
		&c,
		conf.UseEnv(),
	)
	svcCtx := svc.NewServiceContext(c)

	messageConsumer :=
		consumer.NewMessageStoreConsumer(svcCtx)

	queue := kq.MustNewQueue(
		kq.KqConf{
			ServiceConf: c.ServiceConf,
			Brokers:     c.Kafka.Brokers,
			Group:       c.Kafka.StoreGroup,
			Topic:       c.Kafka.SequencedTopic,

			Offset:     "first",
			Conns:      1,
			Consumers:  1,
			Processors: 1,

			ForceCommit: false,
		},
		messageConsumer,
	)
	log.Printf(
		"offline consumer started: topic=%s group=%s",
		c.Kafka.SequencedTopic,
		c.Kafka.StoreGroup,
	)

	server := rest.MustNewServer(c.RestConf)
	server.Use(middleware.RecoverMiddleware)
	httpapi.RegisterRoutes(server, svcCtx)
	log.Printf(
		"offline sync API started: http://%s:%d/api/offline/sync",
		c.Host,
		c.Port,
	)

	serviceGroup := service.NewServiceGroup()
	serviceGroup.Add(queue)
	serviceGroup.Add(server)
	serviceGroup.Start()
}
