package main

import (
	"flag"
	"log"

	"infinitechatgo/service/realtime/internal/config"
	"infinitechatgo/service/realtime/internal/consumer"
	"infinitechatgo/service/realtime/internal/svc"
	realtimews "infinitechatgo/service/realtime/internal/websocket"

	"github.com/zeromicro/go-queue/kq"
	"github.com/zeromicro/go-zero/core/conf"
)

var configFile = flag.String(
	"f",
	"etc/realtime.yaml",
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

	svcCtx, err := svc.NewServiceContext(c)
	if err != nil {
		log.Fatalf(
			"initialize realtime service: %v",
			err,
		)
	}

	defer func() {
		if err := svcCtx.Close(); err != nil {
			log.Printf(
				"close realtime service: %v",
				err,
			)
		}
	}()

	manager := realtimews.NewManager()
	pushConsumer := consumer.NewMessagePushConsumer(
		manager,
		svcCtx.UserRPC,
	)
	pushQueue := kq.MustNewQueue(
		kq.KqConf{
			ServiceConf: c.ServiceConf,
			Brokers:     c.Kafka.Brokers,
			Group:       c.Kafka.PushGroup,
			Topic:       c.Kafka.SequencedTopic,
			Offset:      "first",
			Conns:       1,
			Consumers:   1,
			Processors:  1,
			ForceCommit: false,
		},
		pushConsumer,
	)
	go pushQueue.Start()
	defer pushQueue.Stop()

	log.Printf(
		"realtime push consumer started: topic=%s group=%s",
		c.Kafka.SequencedTopic,
		c.Kafka.PushGroup,
	)

	if err := realtimews.StartServer(
		c.ListenOn,
		manager,
		svcCtx,
	); err != nil {
		log.Printf(
			"realtime server stopped: %v",
			err,
		)
	}
}
