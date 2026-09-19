package main

import (
	"flag"
	"log"

	"infinitechatgo/service/transfer/internal/config"
	"infinitechatgo/service/transfer/internal/consumer"
	"infinitechatgo/service/transfer/internal/svc"

	"github.com/zeromicro/go-queue/kq"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"
)

var configFile = flag.String(
	"f",
	"etc/transfer.yaml",
	"the config file",
)

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c, conf.UseEnv())

	svcCtx, err := svc.NewServiceContext(c)
	if err != nil {
		log.Fatalf("initialize message transfer service: %v", err)
	}
	defer func() {
		if err := svcCtx.Close(); err != nil {
			log.Printf("close message transfer service: %v", err)
		}
	}()

	if err := consumer.ValidateKafkaConfig(svcCtx); err != nil {
		log.Fatal(err)
	}

	transferConsumer := consumer.NewMessageTransferConsumer(svcCtx)
	queue := kq.MustNewQueue(
		kq.KqConf{
			ServiceConf: c.ServiceConf,
			Brokers:     c.Kafka.Brokers,
			Group:       c.Kafka.AcceptedGroup,
			Topic:       c.Kafka.AcceptedTopic,
			Offset:      "first",
			Conns:       1,
			Consumers:   1,
			Processors:  1,
			ForceCommit: false,
		},
		transferConsumer, //负责处理接收到的消息
	)

	log.Printf(
		"message transfer started: acceptedTopic=%s sequencedTopic=%s group=%s",
		c.Kafka.AcceptedTopic,
		c.Kafka.SequencedTopic,
		c.Kafka.AcceptedGroup,
	)
	//创建服务管理器
	serviceGroup := service.NewServiceGroup()

	serviceGroup.Add(queue)
	serviceGroup.Start()
}
