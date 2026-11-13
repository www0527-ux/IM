package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"infinitechatgo/common/groupmembercache"
	"infinitechatgo/common/kafkareader"

	"infinitechatgo/service/realtime/internal/config"
	"infinitechatgo/service/realtime/internal/consumer"
	"infinitechatgo/service/realtime/internal/svc"
	realtimews "infinitechatgo/service/realtime/internal/websocket"

	"github.com/segmentio/kafka-go"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
)

var configFile = flag.String(
	"f",
	"etc/realtime.yaml",
	"the config file",
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	flag.Parse()

	var c config.Config
	conf.MustLoad(
		*configFile,
		&c,
		conf.UseEnv(),
	)
	if c.Log.ServiceName == "" {
		c.Log.ServiceName = c.Name
	}
	if err := logx.SetUp(c.Log); err != nil {
		return fmt.Errorf("initialize realtime logging: %w", err)
	}

	svcCtx, err := svc.NewServiceContext(c)
	if err != nil {
		return fmt.Errorf("initialize realtime service: %w", err)
	}
	defer func() {
		svcCtx.BatchWriter.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := svcCtx.Publisher.Close(ctx); err != nil {
			log.Printf("drain committed message notifications: %v", err)
		}
	}()
	dlqPublisher, err := consumer.NewKafkaDeadLetterPublisher(c.Kafka.Brokers, c.Kafka.DLQTopic)
	if err != nil {
		return fmt.Errorf("initialize Kafka DLQ publisher: %w", err)
	}
	defer func() {
		if err := dlqPublisher.Close(); err != nil {
			log.Printf("close Kafka DLQ publisher: %v", err)
		}
	}()
	//创建一个可以监听系统中断信号的上下文
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	readers := make([]*kafkareader.Reader, 0, 3)
	var consumerWorkers sync.WaitGroup
	defer func() {
		stop()
		for _, reader := range readers {
			_ = reader.Close()
		}
		consumerWorkers.Wait()
	}()
	consumerErrors := make(chan error, 3)
	// startConsumer is a helper function to start a Kafka consumer in a separate goroutine.
	startConsumer := func(topic, group string, handler consumer.KafkaConsumer) {
		// 创建 Reader 并启动消费组后台任务；Kafka 连接异步建立。
		//handler为消费者的处理逻辑，实现了consume接口
		reader := kafkareader.New(kafka.ReaderConfig{
			Brokers:        c.Kafka.Brokers, //kafka集群地址
			GroupID:        group,
			Topic:          topic,
			StartOffset:    kafka.FirstOffset, // Only used when this group has no committed offset.
			CommitInterval: 0,                 // CommitMessages 调用时同步等待提交结果；不会自动提交。
		})
		readers = append(readers, reader)
		consumerWorkers.Add(1)
		go func() {
			defer consumerWorkers.Done()
			if err := consumer.RunKafkaConsumerBatched(ctx, reader, handler, group, dlqPublisher, c.MessagePipeline.ConsumerCommits); err != nil {
				consumerErrors <- fmt.Errorf("Kafka consumer group=%s topic=%s stopped: %w", group, topic, err)
			}
		}()
		log.Printf("Kafka consumer started: topic=%s group=%s", topic, group)
	}

	manager := realtimews.NewManager()
	//创建handler
	pushConsumer := consumer.NewMessagePushConsumer(manager, svcCtx.UserRPC)
	memberCacheConfig, _ := c.GroupMemberCache.WithDefaults() // validated by ServiceContext
	if !memberCacheConfig.Disabled {
		local, err := groupmembercache.NewLocal(memberCacheConfig)
		if err != nil {
			return err
		}
		client, err := groupmembercache.NewRedisClient(c.Redis)
		if err != nil {
			return err
		}
		defer client.Close()
		subscription := groupmembercache.Subscribe(ctx, client, memberCacheConfig.Channel, local)
		defer func() {
			subscription.Close()
			log.Printf("group member push cache stats: %+v", local.Stats())
		}()
		pushConsumer = consumer.NewMessagePushConsumer(manager, consumer.NewCachedGroupMembers(svcCtx.UserRPC, local))
	}
	cacheConsumer, err := consumer.NewMessageCacheConsumer(
		svcCtx.Redis,
		c.MessagePipeline.ContentCacheTTLSeconds,
		c.MessagePipeline.IdempotencyCacheTTLSeconds,
	)
	if err != nil {
		return fmt.Errorf("initialize message cache consumer: %w", err)
	}
	//启动消费者循环
	startConsumer(
		c.Kafka.PersistedTopic,
		c.Kafka.PushGroup,
		pushConsumer,
	)
	startConsumer(
		c.Kafka.PersistedTopic,
		c.Kafka.MessageCacheGroup,
		cacheConsumer,
	)

	serverErrors := make(chan error, 1)
	//启动websocket服务
	go func() {
		serverErrors <- realtimews.StartServer(c.ListenOn, manager, svcCtx)
	}()
	select {
	case err := <-consumerErrors:
		return err
	case err := <-serverErrors:
		if err != nil {
			return fmt.Errorf("realtime server stopped: %w", err)
		}
		return fmt.Errorf("realtime server stopped unexpectedly")
	case <-ctx.Done():
		return nil
	}
}
