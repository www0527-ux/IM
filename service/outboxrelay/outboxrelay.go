package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"infinitechatgo/common/mysqlpool"
	"infinitechatgo/service/outboxrelay/internal/config"
	"infinitechatgo/service/outboxrelay/internal/mq"
	"infinitechatgo/service/outboxrelay/internal/relay"

	"github.com/zeromicro/go-zero/core/conf"
)

var configFile = flag.String("f", "etc/outbox-relay.yaml", "the config file")

// relay worker编排
func main() {
	flag.Parse()
	var c config.Config
	conf.MustLoad(*configFile, &c, conf.UseEnv())
	if err := validate(c); err != nil {
		log.Fatal(err)
	}

	publisher, err := mq.NewPublisher(c.Kafka.Brokers)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			log.Printf("close outbox publisher: %v", err)
		}
	}()

	conn, err := mysqlpool.NewMysql(c.DataSource, c.MySQLPool)
	if err != nil {
		log.Fatal(err)
	}
	store := relay.NewMySQLStore(conn)
	options := relay.Options{
		WorkerID:        c.Relay.WorkerID,
		BatchSize:       c.Relay.BatchSize, //一轮最多领取的消息数量
		CheckpointSize:  c.Relay.CheckpointSize,
		LeaseDuration:   time.Duration(c.Relay.LeaseSeconds) * time.Second,        //领取后租约的保存时间
		MaxRetryBackoff: time.Duration(c.Relay.MaxRetryBackoffSecs) * time.Second, //指数退避最大时长
	}
	if c.Relay.LogTimings {
		options.Observe = func(timing relay.Timing) {
			encoded, err := json.Marshal(timing)
			if err == nil {
				log.Printf("outbox_timing %s", encoded)
			}
		}
	}
	worker := relay.NewWorker(store, publisher, options)
	//监听系统信号，优雅退出
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("outbox relay started: worker=%s", c.Relay.WorkerID)
	if err := relay.Run(ctx, worker, time.Duration(c.Relay.PollIntervalMillis)*time.Millisecond, func(err error) {
		log.Printf("outbox relay batch failed: %v", err)
	}); err != nil {
		log.Printf("outbox relay stopped: %v", err)
	}
}

func validate(c config.Config) error {
	if strings.TrimSpace(c.DataSource) == "" || strings.TrimSpace(c.Relay.WorkerID) == "" {
		return fmt.Errorf("relay data source and worker ID are required")
	}
	if len(c.Relay.WorkerID) > 64 {
		return fmt.Errorf("relay worker ID must fit the outbox lease_owner column")
	}
	if c.Relay.BatchSize <= 0 || c.Relay.BatchSize > 2000 || c.Relay.CheckpointSize <= 0 || c.Relay.CheckpointSize > c.Relay.BatchSize || c.Relay.PollIntervalMillis <= 0 ||
		c.Relay.LeaseSeconds <= 0 || c.Relay.MaxRetryBackoffSecs <= 0 {
		return fmt.Errorf("relay batch must be 1..2000; checkpoint must be 1..batch; poll, lease and backoff must be positive")
	}
	return nil
}
