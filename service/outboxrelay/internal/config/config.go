package config

import (
	"github.com/zeromicro/go-zero/core/service"
	"infinitechatgo/common/mysqlpool"
)

type KafkaConfig struct {
	Brokers []string
}

type RelayConfig struct {
	WorkerID            string
	BatchSize           int  `json:",default=500"`
	CheckpointSize      int  `json:",default=20"`
	PollIntervalMillis  int  `json:",default=200"`
	LeaseSeconds        int  `json:",default=120"`
	MaxRetryBackoffSecs int  `json:",default=60"`
	LogTimings          bool `json:",default=false"`
}

type Config struct {
	service.ServiceConf
	DataSource string
	MySQLPool  mysqlpool.Config
	Kafka      KafkaConfig
	Relay      RelayConfig
}
