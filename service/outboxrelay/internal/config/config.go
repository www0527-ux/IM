package config

import "github.com/zeromicro/go-zero/core/service"

type KafkaConfig struct {
	Brokers []string
}

type RelayConfig struct {
	WorkerID            string
	BatchSize           int `json:",default=100"`
	PollIntervalMillis  int `json:",default=200"`
	LeaseSeconds        int `json:",default=30"`
	MaxRetryBackoffSecs int `json:",default=60"`
}

type Config struct {
	service.ServiceConf
	DataSource string
	Kafka      KafkaConfig
	Relay      RelayConfig
}
