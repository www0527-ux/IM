package config

import (
	"fmt"
	"strings"

	"infinitechatgo/common/auth"
	"infinitechatgo/common/idgen"
	"infinitechatgo/common/mysqlpool"

	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type KafkaConfig struct {
	Brokers              []string
	CreateRequestedTopic string
	PersistedTopic       string
	DLQTopic             string
	MessageWriterGroup   string
	PushGroup            string
	MessageCacheGroup    string
}

type MessagePipelineConfig struct {
	ContentCacheTTLSeconds     int `json:",default=604800"`
	IdempotencyCacheTTLSeconds int `json:",default=604800"`
}

func (c Config) ValidatePipeline() error {
	if strings.TrimSpace(c.DataSource) == "" {
		return fmt.Errorf("DataSource is required for message persistence")
	}
	if strings.TrimSpace(c.Kafka.CreateRequestedTopic) == "" ||
		strings.TrimSpace(c.Kafka.PersistedTopic) == "" ||
		strings.TrimSpace(c.Kafka.DLQTopic) == "" ||
		strings.TrimSpace(c.Kafka.MessageWriterGroup) == "" ||
		strings.TrimSpace(c.Kafka.PushGroup) == "" ||
		strings.TrimSpace(c.Kafka.MessageCacheGroup) == "" {
		return fmt.Errorf("outbox message pipeline Kafka topics and groups are required")
	}
	if c.MessagePipeline.ContentCacheTTLSeconds <= 0 ||
		c.MessagePipeline.IdempotencyCacheTTLSeconds <= 0 {
		return fmt.Errorf("outbox message cache TTL values must be positive")
	}
	return nil
}

type Config struct {
	service.ServiceConf

	ListenOn        string
	DataSource      string
	MySQLPool       mysqlpool.Config
	Redis           redis.RedisConf
	Auth            auth.Config
	Snowflake       idgen.Config
	UserRpc         zrpc.RpcClientConf
	Kafka           KafkaConfig
	MessagePipeline MessagePipelineConfig
}
