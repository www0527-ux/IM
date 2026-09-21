package config

import (
	"fmt"
	"strings"

	"infinitechatgo/common/auth"
	"infinitechatgo/common/idgen"

	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type KafkaConfig struct {
	Brokers              []string
	AcceptedTopic        string
	SequencedTopic       string
	CreateRequestedTopic string
	PersistedTopic       string
	MessageWriterGroup   string
	PushGroup            string
	MessageCacheGroup    string
}

const (
	PipelineModeLegacy = "legacy"
	PipelineModeOutbox = "outbox"
)

type MessagePipelineConfig struct {
	Mode                       string
	ContentCacheTTLSeconds     int `json:",default=604800"`
	IdempotencyCacheTTLSeconds int `json:",default=604800"`
}

func (c MessagePipelineConfig) NormalizedMode() string {
	mode := strings.ToLower(strings.TrimSpace(c.Mode))
	if mode == "" {
		return PipelineModeLegacy
	}
	return mode
}

func (c Config) ValidatePipeline() error {
	switch c.MessagePipeline.NormalizedMode() {
	case PipelineModeLegacy:
		return nil
	case PipelineModeOutbox:
		if strings.TrimSpace(c.DataSource) == "" {
			return fmt.Errorf("DataSource is required in outbox message pipeline")
		}
		if strings.TrimSpace(c.Kafka.CreateRequestedTopic) == "" ||
			strings.TrimSpace(c.Kafka.PersistedTopic) == "" ||
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
	default:
		return fmt.Errorf("unsupported message pipeline mode %q", c.MessagePipeline.Mode)
	}
}

type Config struct {
	service.ServiceConf

	ListenOn        string
	DataSource      string
	Redis           redis.RedisConf
	Auth            auth.Config
	Snowflake       idgen.Config
	UserRpc         zrpc.RpcClientConf
	Kafka           KafkaConfig
	MessagePipeline MessagePipelineConfig
}
