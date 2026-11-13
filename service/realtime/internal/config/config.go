package config

import (
	"fmt"
	"strings"

	"infinitechatgo/common/auth"
	"infinitechatgo/common/groupmembercache"
	"infinitechatgo/common/idgen"
	"infinitechatgo/common/kafkareader"
	"infinitechatgo/common/messagepublish"
	"infinitechatgo/common/mysqlpool"
	"infinitechatgo/service/message/batching"

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
	ContentCacheTTLSeconds     int                           `json:",default=604800"`
	IdempotencyCacheTTLSeconds int                           `json:",default=604800"`
	Batching                   batching.Config               `json:",optional"`
	Publishing                 messagepublish.Config         `json:",optional"`
	ConsumerCommits            kafkareader.BatchCommitConfig `json:",optional"`
}

func (c Config) ValidatePipeline() error {
	if strings.TrimSpace(c.DataSource) == "" {
		return fmt.Errorf("DataSource is required for message persistence")
	}
	if strings.TrimSpace(c.Kafka.PersistedTopic) == "" ||
		strings.TrimSpace(c.Kafka.DLQTopic) == "" ||
		strings.TrimSpace(c.Kafka.PushGroup) == "" ||
		strings.TrimSpace(c.Kafka.MessageCacheGroup) == "" {
		return fmt.Errorf("message pipeline Kafka topics and groups are required")
	}
	if c.MessagePipeline.ContentCacheTTLSeconds <= 0 ||
		c.MessagePipeline.IdempotencyCacheTTLSeconds <= 0 {
		return fmt.Errorf("message cache TTL values must be positive")
	}
	if _, err := c.MessagePipeline.Batching.WithDefaults(); err != nil {
		return err
	}
	if _, err := c.GroupMemberCache.WithDefaults(); err != nil {
		return err
	}
	if _, err := c.MessagePipeline.Publishing.WithDefaults(); err != nil {
		return err
	}
	if _, err := c.MessagePipeline.ConsumerCommits.WithDefaults(); err != nil {
		return err
	}
	return nil
}

type Config struct {
	service.ServiceConf

	ListenOn         string
	DataSource       string
	MySQLPool        mysqlpool.Config
	Redis            redis.RedisConf
	Auth             auth.Config
	Snowflake        idgen.Config
	UserRpc          zrpc.RpcClientConf
	Kafka            KafkaConfig
	MessagePipeline  MessagePipelineConfig
	GroupMemberCache groupmembercache.Config `json:",optional"`
}
