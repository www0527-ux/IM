package config

import (
	"infinitechatgo/common/groupmembercache"
	"testing"

	"github.com/zeromicro/go-zero/core/conf"
)

func TestGroupMemberCacheDefaultsAndValidation(t *testing.T) {
	for _, raw := range []string{"{}", "GroupMemberCache: {}", "GroupMemberCache:\n  Disabled: true"} {
		var target struct {
			GroupMemberCache groupmembercache.Config `json:",optional"`
		}
		if err := conf.LoadFromYamlBytes([]byte(raw), &target); err != nil {
			t.Fatal(err)
		}
		settings, err := target.GroupMemberCache.WithDefaults()
		if err != nil || settings.TTLSeconds != 5 || settings.Capacity != 1024 || settings.Channel != groupmembercache.DefaultChannel {
			t.Fatalf("settings=%+v err=%v", settings, err)
		}
	}
	if _, err := (groupmembercache.Config{TTLSeconds: -1}).WithDefaults(); err == nil {
		t.Fatal("invalid TTL accepted")
	}
}

func TestPublisherAndCommitBatchDefaultsWithoutConfigMigration(t *testing.T) {
	var target struct{ MessagePipeline MessagePipelineConfig }
	if err := conf.LoadFromYamlBytes([]byte("MessagePipeline: {}"), &target); err != nil {
		t.Fatal(err)
	}
	p, err := target.MessagePipeline.Publishing.WithDefaults()
	if err != nil || p.BatchSize != 32 || p.FlushMillis != 1 || p.QueueCapacity != 512 {
		t.Fatalf("publisher=%+v err=%v", p, err)
	}
	c, err := target.MessagePipeline.ConsumerCommits.WithDefaults()
	if err != nil || c.BatchSize != 32 || c.IntervalMillis != 10 || c.Disabled {
		t.Fatalf("commits=%+v err=%v", c, err)
	}
}

func TestValidatePipelineRequiresTargetDependenciesByDefault(t *testing.T) {
	if err := (Config{}).ValidatePipeline(); err == nil {
		t.Fatal("configuration without database and topics was accepted")
	}
}

func TestMessageBatchingDefaultsDoNotRequireLocalConfigMigration(t *testing.T) {
	for _, raw := range []string{
		"MessagePipeline: {}",
		"MessagePipeline:\n  Batching:\n    Workers: 2\n    BatchSize: 16",
	} {
		var target struct{ MessagePipeline MessagePipelineConfig }
		if err := conf.LoadFromYamlBytes([]byte(raw), &target); err != nil {
			t.Fatal(err)
		}
		settings, err := target.MessagePipeline.Batching.WithDefaults()
		if err != nil || settings.QueueCapacity != 256 || settings.FlushMillis != 2 {
			t.Fatalf("settings=%+v err=%v", settings, err)
		}
	}
}

func TestValidatePipelineAcceptsCompleteOutboxConfig(t *testing.T) {
	c := Config{
		DataSource: "mysql",
		Kafka: KafkaConfig{
			CreateRequestedTopic: "message-create-requested-topic",
			PersistedTopic:       "message-persisted-topic",
			DLQTopic:             "message-consumer-dlq-topic",
			MessageWriterGroup:   "message-writer",
			PushGroup:            "message-push",
			MessageCacheGroup:    "message-cache",
		},
		MessagePipeline: MessagePipelineConfig{
			ContentCacheTTLSeconds:     60,
			IdempotencyCacheTTLSeconds: 60,
		},
	}
	if err := c.ValidatePipeline(); err != nil {
		t.Fatalf("complete outbox config rejected: %v", err)
	}
}
