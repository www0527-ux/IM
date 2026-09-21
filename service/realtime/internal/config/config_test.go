package config

import "testing"

func TestValidatePipelineKeepsLegacyAsDefault(t *testing.T) {
	if err := (Config{}).ValidatePipeline(); err != nil {
		t.Fatalf("default legacy config rejected: %v", err)
	}
}

func TestValidatePipelineRequiresTargetDependencies(t *testing.T) {
	c := Config{MessagePipeline: MessagePipelineConfig{Mode: PipelineModeOutbox}}
	if err := c.ValidatePipeline(); err == nil {
		t.Fatal("outbox config without database and topics was accepted")
	}
}

func TestValidatePipelineAcceptsCompleteOutboxConfig(t *testing.T) {
	c := Config{
		DataSource: "mysql",
		Kafka: KafkaConfig{
			CreateRequestedTopic: "message-create-requested-topic",
			PersistedTopic:       "message-persisted-topic",
			MessageWriterGroup:   "message-writer",
			PushGroup:            "message-push",
			MessageCacheGroup:    "message-cache",
		},
		MessagePipeline: MessagePipelineConfig{
			Mode:                       PipelineModeOutbox,
			ContentCacheTTLSeconds:     60,
			IdempotencyCacheTTLSeconds: 60,
		},
	}
	if err := c.ValidatePipeline(); err != nil {
		t.Fatalf("complete outbox config rejected: %v", err)
	}
}
