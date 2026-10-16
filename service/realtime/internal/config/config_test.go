package config

import "testing"

func TestValidatePipelineRequiresTargetDependenciesByDefault(t *testing.T) {
	if err := (Config{}).ValidatePipeline(); err == nil {
		t.Fatal("configuration without database and topics was accepted")
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
