package consumer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"infinitechatgo/common/kafkareader"
	"infinitechatgo/common/messagetrace"
)

type BatchCommitConfig = kafkareader.BatchCommitConfig

type fetchedRecord struct {
	message kafka.Message
	err     error
}

// RunKafkaConsumerBatched keeps business processing serial. Fetching may run
// ahead by one bounded buffer, but neither handling nor commit crosses a failed
// record. Only successfully handled/DLQ-acknowledged records enter pending.
func RunKafkaConsumerBatched(ctx context.Context, reader MessageReader, handler KafkaConsumer, group string, dlq DeadLetterPublisher, config BatchCommitConfig) error {
	config, err := config.WithDefaults()
	if err != nil {
		return err
	}
	if config.Disabled {
		return RunKafkaConsumer(ctx, reader, handler, group, dlq)
	}
	if reader == nil || handler == nil || dlq == nil || strings.TrimSpace(group) == "" {
		return fmt.Errorf("Kafka reader, consumer, group and DLQ publisher are required")
	}
	runCtx, cancel := context.WithCancel(ctx)
	records := make(chan fetchedRecord, config.BatchSize)
	fetchDone := make(chan struct{})
	_, isPush := handler.(*MessagePushConsumer)
	trace := isPush && messagetrace.Enabled()
	go func() { defer close(fetchDone); defer close(records); fetchRecords(runCtx, reader, records, trace) }()
	defer func() { cancel(); <-fetchDone }()
	interval := time.Duration(config.IntervalMillis) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	pending := make([]kafka.Message, 0, config.BatchSize)
	var firstPending time.Time
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		if err := commitBatch(runCtx, reader, pending, group, trace); err != nil {
			return err
		}
		pending = pending[:0]
		return nil
	}
	for {
		select {
		case <-runCtx.Done():
			return nil // uncommitted successful records replay
		case <-ticker.C:
			if err := flush(); err != nil {
				if runCtx.Err() != nil {
					return nil
				}
				return err
			}
		case fetched, ok := <-records:
			if !ok {
				if runCtx.Err() != nil {
					return nil
				}
				return fmt.Errorf("Kafka fetch loop stopped")
			}
			if fetched.err != nil {
				return fetched.err
			}
			message := fetched.message
			if trace {
				messagetrace.Event(message.Value, "push_dispatch_start", messagetrace.Stamp())
			}
			if err := processRecord(runCtx, handler, group, dlq, message); err != nil {
				if runCtx.Err() != nil {
					return nil
				}
				return err
			}
			if runCtx.Err() != nil {
				return nil
			}
			if len(pending) == 0 {
				firstPending = time.Now()
			}
			pending = append(pending, message)
			if len(pending) >= config.BatchSize || time.Since(firstPending) >= interval {
				if err := flush(); err != nil {
					if runCtx.Err() != nil {
						return nil
					}
					return err
				}
			}
		}
	}
}

func fetchRecords(ctx context.Context, reader MessageReader, output chan<- fetchedRecord, trace bool) {
	for {
		for attempts := 0; ; attempts++ {
			var start int64
			if trace {
				start = messagetrace.Stamp()
			}
			message, err := reader.FetchMessage(ctx)
			if ctx.Err() != nil {
				return
			}
			if err == nil {
				if trace {
					messagetrace.Event(message.Value, "push_fetch_start", start)
					messagetrace.Event(message.Value, "push_fetched", messagetrace.Stamp())
				}
				select {
				case output <- fetchedRecord{message: message}:
				case <-ctx.Done():
					return
				}
				break
			}
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
				select {
				case output <- fetchedRecord{err: fmt.Errorf("Kafka reader stopped: %w", err)}:
				case <-ctx.Done():
				}
				return
			}
			logRetry("fetch Kafka record", attempts+1, err)
			if !waitRetry(ctx, retryDelay(attempts)) {
				return
			}
		}
	}
}

func processRecord(ctx context.Context, handler KafkaConsumer, group string, dlq DeadLetterPublisher, message kafka.Message) error {
	for attempts := 0; ; attempts++ {
		err := handler.Consume(ctx, string(message.Key), string(message.Value))
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if asFatal(err) {
			return fmt.Errorf("consumer cannot process records group=%s topic=%s: %w", group, message.Topic, err)
		}
		if failure, ok := asPermanent(err); ok {
			record := newDeadLetter(group, message, failure)
			log.Printf("permanent Kafka record failure group=%s topic=%s partition=%d offset=%d: %v", group, message.Topic, message.Partition, message.Offset, err)
			for dlqAttempts := 0; ; dlqAttempts++ {
				if err := dlq.Publish(ctx, record); err == nil {
					return nil
				} else if ctx.Err() != nil {
					return ctx.Err()
				} else {
					logRetry("publish Kafka DLQ record "+record.ID, dlqAttempts+1, err)
					if !waitRetry(ctx, retryDelay(dlqAttempts)) {
						return ctx.Err()
					}
				}
			}
		}
		logRetry(fmt.Sprintf("consume Kafka record group=%s topic=%s partition=%d offset=%d", group, message.Topic, message.Partition, message.Offset), attempts+1, err)
		if !waitRetry(ctx, retryDelay(attempts)) {
			return ctx.Err()
		}
	}
}

func commitBatch(ctx context.Context, reader MessageReader, pending []kafka.Message, group string, trace bool) error {
	type partition struct {
		topic string
		id    int
	}
	highest := make(map[partition]kafka.Message)
	for _, message := range pending {
		key := partition{message.Topic, message.Partition}
		if current, ok := highest[key]; !ok || message.Offset > current.Offset {
			highest[key] = message
		}
	}
	messages := make([]kafka.Message, 0, len(highest))
	for _, message := range highest {
		messages = append(messages, message)
	}
	for attempts := 0; ; attempts++ {
		if trace {
			for _, message := range pending {
				messagetrace.Event(message.Value, "push_commit_start", messagetrace.Stamp())
			}
		}
		err := reader.CommitMessages(ctx, messages...)
		if err == nil {
			if trace {
				for _, message := range pending {
					messagetrace.Event(message.Value, "push_commit_end", messagetrace.Stamp())
				}
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, kafka.IllegalGeneration) || errors.Is(err, kafka.RebalanceInProgress) || errors.Is(err, kafka.UnknownMemberId) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
			return fmt.Errorf("Kafka batch commit stopped group=%s: %w", group, err)
		}
		logRetry("commit Kafka batch group="+group, attempts+1, err)
		if !waitRetry(ctx, retryDelay(attempts)) {
			return ctx.Err()
		}
	}
}
