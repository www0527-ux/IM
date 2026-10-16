package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"infinitechatgo/common/kafkareader"

	messageevent "infinitechatgo/common/model/event"

	"github.com/segmentio/kafka-go"
)

type invalidCacheEvent struct{ reason string }

func (e invalidCacheEvent) Error() string { return e.reason }

type candidatePoolBuilder interface {
	Rebuild(ctx context.Context, redPacketID int64) error
}

type sendResultBuilder interface {
	Build(ctx context.Context, redPacketID int64) error
}

func rebuildFromEvent(ctx context.Context, pool candidatePoolBuilder,
	sendResults sendResultBuilder, value []byte) error {
	var event messageevent.RedPacketCacheBuildEvent
	if err := json.Unmarshal(value, &event); err != nil {
		return invalidCacheEvent{reason: "invalid_json"}
	}
	if event.EventType != messageevent.RedPacketCacheBuildEventType || event.Version != 1 ||
		event.EventID <= 0 || event.RedPacketID <= 0 {
		return invalidCacheEvent{reason: "invalid_cache_build_event"}
	}
	if pool == nil || sendResults == nil {
		return fmt.Errorf("red packet cache builders are missing")
	}
	if err := sendResults.Build(ctx, event.RedPacketID); err != nil {
		return fmt.Errorf("build red packet send-result cache: %w", err)
	}
	if err := pool.Rebuild(ctx, event.RedPacketID); err != nil {
		return fmt.Errorf("build red packet share candidates: %w", err)
	}
	return nil
}

type deadLetter struct {
	ID            string    `json:"id"`
	EventID       int64     `json:"eventId,omitempty"`
	ConsumerGroup string    `json:"consumerGroup"`
	Topic         string    `json:"topic"`
	Partition     int       `json:"partition"`
	Offset        int64     `json:"offset"`
	Key           []byte    `json:"key"`
	Value         []byte    `json:"value"`
	ErrorType     string    `json:"errorType"`
	ErrorMessage  string    `json:"errorMessage"`
	FailedAt      time.Time `json:"failedAt"`
}

func consumeCacheBuild(ctx context.Context, reader *kafkareader.Reader, dlq *kafka.Writer,
	pool candidatePoolBuilder, sendResults sendResultBuilder, group string) error {
	for {
		message, err := reader.FetchMessage(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
				return fmt.Errorf("cache build reader closed: %w", err)
			}
			log.Printf("fetch cache build event: %v", err)
			if !wait(ctx, time.Second) {
				return nil
			}
			continue
		}
		for {
			err = rebuildFromEvent(ctx, pool, sendResults, message.Value)
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return nil
			}
			var invalid invalidCacheEvent
			if errors.As(err, &invalid) {
				var envelope struct {
					EventID int64 `json:"eventId,string"`
				}
				_ = json.Unmarshal(message.Value, &envelope)
				record := deadLetter{
					ID: fmt.Sprintf("%s/%s/%d/%d", group, message.Topic,
						message.Partition, message.Offset),
					EventID:       envelope.EventID,
					ConsumerGroup: group, Topic: message.Topic,
					Partition: message.Partition, Offset: message.Offset,
					Key: message.Key, Value: message.Value, ErrorType: invalid.reason,
					ErrorMessage: err.Error(), FailedAt: time.Now().UTC(),
				}
				body, marshalErr := json.Marshal(record)
				if marshalErr != nil {
					return marshalErr
				}
				if err := dlq.WriteMessages(ctx, kafka.Message{Key: []byte(record.ID), Value: body}); err == nil {
					break
				} else {
					log.Printf("publish invalid cache event to DLQ: %v", err)
				}
			} else {
				log.Printf("rebuild red packet cache: %v", err)
			}
			if !wait(ctx, 2*time.Second) {
				return nil
			}
		}
		for {
			if err := reader.CommitMessages(ctx, message); err == nil {
				break
			} else if ctx.Err() != nil {
				return nil
			} else if errors.Is(err, kafka.IllegalGeneration) ||
				errors.Is(err, kafka.RebalanceInProgress) || errors.Is(err, kafka.UnknownMemberId) {
				return fmt.Errorf("cache build group changed before commit: %w", err)
			} else if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
				return fmt.Errorf("cache build reader closed before commit: %w", err)
			} else {
				log.Printf("commit cache build event: %v", err)
			}
			if !wait(ctx, 2*time.Second) {
				return nil
			}
		}
	}
}

func wait(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
