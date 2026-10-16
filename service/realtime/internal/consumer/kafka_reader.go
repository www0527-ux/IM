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
)

type KafkaConsumer interface {
	Consume(ctx context.Context, key string, value string) error
}

type MessageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, messages ...kafka.Message) error
}

// 只有消息处理成功或者进入DLQ才能推进消费位置。
// 生产 Reader 使用 common/kafkareader 区分主动 Close 和网络 EOF。
func RunKafkaConsumer(
	ctx context.Context,
	reader MessageReader,
	handler KafkaConsumer,
	group string,
	dlq DeadLetterPublisher,
) error {
	if reader == nil || handler == nil || dlq == nil || strings.TrimSpace(group) == "" {
		return fmt.Errorf("Kafka reader, consumer, group and DLQ publisher are required")
	}

	for {
		var message kafka.Message
		for attempts := 0; ; attempts++ {
			fetched, err := reader.FetchMessage(ctx)
			if err == nil {
				message = fetched
				break
			}
			if ctx.Err() != nil {
				return nil
			}
			// 网络 EOF 已由 Reader 封装转换为 UnexpectedEOF；关闭错误才停止。
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
				return fmt.Errorf("Kafka reader stopped: %w", err)
			}

			//其他读取错误，如临时连接或kafka服务异常，就重试
			logRetry("fetch Kafka record", attempts+1, err)
			if !waitRetry(ctx, retryDelay(attempts)) {
				return nil
			}
		}

		for attempts := 0; ; attempts++ {
			err := handler.Consume(ctx, string(message.Key), string(message.Value))
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return nil
			}
			if asFatal(err) {
				return fmt.Errorf("consumer cannot process records group=%s topic=%s: %w",
					group, message.Topic, err)
			}
			if permanentFailure, ok := asPermanent(err); ok {
				record := newDeadLetter(group, message, permanentFailure)
				log.Printf("permanent Kafka record failure group=%s topic=%s partition=%d offset=%d: %v",
					group, message.Topic, message.Partition, message.Offset, err)
				for dlqAttempts := 0; ; dlqAttempts++ {
					if err := dlq.Publish(ctx, record); err == nil {
						break
					} else if ctx.Err() != nil {
						return nil
					} else {
						logRetry("publish Kafka DLQ record "+record.ID, dlqAttempts+1, err)
						if !waitRetry(ctx, retryDelay(dlqAttempts)) {
							return nil
						}
					}
				}
				break
			}
			logRetry(fmt.Sprintf("consume Kafka record group=%s topic=%s partition=%d offset=%d",
				group, message.Topic, message.Partition, message.Offset), attempts+1, err)
			if !waitRetry(ctx, retryDelay(attempts)) {
				return nil
			}
		}

		for attempts := 0; ; attempts++ {
			//message包含着topic，partition，offset等字段
			err := reader.CommitMessages(ctx, message)
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return nil
			}
			//消费组状态改变
			/*IllegalGeneration：提交使用的代次已不合法，可以理解为拿着旧值班表办事。
			RebalanceInProgress：正在换值班表，当前无法按原状态提交。
			UnknownMemberId：Kafka 不再认可这个成员身份，例如成员会话已经失效。*/
			if errors.Is(err, kafka.IllegalGeneration) ||
				errors.Is(err, kafka.RebalanceInProgress) ||
				errors.Is(err, kafka.UnknownMemberId) {
				return fmt.Errorf("Kafka group generation changed before commit group=%s topic=%s partition=%d offset=%d: %w",
					group, message.Topic, message.Partition, message.Offset, err)
			}
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
				return fmt.Errorf("Kafka reader stopped before commit topic=%s partition=%d offset=%d: %w",
					message.Topic, message.Partition, message.Offset, err)
			}
			logRetry(fmt.Sprintf("commit Kafka record group=%s topic=%s partition=%d offset=%d",
				group, message.Topic, message.Partition, message.Offset), attempts+1, err)
			if !waitRetry(ctx, retryDelay(attempts)) {
				return nil
			}
		}
	}
}

func retryDelay(attempt int) time.Duration {
	delay := time.Second
	for i := 0; i < attempt && delay < 30*time.Second; i++ {
		delay *= 2
	}
	if delay > 30*time.Second {
		return 30 * time.Second
	}
	return delay
}

func waitRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func logRetry(action string, attempt int, err error) {
	if attempt == 1 || attempt == 5 || attempt%10 == 0 {
		log.Printf("%s failed (attempt=%d): %v", action, attempt, err)
	}
}
