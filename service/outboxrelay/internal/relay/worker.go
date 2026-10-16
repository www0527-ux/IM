package relay

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"
)

type Event struct {
	EventID       int64         `db:"event_id"`
	Topic         string        `db:"topic"`
	MessageKey    string        `db:"message_key"`
	Payload       string        `db:"payload"`
	RetryCount    int64         `db:"retry_count"`
	EventType     string        `db:"event_type"`
	AggregateType string        `db:"aggregate_type"`
	AggregateID   int64         `db:"aggregate_id"`
	AggregateSeq  sql.NullInt64 `db:"aggregate_seq"`
	Status        int           `db:"status"`
	NextRetryAt   time.Time     `db:"next_retry_at"`
	LeaseUntil    sql.NullTime  `db:"lease_until"`
	DBNow         time.Time     `db:"db_now"`
}

type Store interface {
	ClaimBatch(
		ctx context.Context,
		workerID string,
		batchSize int,
		leaseUntil time.Time,
	) ([]Event, error)
	MarkPublishedBatch(ctx context.Context, eventIDs []int64, workerID string, publishedAt time.Time) error
	ReleaseBatch(ctx context.Context, eventIDs []int64, workerID string) error
	Reschedule(
		ctx context.Context,
		eventID int64,
		workerID string,
		nextRetryAt time.Time,
		lastError string,
	) error
}

type Publisher interface {
	Publish(ctx context.Context, topic, messageKey, payload string) error
}

type Options struct {
	WorkerID        string
	BatchSize       int
	LeaseDuration   time.Duration
	MaxRetryBackoff time.Duration
	Observe         func(Timing)
	CheckpointSize  int
}

// Timing records a completed operation, including failures. The callback is
// optional; no message payloads or connection credentials are recorded.
type Timing struct {
	At         time.Time `json:"at"`
	Phase      string    `json:"phase"`
	DurationMs float64   `json:"durationMs"`
	EventID    string    `json:"eventId,omitempty"`
	Count      int       `json:"count"`
	Failed     bool      `json:"failed"`
}

type Worker struct {
	store     Store
	publisher Publisher
	options   Options
}

func NewWorker(store Store, publisher Publisher, options Options) *Worker {
	return &Worker{store: store, publisher: publisher, options: options}
}

// 执行一次轮询，读取->发布->标记已发布
func (w *Worker) RunBatch(ctx context.Context) (count int, resultErr error) {
	leaseUntil := time.Now().Add(w.options.LeaseDuration)
	claimStarted := time.Now()
	claimed, err := w.store.ClaimBatch(
		ctx,
		w.options.WorkerID,
		w.options.BatchSize,
		leaseUntil,
	)
	w.observe("claim", claimStarted, 0, len(claimed), err)
	if err != nil {
		return 0, fmt.Errorf("claim outbox batch: %w", err)
	}

	count = len(claimed)
	// completed advances only after a checkpoint COMMIT is confirmed. Anything
	// else remains recoverable, including ACKed events with an unknown DB result.
	completed := 0
	defer func() {
		if completed == len(claimed) {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		started := time.Now()
		err := w.store.ReleaseBatch(cleanupCtx, eventIDs(claimed[completed:]), w.options.WorkerID)
		w.observe("release", started, 0, len(claimed)-completed, err)
		if err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("release unfinished outbox events: %w", err))
		}
	}()
	checkpointSize := w.options.CheckpointSize
	if checkpointSize <= 0 {
		checkpointSize = 20
	}
	flush := func(end int) error {
		if end == completed {
			return nil
		}
		started := time.Now()
		err := w.store.MarkPublishedBatch(ctx, eventIDs(claimed[completed:end]), w.options.WorkerID, time.Now())
		w.observe("mark_published", started, 0, end-completed, err)
		if err != nil {
			return fmt.Errorf("confirm ACKed outbox checkpoint: %w", err)
		}
		completed = end
		return nil
	}
	for index, event := range claimed {
		if err := ctx.Err(); err != nil {
			return count, err
		}
		if !time.Now().Before(leaseUntil) {
			return count, fmt.Errorf("outbox event %d lease expired before publish", event.EventID)
		}
		publishStarted := time.Now()
		publishErr := w.publisher.Publish(ctx, event.Topic, event.MessageKey, event.Payload)
		w.observe("publish", publishStarted, event.EventID, 1, publishErr)
		if err := publishErr; err != nil {
			// Confirm only the preceding ACKed prefix. The failed event and every
			// later event must never enter this checkpoint.
			if markErr := flush(index); markErr != nil {
				return count, errors.Join(err, markErr)
			}
			nextRetryAt := time.Now().Add(retryBackoff(event.RetryCount, w.options.MaxRetryBackoff))
			scheduleStarted := time.Now()
			scheduleErr := w.store.Reschedule(ctx, event.EventID, w.options.WorkerID, nextRetryAt, err.Error())
			w.observe("reschedule", scheduleStarted, event.EventID, 1, scheduleErr)
			if scheduleErr != nil {
				return count, fmt.Errorf("publish outbox event %d: %v; reschedule: %w", event.EventID, err, scheduleErr)
			}
			return count, fmt.Errorf("publish outbox event %d: %w", event.EventID, err)
		}
		if index+1-completed >= checkpointSize {
			if err := flush(index + 1); err != nil {
				return count, err
			}
		}
	}
	return count, flush(len(claimed))
}

func eventIDs(events []Event) []int64 {
	ids := make([]int64, len(events))
	for i, event := range events {
		ids[i] = event.EventID
	}
	return ids
}

func (w *Worker) observe(phase string, started time.Time, eventID int64, count int, err error) {
	if w.options.Observe == nil {
		return
	}
	timing := Timing{At: time.Now().UTC(), Phase: phase,
		DurationMs: float64(time.Since(started)) / float64(time.Millisecond), Count: count, Failed: err != nil}
	if eventID != 0 {
		timing.EventID = strconv.FormatInt(eventID, 10)
	}
	w.options.Observe(timing)
}

func retryBackoff(retryCount int64, max time.Duration) time.Duration {
	delay := time.Second
	for attempts := int64(0); attempts < retryCount && delay < max; attempts++ {
		if delay > max/2 {
			return max
		}
		delay *= 2
	}
	if delay > max {
		return max
	}
	return delay
}
