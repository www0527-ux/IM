package relay

import (
	"context"
	"errors"
	"time"
)

var ErrCoreNotImplemented = errors.New("outbox relay core is not implemented")

type Event struct {
	EventID    int64
	Topic      string
	MessageKey string
	Payload    string
	RetryCount int64
}

type Store interface {
	ClaimBatch(
		ctx context.Context,
		workerID string,
		batchSize int,
		leaseUntil time.Time,
	) ([]Event, error)
	MarkPublished(ctx context.Context, eventID int64, publishedAt time.Time) error
	Reschedule(
		ctx context.Context,
		eventID int64,
		nextRetryAt time.Time,
		lastError string,
	) error
}

type Publisher interface {
	Publish(ctx context.Context, topic, messageKey, payload string) error
}

type Worker struct {
	store     Store
	publisher Publisher
}

func NewWorker(store Store, publisher Publisher) *Worker {
	return &Worker{store: store, publisher: publisher}
}

func (w *Worker) RunBatch(context.Context) (int, error) {
	// TODO(user): implement lease acquisition (FOR UPDATE SKIP LOCKED), ordered
	// publication, Broker ACK handling, PUBLISHED transition and retry
	// scheduling. Never mark PUBLISHED before the broker acknowledges.
	return 0, ErrCoreNotImplemented
}
