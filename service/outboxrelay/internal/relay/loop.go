package relay

import (
	"context"
	"fmt"
	"time"
)

type BatchRunner interface {
	RunBatch(context.Context) (int, error)
}

// Run drains eligible work without sleeping after productive batches. Empty
// batches and failures wait a full interval to avoid spinning or hammering a
// failed dependency. RunBatch still owns ordering, leases and retry scheduling.
func Run(ctx context.Context, runner BatchRunner, pollInterval time.Duration, onError func(error)) error {
	if pollInterval <= 0 {
		return fmt.Errorf("outbox polling interval must be positive")
	}
	for ctx.Err() == nil {
		count, err := runner.RunBatch(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil && onError != nil {
			onError(err)
		}
		if count > 0 && err == nil {
			continue
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
	return nil
}
