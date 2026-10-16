// Package kafkareader distinguishes an application-owned Close from a broker
// disconnect. kafka-go can return raw EOF from its consumer-group runError path.
package kafkareader

import (
	"context"
	"errors"
	"io"
	"sync/atomic"

	"github.com/segmentio/kafka-go"
)

type Client interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
	Close() error
}

type Reader struct {
	client Client
	closed atomic.Bool
}

func New(config kafka.ReaderConfig) *Reader { return Wrap(kafka.NewReader(config)) }

func Wrap(client Client) *Reader { return &Reader{client: client} }

func (r *Reader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	message, err := r.client.FetchMessage(ctx)
	return message, r.classify(err)
}

func (r *Reader) CommitMessages(ctx context.Context, messages ...kafka.Message) error {
	return r.classify(r.client.CommitMessages(ctx, messages...))
}

func (r *Reader) Close() error {
	r.closed.Store(true)
	return r.client.Close()
}

func (r *Reader) classify(err error) error {
	if errors.Is(err, io.EOF) {
		if r.closed.Load() {
			return io.ErrClosedPipe
		}
		return io.ErrUnexpectedEOF
	}
	return err
}
