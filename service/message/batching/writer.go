// Package batching adapts OpenIM's conversation-key grouping and sharded batch
// processing pattern to this project's MySQL stored-ACK contract. It is an
// in-memory scheduler, not a durable acceptance queue.
package batching

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"strconv"
	"sync"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"infinitechatgo/common/messagetrace"
	messageevent "infinitechatgo/common/model/event"
	"infinitechatgo/common/txctx"
	"infinitechatgo/service/message/application"
)

var (
	ErrClosed    = errors.New("message batch writer is closed")
	ErrQueueFull = errors.New("message batch queue is full")
)

type Config struct {
	Workers               int  `json:",optional"`
	QueueCapacity         int  `json:",optional"` // Per worker, excluding its active batch.
	BatchSize             int  `json:",optional"`
	FlushMillis           int  `json:",optional"`
	TimeoutMillis         int  `json:",optional"`
	SeparateConversations bool `json:",optional"` // rollback option: one Tx per conversation
}

func (c Config) WithDefaults() (Config, error) {
	if c.Workers == 0 {
		c.Workers = 8
	}
	if c.QueueCapacity == 0 {
		c.QueueCapacity = 256
	}
	if c.BatchSize == 0 {
		c.BatchSize = 32
	}
	if c.FlushMillis == 0 {
		c.FlushMillis = 2
	}
	if c.TimeoutMillis == 0 {
		c.TimeoutMillis = 10000
	}
	if c.Workers < 1 || c.Workers > 128 || c.QueueCapacity < 1 || c.QueueCapacity > 4096 ||
		c.BatchSize < 1 || c.BatchSize > application.MaxBatchSize || c.FlushMillis < 1 || c.FlushMillis > 100 ||
		c.TimeoutMillis < 1 || c.TimeoutMillis > 30000 {
		return Config{}, fmt.Errorf("invalid message batching configuration")
	}
	return c, nil
}

// Publisher must enqueue without waiting for a Kafka ACK. False means the
// notification was dropped; committed content is still recoverable by sync.
type Publisher interface {
	TryPublish(message messageevent.ChatMessage, fingerprint string) bool
}

type outcome struct {
	result application.Result
	err    error
}

type request struct {
	ctx     context.Context
	command application.Command
	reply   chan outcome
}

type Writer struct {
	config    Config
	store     application.BatchPersistentWriter
	publisher Publisher
	queues    []chan *request
	mu        sync.RWMutex
	closed    bool
	workers   sync.WaitGroup
}

func New(store application.BatchPersistentWriter, publisher Publisher, config Config) (*Writer, error) {
	if store == nil {
		return nil, application.ErrPersistenceDependencyMissing
	}
	config, err := config.WithDefaults()
	if err != nil {
		return nil, err
	}
	w := &Writer{config: config, store: store, publisher: publisher, queues: make([]chan *request, config.Workers)}
	for i := range w.queues {
		w.queues[i] = make(chan *request, config.QueueCapacity)
		w.workers.Add(1)
		go w.run(w.queues[i])
	}
	return w, nil
}

func (w *Writer) Persist(ctx context.Context, command application.Command) (application.Result, error) {
	if err := application.ValidateCommand(command); err != nil {
		return application.Result{}, err
	}
	if command.Source != application.SourceRealtime || txctx.GetSession(ctx) != nil {
		return application.Result{}, fmt.Errorf("%w: batch scheduler cannot own an external transaction or red packet", application.ErrInvalidCommand)
	}
	if err := ctx.Err(); err != nil {
		return application.Result{}, err
	}
	r := &request{ctx: ctx, command: command, reply: make(chan outcome, 1)}
	// Closing and enqueueing are coordinated so no sender can write to a closed
	// queue. Admission is nonblocking; full queues never masquerade as stored.
	w.mu.RLock()
	if w.closed {
		w.mu.RUnlock()
		return application.Result{}, ErrClosed
	}
	queue := w.queues[conversationShard(command.Message.SessionID, len(w.queues))]
	messagetrace.Intent(command.Message.SenderID, command.Message.ClientMessageID, "batch_enqueue_start")
	select {
	case queue <- r:
		w.mu.RUnlock()
	default:
		w.mu.RUnlock()
		return application.Result{}, ErrQueueFull
	}
	select {
	case result := <-r.reply:
		return result.result, result.err
	case <-ctx.Done():
		// An executing batch may still commit. Retry the same idempotency key;
		// never interpret a client timeout as proof that the message is absent.
		return application.Result{}, ctx.Err()
	}
}

// Hash all ID digits: Snowflake IDs created in different milliseconds often
// have identical low sequence bits, so raw ID modulo would skew their shards.
func conversationShard(id int64, workers int) uint64 {
	h := fnv.New64a()
	var buffer [20]byte
	_, _ = h.Write(strconv.AppendInt(buffer[:0], id, 10))
	return h.Sum64() % uint64(workers)
}

// Close rejects new admission and drains admitted requests. Every DB operation
// has a bounded timeout; queued requests whose contexts expired are discarded.
func (w *Writer) Close() {
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		for _, queue := range w.queues {
			close(queue)
		}
	}
	w.mu.Unlock()
	w.workers.Wait()
}

func (w *Writer) run(queue <-chan *request) {
	defer w.workers.Done()
	for first := range queue {
		batch := []*request{first}
		timer := time.NewTimer(time.Duration(w.config.FlushMillis) * time.Millisecond)
		closed := false
	collect:
		for len(batch) < w.config.BatchSize {
			select {
			case next, ok := <-queue:
				if !ok {
					closed = true
					break collect
				}
				batch = append(batch, next)
			case <-timer.C:
				break collect
			}
		}
		timer.Stop()
		if !w.config.SeparateConversations {
			w.process(batch)
			if closed {
				return
			}
			continue
		}
		groups := make(map[int64][]*request)
		order := make([]int64, 0, len(batch))
		for _, r := range batch {
			if err := r.ctx.Err(); err != nil {
				r.reply <- outcome{err: err}
				continue
			}
			id := r.command.Message.SessionID
			if _, exists := groups[id]; !exists {
				order = append(order, id)
			}
			groups[id] = append(groups[id], r)
		}
		for _, id := range order {
			w.process(groups[id])
		}
		if closed {
			return
		}
	}
}

func (w *Writer) process(requests []*request) {
	// Re-check immediately before this conversation starts: another group in
	// the same shard may have spent time doing I/O.
	active := make([]*request, 0, len(requests))
	commands := make([]application.Command, 0, len(requests))
	for _, r := range requests {
		if err := r.ctx.Err(); err != nil {
			r.reply <- outcome{err: err}
			continue
		}
		active = append(active, r)
		commands = append(commands, r.command)
	}
	if len(active) == 0 {
		return
	}
	// One caller cancelling must not abort unrelated requests in its batch.
	// Preserve context values, but bound the owned transaction independently.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(active[0].ctx), time.Duration(w.config.TimeoutMillis)*time.Millisecond)
	defer cancel()
	for _, command := range commands {
		messagetrace.Intent(command.Message.SenderID, command.Message.ClientMessageID, "write_start")
	}
	results, err := w.store.PersistBatch(ctx, commands)
	if isDeadlock(err) && ctx.Err() == nil {
		// MySQL 1213 aborts the transaction. Retry once with fresh Seq allocation;
		// connection errors and unknown COMMIT results never take this path.
		results, err = w.store.PersistBatch(ctx, commands)
	}
	if errors.Is(err, application.ErrBatchNeedsSplit) {
		w.resolveIndividually(ctx, active)
		return
	}
	if err == nil && len(results) != len(active) {
		err = fmt.Errorf("%w: batch result count mismatch", application.ErrUniqueConstraintViolation)
	}
	if err != nil {
		for _, r := range active {
			r.reply <- outcome{err: err}
		}
		return
	}
	committedAt := messagetrace.Stamp()
	// Enqueue notifications in batch Seq order before releasing individual
	// waiters. This does not claim ordering against another service's publisher.
	for i, result := range results {
		if result.Created {
			messagetrace.MessageAt(result.Message, "committed", committedAt)
		}
		w.publish(result, commands[i].Fingerprint)
	}
	for i, r := range active {
		r.reply <- outcome{result: results[i]}
	}
}

func (w *Writer) resolveIndividually(ctx context.Context, requests []*request) {
	for i, r := range requests {
		if err := r.ctx.Err(); err != nil {
			r.reply <- outcome{err: err}
			continue
		}
		result, err := w.store.Persist(ctx, r.command)
		if err == nil {
			if result.Created {
				messagetrace.Message(result.Message, "committed")
			}
			w.publish(result, r.command.Fingerprint)
		}
		r.reply <- outcome{result: result, err: err}
		if err != nil && !errors.Is(err, application.ErrIdempotencyConflict) && !errors.Is(err, application.ErrInvalidCommand) {
			// A semantic conflict can be isolated. Infrastructure failures or an
			// invariant violation must not fan out into a database retry storm.
			for _, pending := range requests[i+1:] {
				pending.reply <- outcome{err: err}
			}
			return
		}
	}
}

func (w *Writer) publish(result application.Result, fingerprint string) {
	if result.Created && w.publisher != nil && !w.publisher.TryPublish(result.Message, fingerprint) {
		log.Printf("best-effort message publish queue full messageId=%d", result.Message.MessageID)
	}
}

func isDeadlock(err error) bool {
	var mysqlErr *mysqlDriver.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1213
}
