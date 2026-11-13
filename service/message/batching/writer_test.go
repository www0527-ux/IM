package batching

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"infinitechatgo/common/messageidentity"
	messageevent "infinitechatgo/common/model/event"
	"infinitechatgo/service/message/application"
)

type testStore struct {
	batch  func(context.Context, []application.Command) ([]application.Result, error)
	single func(context.Context, application.Command) (application.Result, error)
}

func (s testStore) PersistBatch(ctx context.Context, commands []application.Command) ([]application.Result, error) {
	return s.batch(ctx, commands)
}
func (s testStore) Persist(ctx context.Context, command application.Command) (application.Result, error) {
	if s.single == nil {
		panic("unexpected individual retry")
	}
	return s.single(ctx, command)
}

type testPublisher struct {
	mu     sync.Mutex
	seqs   []int64
	accept bool
}

func (p *testPublisher) TryPublish(m messageevent.ChatMessage, _ string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seqs = append(p.seqs, m.Seq)
	return p.accept
}
func (p *testPublisher) published() []int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]int64(nil), p.seqs...)
}

func testCommand(t *testing.T, session int64, key string) application.Command {
	t.Helper()
	m := messageevent.ChatMessage{SenderID: 1, ClientMessageID: key, SessionID: session,
		SessionType: messageevent.SessionTypeGroup, MessageType: messageevent.MessageTypeText,
		Body: messageevent.MessageBody{Content: key}}
	fp, err := messageidentity.Fingerprint(m)
	if err != nil {
		t.Fatal(err)
	}
	return application.Command{Source: application.SourceRealtime, Message: m, Fingerprint: fp}
}
func testRequest(t *testing.T, session int64, key string) *request {
	return &request{ctx: context.Background(), command: testCommand(t, session, key), reply: make(chan outcome, 1)}
}
func committed(commands []application.Command) []application.Result {
	results := make([]application.Result, len(commands))
	for i, command := range commands {
		m := command.Message
		m.MessageID = int64(i + 100)
		m.Seq = int64(i + 1)
		results[i] = application.Result{Message: m, Created: true}
	}
	return results
}
func receive(t *testing.T, ch <-chan outcome) outcome {
	t.Helper()
	select {
	case out := <-ch:
		return out
	case <-time.After(2 * time.Second):
		t.Fatal("request did not complete")
		return outcome{}
	}
}

func TestResultsAndOrderedNotificationsWaitForCommit(t *testing.T) {
	started, commit := make(chan struct{}), make(chan struct{})
	publisher := &testPublisher{accept: false} // A dropped notification cannot turn stored into failure.
	store := testStore{batch: func(_ context.Context, commands []application.Command) ([]application.Result, error) {
		close(started)
		<-commit
		results := committed(commands)
		results[1] = application.Result{Message: results[0].Message, Created: false}
		return results, nil
	}}
	w := &Writer{store: store, publisher: publisher, config: Config{TimeoutMillis: 1000}}
	a, duplicate, b := testRequest(t, 1, "a"), testRequest(t, 1, "a"), testRequest(t, 1, "b")
	done := make(chan struct{})
	go func() { defer close(done); w.process([]*request{a, duplicate, b}) }()
	<-started
	for _, r := range []*request{a, duplicate, b} {
		select {
		case <-r.reply:
			t.Fatal("returned stored before commit")
		default:
		}
	}
	if len(publisher.published()) != 0 {
		t.Fatal("published an uncommitted message")
	}
	close(commit)
	for _, r := range []*request{a, duplicate, b} {
		if out := receive(t, r.reply); out.err != nil {
			t.Fatal(out.err)
		}
	}
	<-done
	seqs := publisher.published()
	if len(seqs) != 2 || seqs[0] != 1 || seqs[1] != 3 {
		t.Fatalf("notifications not ordered or duplicate sent: %v", seqs)
	}
}

func TestUnknownCommitDoesNotPublishOrRetryIndividually(t *testing.T) {
	wanted := errors.New("COMMIT outcome unknown")
	store := testStore{batch: func(context.Context, []application.Command) ([]application.Result, error) { return nil, wanted }}
	publisher := &testPublisher{}
	w := &Writer{store: store, publisher: publisher, config: Config{TimeoutMillis: 1000}}
	a, b := testRequest(t, 1, "a"), testRequest(t, 1, "b")
	w.process([]*request{a, b})
	for _, r := range []*request{a, b} {
		if out := receive(t, r.reply); !errors.Is(out.err, wanted) {
			t.Fatalf("outcome=%+v", out)
		}
	}
	if len(publisher.published()) != 0 {
		t.Fatal("published unknown commit")
	}
}

func TestDeadlockRetriesBatchOnce(t *testing.T) {
	calls := 0
	store := testStore{batch: func(_ context.Context, commands []application.Command) ([]application.Result, error) {
		calls++
		if calls == 1 {
			return nil, &mysqlDriver.MySQLError{Number: 1213, Message: "deadlock"}
		}
		return committed(commands), nil
	}}
	w := &Writer{store: store, config: Config{TimeoutMillis: 1000}}
	r := testRequest(t, 1, "a")
	w.process([]*request{r})
	if out := receive(t, r.reply); out.err != nil || !out.result.Created || calls != 2 {
		t.Fatalf("outcome=%+v calls=%d", out, calls)
	}
}

func TestSplitIsolatesSemanticConflictButStopsOnInfrastructureFailure(t *testing.T) {
	calls := 0
	store := testStore{
		batch: func(context.Context, []application.Command) ([]application.Result, error) {
			return nil, application.ErrBatchNeedsSplit
		},
		single: func(_ context.Context, command application.Command) (application.Result, error) {
			calls++
			switch command.Message.ClientMessageID {
			case "conflict":
				return application.Result{}, application.ErrIdempotencyConflict
			case "offline":
				return application.Result{}, errors.New("database unavailable")
			default:
				return committed([]application.Command{command})[0], nil
			}
		},
	}
	publisher := &testPublisher{accept: true}
	w := &Writer{store: store, publisher: publisher, config: Config{TimeoutMillis: 1000}}
	a, b, c, d := testRequest(t, 1, "conflict"), testRequest(t, 1, "good"), testRequest(t, 1, "offline"), testRequest(t, 1, "not-attempted")
	w.process([]*request{a, b, c, d})
	if out := receive(t, a.reply); !errors.Is(out.err, application.ErrIdempotencyConflict) {
		t.Fatal(out)
	}
	if out := receive(t, b.reply); out.err != nil || !out.result.Created {
		t.Fatal(out)
	}
	if out := receive(t, c.reply); out.err == nil {
		t.Fatal("infrastructure failure was lost")
	}
	if out := receive(t, d.reply); out.err == nil {
		t.Fatal("continued writing after infrastructure failure")
	}
	if calls != 3 || len(publisher.published()) != 1 {
		t.Fatalf("calls=%d published=%v", calls, publisher.published())
	}
}

func TestQueueIsBoundedAndCancelledRequestsAreNotWritten(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	store := testStore{batch: func(_ context.Context, commands []application.Command) ([]application.Result, error) {
		close(started)
		<-release
		return committed(commands), nil
	}}
	w, err := New(store, nil, Config{Workers: 1, QueueCapacity: 1, BatchSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Close)
	done := make(chan outcome, 1)
	command := testCommand(t, 1, "running")
	go func() { result, err := w.Persist(context.Background(), command); done <- outcome{result, err} }()
	<-started
	// Admit a second request deterministically while the worker is blocked.
	queued := testRequest(t, 1, "cancelled")
	ctx, cancel := context.WithCancel(context.Background())
	queued.ctx = ctx
	w.queues[0] <- queued
	_, err = w.Persist(context.Background(), testCommand(t, 1, "full"))
	cancel()
	close(release)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("full queue error=%v", err)
	}
	if out := receive(t, done); out.err != nil {
		t.Fatal(out.err)
	}
	w.Close()
	if out := receive(t, queued.reply); !errors.Is(out.err, context.Canceled) {
		t.Fatalf("cancelled request=%+v", out)
	}
	if _, err := w.Persist(context.Background(), command); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed writer error=%v", err)
	}
}

func TestDifferentShardsCanPersistWhileOneConversationWaits(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	store := testStore{batch: func(_ context.Context, commands []application.Command) ([]application.Result, error) {
		if commands[0].Message.SessionID == 1 {
			close(started)
			<-release
		}
		for _, command := range commands {
			if command.Message.SessionID != commands[0].Message.SessionID {
				t.Error("mixed conversations in transaction")
			}
		}
		return committed(commands), nil
	}}
	w, err := New(store, nil, Config{Workers: 2, BatchSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Close)
	slow, fast := make(chan outcome, 1), make(chan outcome, 1)
	first, other := testCommand(t, 1, "slow"), testCommand(t, 2, "fast")
	go func() { result, err := w.Persist(context.Background(), first); slow <- outcome{result, err} }()
	<-started
	go func() { result, err := w.Persist(context.Background(), other); fast <- outcome{result, err} }()
	out := receive(t, fast)
	close(release)
	if out.err != nil {
		t.Fatal(out.err)
	}
	if out := receive(t, slow); out.err != nil {
		t.Fatal(out.err)
	}
}

func TestCancellationOfOneExecutingRequestDoesNotCancelItsBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := testStore{batch: func(batchCtx context.Context, commands []application.Command) ([]application.Result, error) {
		cancel()
		if batchCtx.Err() != nil {
			t.Fatal("one caller cancelled unrelated writes")
		}
		return committed(commands), nil
	}}
	w := &Writer{store: store, config: Config{TimeoutMillis: 1000}}
	a, b := testRequest(t, 1, "a"), testRequest(t, 1, "b")
	a.ctx = ctx
	w.process([]*request{a, b})
	if out := receive(t, b.reply); out.err != nil {
		t.Fatal(out.err)
	}
}

func TestConfigDefaultsAndRejectsUnboundedSettings(t *testing.T) {
	defaults, err := (Config{}).WithDefaults()
	if err != nil || defaults.BatchSize != 32 || defaults.FlushMillis != 2 {
		t.Fatalf("defaults=%+v err=%v", defaults, err)
	}
	for _, config := range []Config{{Workers: -1}, {QueueCapacity: -1}, {BatchSize: application.MaxBatchSize + 1}, {FlushMillis: 101}, {TimeoutMillis: 30001}} {
		if _, err := config.WithDefaults(); err == nil {
			t.Fatalf("invalid settings accepted: %+v", config)
		}
	}
}

func TestWorkerGroupsCollectedRequestsIntoSeparateConversationTransactions(t *testing.T) {
	var sessions []int64
	store := testStore{batch: func(_ context.Context, commands []application.Command) ([]application.Result, error) {
		if len(commands) != 2 {
			t.Errorf("expected two requests per conversation, got %d", len(commands))
		}
		for _, command := range commands {
			if command.Message.SessionID != commands[0].Message.SessionID {
				t.Error("mixed conversations in one transaction")
			}
		}
		sessions = append(sessions, commands[0].Message.SessionID)
		return committed(commands), nil
	}}
	queue := make(chan *request, 4)
	requests := []*request{testRequest(t, 1, "a"), testRequest(t, 2, "x"), testRequest(t, 1, "b"), testRequest(t, 2, "y")}
	for _, r := range requests {
		queue <- r
	}
	close(queue)
	w := &Writer{store: store, config: Config{BatchSize: 4, FlushMillis: 2, TimeoutMillis: 1000, SeparateConversations: true}}
	w.workers.Add(1)
	go w.run(queue)
	for _, r := range requests {
		if out := receive(t, r.reply); out.err != nil {
			t.Fatal(out.err)
		}
	}
	w.workers.Wait()
	if len(sessions) != 2 || sessions[0] != 1 || sessions[1] != 2 {
		t.Fatalf("conversation transaction order=%v", sessions)
	}
}

func TestExecutingRequestCanTimeOutWithoutLosingPostcommitNotification(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	store := testStore{batch: func(_ context.Context, commands []application.Command) ([]application.Result, error) {
		close(started)
		<-release
		return committed(commands), nil
	}}
	publisher := &testPublisher{accept: true}
	w, err := New(store, publisher, Config{Workers: 1, BatchSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Close)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan outcome, 1)
	command := testCommand(t, 1, "timeout-after-start")
	go func() { result, err := w.Persist(ctx, command); done <- outcome{result, err} }()
	<-started
	cancel()
	out := receive(t, done)
	close(release)
	w.Close()
	if !errors.Is(out.err, context.Canceled) || out.result.Created {
		t.Fatalf("caller outcome=%+v", out)
	}
	if seqs := publisher.published(); len(seqs) != 1 || seqs[0] != 1 {
		t.Fatalf("committed message notification lost: %v", seqs)
	}
}
