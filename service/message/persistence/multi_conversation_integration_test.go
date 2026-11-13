//go:build integration

package persistence

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"infinitechatgo/common/idgen"
	"infinitechatgo/common/messageidentity"
	event "infinitechatgo/common/model/event"
	"infinitechatgo/service/message/application"
	sequence "infinitechatgo/service/message/model/conversation_sequence"
	message "infinitechatgo/service/message/model/message"
)

type failMultiInsert struct{ message.Model }

func (m failMultiInsert) InsertBatchTx(ctx context.Context, rows []event.ChatMessage) error {
	if err := m.Model.InsertBatchTx(ctx, rows); err != nil {
		return err
	}
	return errors.New("injected failure after inserting all conversations")
}
func TestRealMultiConversationCommitRollbackAndIdempotency(t *testing.T) {
	dsn := os.Getenv("IM_PIPELINE_TEST_DSN")
	parsed, err := mysqldriver.ParseDSN(dsn)
	if err != nil || !strings.HasPrefix(parsed.DBName, "im_push_opt_") {
		t.Skip("disposable im_push_opt_ database required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn := sqlx.NewMysql(dsn)
	base := time.Now().UnixNano()
	deps := Dependencies{SqlConn: conn, IDGenerator: idgen.MustNew(idgen.Config{NodeID: 96}), Messages: message.NewModel(conn), Sequences: sequence.NewModel(conn)}
	w, err := NewWriter(deps)
	if err != nil {
		t.Fatal(err)
	}
	command := func(session int64, key string) application.Command {
		m := event.ChatMessage{SenderID: base + 2, SessionID: session, SessionType: event.SessionTypeGroup, ClientMessageID: key, MessageType: event.MessageTypeText, Body: event.MessageBody{Content: key}}
		fp, err := messageidentity.Fingerprint(m)
		if err != nil {
			t.Fatal(err)
		}
		return application.Command{Source: application.SourceRealtime, Message: m, Fingerprint: fp}
	}
	initial := []application.Command{command(base+1, "b1"), command(base, "a1"), command(base+1, "b2"), command(base, "a2")}
	stored, err := w.PersistBatch(ctx, initial)
	if err != nil {
		t.Fatal(err)
	}
	for i, result := range stored {
		if result.Message.Seq != int64(i/2+1) {
			t.Fatalf("per-conversation order: %+v", stored)
		}
	}
	next := []application.Command{command(base, "a3"), command(base+1, "b3")}
	failedDeps := deps
	failedDeps.Messages = failMultiInsert{deps.Messages}
	failed, _ := NewWriter(failedDeps)
	if results, err := failed.PersistBatch(ctx, next); err == nil || results != nil {
		t.Fatalf("rollback leaked success: %+v %v", results, err)
	}
	check := func(want int64) {
		t.Helper()
		for _, id := range []int64{base, base + 1} {
			var count, last int64
			if err := conn.QueryRowCtx(ctx, &count, "SELECT COUNT(*) FROM message WHERE session_id=?", id); err != nil {
				t.Fatal(err)
			}
			if err := conn.QueryRowCtx(ctx, &last, "SELECT last_seq FROM conversation_sequence WHERE session_id=?", id); err != nil {
				t.Fatal(err)
			}
			if count != want || last != want {
				t.Fatalf("session=%d count=%d counter=%d want=%d", id, count, last, want)
			}
		}
	}
	check(2)
	stored, err = w.PersistBatch(ctx, next)
	if err != nil || stored[0].Message.Seq != 3 || stored[1].Message.Seq != 3 {
		t.Fatalf("retry=%+v err=%v", stored, err)
	}
	check(3)
	repeated, err := w.PersistBatch(ctx, append(initial, next...))
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range repeated {
		if result.Created {
			t.Fatal("idempotent retry rewrote stored message")
		}
	}
	check(3)
}
