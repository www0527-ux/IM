package persistence

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"infinitechatgo/common/idgen"
	"infinitechatgo/common/messageidentity"
	messageevent "infinitechatgo/common/model/event"
	"infinitechatgo/service/message/application"
	sequence "infinitechatgo/service/message/model/conversation_sequence"
	message "infinitechatgo/service/message/model/message"
)

func batchCommand(t *testing.T, key string) application.Command {
	t.Helper()
	m := messageevent.ChatMessage{SenderID: 10, ClientMessageID: key, SessionID: 20,
		SessionType: messageevent.SessionTypeGroup, MessageType: messageevent.MessageTypeText,
		Body: messageevent.MessageBody{Content: "hello-" + key}}
	fp, err := messageidentity.Fingerprint(m)
	if err != nil {
		t.Fatal(err)
	}
	return application.Command{Source: application.SourceRealtime, Message: m, Fingerprint: fp}
}

func batchTestWriter(t *testing.T) (*Writer, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Close()
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
	conn := sqlx.NewSqlConnFromDB(db)
	w, err := NewWriter(Dependencies{SqlConn: conn, IDGenerator: idgen.MustNew(idgen.Config{NodeID: 9}),
		Messages: message.NewModel(conn), Sequences: sequence.NewModel(conn)})
	if err != nil {
		t.Fatal(err)
	}
	return w, mock
}

func storedRows() *sqlmock.Rows {
	return sqlmock.NewRows(strings.Split("message_id,client_message_id,sender_id,session_id,seq,session_type,message_type,red_packet_id,content,reply_id,created_time", ","))
}

func expectBatchAllocation(mock sqlmock.Sqlmock, last, count int64) {
	mock.ExpectExec("INSERT INTO conversation_sequence").WithArgs(int64(20)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT last_seq FROM conversation_sequence").WithArgs(int64(20)).
		WillReturnRows(sqlmock.NewRows([]string{"last_seq"}).AddRow(last))
	mock.ExpectExec("UPDATE conversation_sequence").WithArgs(last+count, int64(20)).WillReturnResult(sqlmock.NewResult(0, 1))
}

func TestPersistBatchSharesAllocationAndDeduplicatesBeforeInsert(t *testing.T) {
	w, mock := batchTestWriter(t)
	a, b, existing := batchCommand(t, "a"), batchCommand(t, "b"), batchCommand(t, "existing")
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .* FROM message WHERE \\(sender_id, client_message_id\\) IN").
		WithArgs(int64(10), "a", int64(10), "b", int64(10), "existing").
		WillReturnRows(storedRows().AddRow(999, "existing", 10, 20, 7, 1, 0, nil, "hello-existing", nil, time.Now()))
	expectBatchAllocation(mock, 10, 2)
	mock.ExpectExec("INSERT INTO message .* VALUES \\(.*\\), \\(.*\\)").WithArgs(
		sqlmock.AnyArg(), "a", int64(10), int64(20), int64(11), int64(a.Message.SessionType), int64(a.Message.MessageType), nil, "hello-a", nil, sqlmock.AnyArg(),
		sqlmock.AnyArg(), "b", int64(10), int64(20), int64(12), int64(b.Message.SessionType), int64(b.Message.MessageType), nil, "hello-b", nil, sqlmock.AnyArg(),
	).WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()
	results, err := w.PersistBatch(context.Background(), []application.Command{a, a, b, existing})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 4 || !results[0].Created || results[1].Created || !results[2].Created || results[3].Created ||
		results[0].Message.Seq != 11 || results[1].Message.MessageID != results[0].Message.MessageID ||
		results[2].Message.Seq != 12 || results[3].Message.MessageID != 999 {
		t.Fatalf("incorrect per-request outcomes: %+v", results)
	}
}

func TestPersistBatchFailureNeverReturnsPrecommitResults(t *testing.T) {
	for _, stage := range []string{"insert", "duplicate", "commit"} {
		t.Run(stage, func(t *testing.T) {
			w, mock := batchTestWriter(t)
			mock.ExpectBegin()
			mock.ExpectQuery("SELECT .* FROM message WHERE").WillReturnRows(storedRows())
			expectBatchAllocation(mock, 100, 2)
			insert := mock.ExpectExec("INSERT INTO message")
			switch stage {
			case "insert":
				insert.WillReturnError(errors.New("disk unavailable"))
				mock.ExpectRollback()
			case "duplicate":
				insert.WillReturnError(&mysqlDriver.MySQLError{Number: 1062, Message: "unique collision"})
				mock.ExpectRollback()
			case "commit":
				insert.WillReturnResult(sqlmock.NewResult(0, 2))
				mock.ExpectCommit().WillReturnError(errors.New("COMMIT acknowledgement lost"))
			}
			results, err := w.PersistBatch(context.Background(), []application.Command{batchCommand(t, "a"), batchCommand(t, "b")})
			if err == nil || results != nil {
				t.Fatalf("results=%+v error=%v", results, err)
			}
			if errors.Is(err, application.ErrBatchNeedsSplit) != (stage == "duplicate") {
				t.Fatalf("wrong split classification: %v", err)
			}
		})
	}
}

func TestPersistBatchRetryAllocatesFromRolledBackCounter(t *testing.T) {
	w, mock := batchTestWriter(t)
	commands := []application.Command{batchCommand(t, "a"), batchCommand(t, "b")}
	for attempt := 0; attempt < 2; attempt++ {
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT .* FROM message WHERE").WillReturnRows(storedRows())
		expectBatchAllocation(mock, 100, 2)
		insert := mock.ExpectExec("INSERT INTO message")
		if attempt == 0 {
			insert.WillReturnError(errors.New("write failed"))
			mock.ExpectRollback()
		} else {
			insert.WillReturnResult(sqlmock.NewResult(0, 2))
			mock.ExpectCommit()
		}
	}
	if _, err := w.PersistBatch(context.Background(), commands); err == nil {
		t.Fatal("first insert succeeded")
	}
	results, err := w.PersistBatch(context.Background(), commands)
	if err != nil || results[0].Message.Seq != 101 || results[1].Message.Seq != 102 {
		t.Fatalf("retry results=%+v err=%v", results, err)
	}
}

func TestPersistBatchExistingMessagesDoNotAdvanceCounter(t *testing.T) {
	w, mock := batchTestWriter(t)
	command := batchCommand(t, "a")
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .* FROM message WHERE").WillReturnRows(storedRows().AddRow(
		100, "a", 10, 20, 3, int64(command.Message.SessionType), int64(command.Message.MessageType), nil, "hello-a", nil, time.Now()))
	mock.ExpectCommit()
	results, err := w.PersistBatch(context.Background(), []application.Command{command})
	if err != nil || len(results) != 1 || results[0].Created || results[0].Message.Seq != 3 {
		t.Fatalf("results=%+v err=%v", results, err)
	}
}

func TestPersistBatchIsolatesConflictsAndRejectsExternalSources(t *testing.T) {
	w, mock := batchTestWriter(t)
	a, conflict := batchCommand(t, "a"), batchCommand(t, "a")
	conflict.Message.Body.Content = "different intent"
	conflict.Fingerprint, _ = messageidentity.Fingerprint(conflict.Message)
	if _, err := w.PersistBatch(context.Background(), []application.Command{a, conflict}); !errors.Is(err, application.ErrBatchNeedsSplit) {
		t.Fatalf("batch conflict: %v", err)
	}
	other := batchCommand(t, "b")
	other.Source = application.SourceRedPacket
	other.Fingerprint, _ = messageidentity.Fingerprint(other.Message)
	if _, err := w.PersistBatch(context.Background(), []application.Command{a, other}); !errors.Is(err, application.ErrInvalidCommand) {
		t.Fatalf("red packet in ordinary batch: %v", err)
	}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .* FROM message WHERE").WillReturnRows(storedRows().AddRow(
		100, "a", 10, 20, 3, int64(a.Message.SessionType), int64(a.Message.MessageType), nil, "different intent", nil, time.Now()))
	mock.ExpectRollback()
	if _, err := w.PersistBatch(context.Background(), []application.Command{a}); !errors.Is(err, application.ErrBatchNeedsSplit) {
		t.Fatalf("persisted conflict: %v", err)
	}
}
