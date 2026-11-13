package persistence

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"infinitechatgo/common/messageidentity"
	"infinitechatgo/service/message/application"
)

func TestMultiConversationBatchLocksInCanonicalOrderAndRollsBackTogether(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "rollback"}[fail], func(t *testing.T) {
			w, mock := batchTestWriter(t)
			a, b, c := batchCommand(t, "a"), batchCommand(t, "b"), batchCommand(t, "c")
			b.Message.SessionID = 10
			b.Fingerprint, _ = messageidentity.Fingerprint(b.Message)
			mock.ExpectBegin()
			mock.ExpectQuery("SELECT .* FROM message WHERE").WillReturnRows(storedRows())
			// Input starts with session 20, but counters must lock 10 before 20.
			mock.ExpectExec("INSERT INTO conversation_sequence").WithArgs(int64(10), int64(20)).WillReturnResult(sqlmock.NewResult(0, 2))
			mock.ExpectQuery("SELECT session_id,last_seq FROM conversation_sequence").WithArgs(int64(10), int64(20)).WillReturnRows(sqlmock.NewRows([]string{"session_id", "last_seq"}).AddRow(10, 6).AddRow(20, 10))
			mock.ExpectExec("UPDATE conversation_sequence").WithArgs(int64(10), int64(7), int64(20), int64(12), int64(10), int64(20)).WillReturnResult(sqlmock.NewResult(0, 2))
			insert := mock.ExpectExec("INSERT INTO message").WithArgs(
				sqlmock.AnyArg(), "a", int64(10), int64(20), int64(11), int64(1), int64(0), nil, "hello-a", nil, sqlmock.AnyArg(),
				sqlmock.AnyArg(), "b", int64(10), int64(10), int64(7), int64(1), int64(0), nil, "hello-b", nil, sqlmock.AnyArg(),
				sqlmock.AnyArg(), "c", int64(10), int64(20), int64(12), int64(1), int64(0), nil, "hello-c", nil, sqlmock.AnyArg())
			if fail {
				insert.WillReturnError(errors.New("injected insert error"))
				mock.ExpectRollback()
			} else {
				insert.WillReturnResult(sqlmock.NewResult(0, 3))
				mock.ExpectCommit()
			}
			rows, err := w.PersistBatch(context.Background(), []application.Command{a, b, c})
			if fail {
				if err == nil || rows != nil {
					t.Fatalf("precommit results escaped: %+v %v", rows, err)
				}
				return
			}
			if err != nil || len(rows) != 3 || rows[0].Message.Seq != 11 || rows[1].Message.Seq != 7 || rows[2].Message.Seq != 12 {
				t.Fatalf("results=%+v err=%v", rows, err)
			}
		})
	}
}
