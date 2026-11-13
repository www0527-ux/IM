package syncer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"infinitechatgo/common/idgen"
	"infinitechatgo/common/messageidentity"
	messageevent "infinitechatgo/common/model/event"
	"infinitechatgo/service/message/application"
	sequence "infinitechatgo/service/message/model/conversation_sequence"
	message "infinitechatgo/service/message/model/message"
	"infinitechatgo/service/message/persistence"
	offlinemodel "infinitechatgo/service/offline/model/message"
)

// SQL is mocked: this covers the new batch-write DTOs through the actual sync
// reader with no Kafka event or Redis cache, not a real database acceptance run.
func TestCommittedBatchCanBeRecoveredBySeqWithoutNotifications(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn := sqlx.NewSqlConnFromDB(db)
	writer, err := persistence.NewWriter(persistence.Dependencies{SqlConn: conn,
		IDGenerator: idgen.MustNew(idgen.Config{NodeID: 8}), Messages: message.NewModel(conn), Sequences: sequence.NewModel(conn)})
	if err != nil {
		t.Fatal(err)
	}
	var commands []application.Command
	for _, key := range []string{"missed-first", "missed-last"} {
		m := messageevent.ChatMessage{SenderID: 1, ClientMessageID: key, SessionID: 2,
			SessionType: messageevent.SessionTypeGroup, MessageType: messageevent.MessageTypeText, Body: messageevent.MessageBody{Content: key}}
		fp, err := messageidentity.Fingerprint(m)
		if err != nil {
			t.Fatal(err)
		}
		commands = append(commands, application.Command{Source: application.SourceRealtime, Message: m, Fingerprint: fp})
	}
	columns := strings.Split("message_id,client_message_id,sender_id,session_id,seq,session_type,message_type,red_packet_id,content,reply_id,created_time", ",")
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .* FROM message WHERE").WillReturnRows(sqlmock.NewRows(columns))
	mock.ExpectExec("INSERT INTO conversation_sequence").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT last_seq").WillReturnRows(sqlmock.NewRows([]string{"last_seq"}).AddRow(0))
	mock.ExpectExec("UPDATE conversation_sequence").WithArgs(int64(2), int64(2)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO message").WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()
	stored, err := writer.PersistBatch(context.Background(), commands)
	if err != nil {
		t.Fatal(err)
	}
	reader := NewReader(fakeCache{err: errors.New("Redis unavailable")}, offlinemodel.NewMessageModel(conn), fakeSessionAccess{allowed: true})
	for index, result := range stored {
		m := result.Message
		mock.ExpectQuery("SELECT COALESCE\\(MAX\\(seq\\), 0\\)").WithArgs(int64(2)).WillReturnRows(sqlmock.NewRows([]string{"max_seq"}).AddRow(2))
		mock.ExpectQuery("SELECT .* FROM `message` WHERE session_id = \\? AND seq > \\? AND seq <= \\? ORDER BY seq ASC").
			WithArgs(int64(2), int64(index), int64(index+1)).
			WillReturnRows(sqlmock.NewRows(columns).AddRow(m.MessageID, m.ClientMessageID, m.SenderID, m.SessionID, m.Seq,
				int64(m.SessionType), int64(m.MessageType), nil, m.Body.Content, nil, time.UnixMilli(m.CreatedAt)))
		page, err := reader.Sync(context.Background(), 1, 2, int64(index), 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Messages) != 1 || page.Messages[0].MessageID != m.MessageID || page.NextSeq != m.Seq || page.HasMore != (index == 0) {
			t.Fatalf("page %d: %+v", index, page)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
