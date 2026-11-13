//go:build integration

package syncer

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"infinitechatgo/common/idgen"
	"infinitechatgo/common/messageidentity"
	messageevent "infinitechatgo/common/model/event"
	"infinitechatgo/service/message/application"
	sequence "infinitechatgo/service/message/model/conversation_sequence"
	message "infinitechatgo/service/message/model/message"
	"infinitechatgo/service/message/persistence"
	offlinemodel "infinitechatgo/service/offline/model/message"
	packetapp "infinitechatgo/service/redpacket/application"
	balancelog "infinitechatgo/service/redpacket/model/balance_log"
	packet "infinitechatgo/service/redpacket/model/red_packet"
	share "infinitechatgo/service/redpacket/model/red_packet_share"
	balance "infinitechatgo/service/redpacket/model/user_balance"
)

type failBatchAfterInsert struct{ message.Model }

func (m failBatchAfterInsert) InsertBatchTx(ctx context.Context, messages []messageevent.ChatMessage) error {
	if err := m.Model.InsertBatchTx(ctx, messages); err != nil {
		return err
	}
	return errors.New("injected failure after batch insert, before commit")
}

// Use only a disposable database with the project schema. This is a small
// correctness scenario, not a benchmark: batch -> rollback -> red packet ->
// next batch -> paginated MySQL sync, without Kafka or Redis availability.
func TestBatchAndRedPacketSeqSyncRealDatabase(t *testing.T) {
	dsn := os.Getenv("IM_ISOLATED_ATOMIC_TEST_DSN")
	if dsn == "" {
		t.Skip("disposable MySQL not configured")
	}
	conn := sqlx.NewMysql(dsn)
	ctx := context.Background()
	userID, sessionID := time.Now().UnixNano(), time.Now().UnixNano()+1
	ids := idgen.MustNew(idgen.Config{NodeID: 97})
	models := message.NewModel(conn)
	deps := persistence.Dependencies{SqlConn: conn, IDGenerator: ids, Messages: models, Sequences: sequence.NewModel(conn)}
	w, err := persistence.NewWriter(deps)
	if err != nil {
		t.Fatal(err)
	}
	command := func(key string) application.Command {
		m := messageevent.ChatMessage{SenderID: userID, ClientMessageID: key, SessionID: sessionID,
			SessionType: messageevent.SessionTypeGroup, MessageType: messageevent.MessageTypeText, Body: messageevent.MessageBody{Content: key}}
		fp, err := messageidentity.Fingerprint(m)
		if err != nil {
			t.Fatal(err)
		}
		return application.Command{Source: application.SourceRealtime, Message: m, Fingerprint: fp}
	}
	first := []application.Command{command("batch-a"), command("batch-b")}
	stored, err := w.PersistBatch(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	deps.Messages = failBatchAfterInsert{models}
	failing, err := persistence.NewWriter(deps)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := failing.PersistBatch(ctx, []application.Command{command("rolled-back-a"), command("rolled-back-b")}); err == nil {
		t.Fatal("injected failure did not roll back")
	}
	if _, err := conn.ExecCtx(ctx, "INSERT INTO user_balance(user_id,balance) VALUES (?,?)", userID, 1000); err != nil {
		t.Fatal(err)
	}
	sender := packetapp.NewSendService(packetapp.SendDependencies{SqlConn: conn, IDGenerator: ids, UserRPC: allowedGroup{},
		RedPacketModel: packet.NewRedPacketModel(conn), ShareModel: share.NewRedPacketShareModel(conn),
		UserBalanceModel: balance.NewUserBalanceModel(conn), BalanceLogModel: balancelog.NewBalanceLogModel(conn), MessageWriter: w})
	red, err := sender.Send(ctx, packetapp.SendCommand{SenderID: userID, ClientMessageID: "batch-red", SessionID: sessionID,
		SessionType: packetapp.SessionTypeGroup, TotalAmount: 100, TotalCount: 1})
	if err != nil {
		t.Fatal(err)
	}
	last, err := w.PersistBatch(ctx, []application.Command{command("batch-c")})
	if err != nil {
		t.Fatal(err)
	}
	retry, err := w.PersistBatch(ctx, first)
	if err != nil || retry[0].Created || retry[0].Message.MessageID != stored[0].Message.MessageID {
		t.Fatalf("retry=%+v err=%v", retry, err)
	}
	wanted := []int64{stored[0].Message.MessageID, stored[1].Message.MessageID, red.MessageID, last[0].Message.MessageID}
	reader := NewReader(fakeCache{err: errors.New("Redis unavailable")}, offlinemodel.NewMessageModel(conn), fakeSessionAccess{allowed: true})
	for i, id := range wanted {
		page, err := reader.Sync(ctx, userID, sessionID, int64(i), 1)
		if err != nil || len(page.Messages) != 1 || page.Messages[0].MessageID != id || page.Messages[0].Seq != int64(i+1) || page.NextSeq != int64(i+1) {
			t.Fatalf("sync page %d: %+v err=%v", i, page, err)
		}
	}
	var state struct {
		Count   int64 `db:"count"`
		LastSeq int64 `db:"last_seq"`
		Balance int64 `db:"balance"`
	}
	if err := conn.QueryRowCtx(ctx, &state, `SELECT (SELECT COUNT(*) FROM message WHERE session_id=?) AS count,
		(SELECT last_seq FROM conversation_sequence WHERE session_id=?) AS last_seq,
		(SELECT balance FROM user_balance WHERE user_id=?) AS balance`, sessionID, sessionID, userID); err != nil {
		t.Fatal(err)
	}
	if state.Count != 4 || state.LastSeq != 4 || state.Balance != 900 {
		t.Fatalf("unexpected final state: %+v", state)
	}
}
