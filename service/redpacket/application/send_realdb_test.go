//go:build integration

package application

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"infinitechatgo/common/idgen"
	messageapp "infinitechatgo/service/message/application"
	sequence "infinitechatgo/service/message/model/conversation_sequence"
	message "infinitechatgo/service/message/model/message"
	"infinitechatgo/service/message/persistence"
	balancelog "infinitechatgo/service/redpacket/model/balance_log"
	packet "infinitechatgo/service/redpacket/model/red_packet"
	share "infinitechatgo/service/redpacket/model/red_packet_share"
	balance "infinitechatgo/service/redpacket/model/user_balance"
)

type failAfterMessage struct{ writer messageapp.PersistentWriter }

func (f failAfterMessage) Persist(ctx context.Context, command messageapp.Command) (messageapp.Result, error) {
	_, err := f.writer.Persist(ctx, command)
	if err != nil {
		return messageapp.Result{}, err
	}
	return messageapp.Result{}, errors.New("injected failure after message insert")
}

func TestRealDatabaseRedPacketAtomicCommitAndRollback(t *testing.T) {
	dsn := os.Getenv("IM_ISOLATED_ATOMIC_TEST_DSN")
	if dsn == "" {
		t.Skip("isolated test database not configured")
	}
	conn := sqlx.NewMysql(dsn)
	ctx := context.Background()
	if _, err := conn.ExecCtx(ctx, "INSERT INTO user_balance(user_id,balance) VALUES (?,?)", 918201, 1000); err != nil {
		t.Fatal(err)
	}
	ids := idgen.MustNew(idgen.Config{NodeID: 99})
	writer, err := persistence.NewWriter(persistence.Dependencies{SqlConn: conn, IDGenerator: ids, Messages: message.NewModel(conn), Sequences: sequence.NewModel(conn)})
	if err != nil {
		t.Fatal(err)
	}
	publisher := &recordPublisher{}
	deps := SendDependencies{SqlConn: conn, IDGenerator: ids, UserRPC: allowGroupRPC{}, RedPacketModel: packet.NewRedPacketModel(conn), ShareModel: share.NewRedPacketShareModel(conn), UserBalanceModel: balance.NewUserBalanceModel(conn), BalanceLogModel: balancelog.NewBalanceLogModel(conn), MessageWriter: writer, Publisher: publisher}
	command := SendCommand{SenderID: 918201, ClientMessageID: "real-commit", SessionID: 918202, SessionType: SessionTypeGroup, TotalAmount: 100, TotalCount: 1}
	result, err := NewSendService(deps).Send(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := NewSendService(deps).Send(ctx, command)
	if err != nil || retry.MessageID != result.MessageID {
		t.Fatalf("retry=%+v err=%v", retry, err)
	}
	deps.MessageWriter = failAfterMessage{writer}
	command.ClientMessageID = "real-rollback"
	if _, err := NewSendService(deps).Send(ctx, command); err == nil {
		t.Fatal("expected rollback error")
	}
	var rows struct {
		Balance      int64 `db:"balance"`
		PacketCount  int64 `db:"packet_count"`
		MessageCount int64 `db:"message_count"`
		LogCount     int64 `db:"log_count"`
		LastSeq      int64 `db:"last_seq"`
	}
	if err := conn.QueryRowCtx(ctx, &rows, `SELECT (SELECT balance FROM user_balance WHERE user_id=918201) AS balance, (SELECT COUNT(*) FROM red_packet WHERE sender_id=918201) AS packet_count, (SELECT COUNT(*) FROM message WHERE sender_id=918201) AS message_count, (SELECT COUNT(*) FROM balance_log WHERE user_id=918201) AS log_count, (SELECT last_seq FROM conversation_sequence WHERE session_id=918202) AS last_seq`); err != nil {
		t.Fatal(err)
	}
	if rows.Balance != 900 || rows.PacketCount != 1 || rows.MessageCount != 1 || rows.LogCount != 1 || rows.LastSeq != 1 || publisher.calls != 1 {
		t.Fatalf("incorrect state after commit/retry/rollback: %+v publishes=%d", rows, publisher.calls)
	}
}
