//go:build integration

package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"google.golang.org/grpc"
	"infinitechatgo/common/idgen"
	"infinitechatgo/common/messageidentity"
	event "infinitechatgo/common/model/event"
	messageapp "infinitechatgo/service/message/application"
	seqmodel "infinitechatgo/service/message/model/conversation_sequence"
	msgmodel "infinitechatgo/service/message/model/message"
	"infinitechatgo/service/message/persistence"
	offlinemodel "infinitechatgo/service/offline/model/message"
	packetapp "infinitechatgo/service/redpacket/application"
	balancelog "infinitechatgo/service/redpacket/model/balance_log"
	packet "infinitechatgo/service/redpacket/model/red_packet"
	share "infinitechatgo/service/redpacket/model/red_packet_share"
	balance "infinitechatgo/service/redpacket/model/user_balance"
	"infinitechatgo/service/user/rpc/userrpc"
)

type allowedGroup struct{ userrpc.UserRpc }

func (allowedGroup) ValidateGroupMessage(context.Context, *userrpc.ValidateGroupMessageReq, ...grpc.CallOption) (*userrpc.MessageValidateResp, error) {
	return &userrpc.MessageValidateResp{Allowed: true}, nil
}

// This publisher records an actual failed Kafka network call. It is injected
// into the red-packet service so that send succeeds despite publish failure.
type unavailablePublisher struct {
	address  string
	failures int
}

func (p *unavailablePublisher) TryPublish(message event.ChatMessage, fingerprint string) bool {
	body, _ := json.Marshal(event.NewChatMessagePersistedEventWithIdentity(message.MessageID, message, fingerprint, message.CreatedAt))
	w := &kafka.Writer{Addr: kafka.TCP(p.address), Topic: "unavailable", Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, BatchSize: 1, MaxAttempts: 1}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	defer w.Close()
	if err := w.WriteMessages(ctx, kafka.Message{Key: []byte(strconv.FormatInt(message.SessionID, 10)), Value: body}); err != nil {
		p.failures++
		return false
	}
	return true
}

func TestKafkaFailureThenSeqSyncRealDatabase(t *testing.T) {
	dsn := os.Getenv("IM_ISOLATED_ATOMIC_TEST_DSN")
	if dsn == "" {
		t.Skip("isolated MySQL not configured")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	conn := sqlx.NewMysql(dsn)
	ctx := context.Background()
	ids := idgen.MustNew(idgen.Config{NodeID: 98})
	// A separate session and user ID keep this test independent of other tests.
	userID := time.Now().UnixNano()
	sessionID := userID - 1
	if _, err := conn.ExecCtx(ctx, "INSERT INTO user_balance(user_id,balance) VALUES (?,?)", userID, 1000); err != nil {
		t.Fatal(err)
	}
	pw, err := persistence.NewWriter(persistence.Dependencies{SqlConn: conn, IDGenerator: ids, Messages: msgmodel.NewModel(conn), Sequences: seqmodel.NewModel(conn)})
	if err != nil {
		t.Fatal(err)
	}
	writer := messageapp.NewMessageWriteService(nil, pw)
	m := event.ChatMessage{SenderID: userID, ClientMessageID: "failed-kafka-normal", SessionID: sessionID, SessionType: event.SessionTypeGroup, MessageType: event.MessageTypeText, Body: event.MessageBody{Content: "normal-failed-publish"}}
	fp, err := messageidentity.Fingerprint(m)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := writer.Send(ctx, messageapp.Command{Source: messageapp.SourceRealtime, Message: m, Fingerprint: fp})
	if err != nil {
		t.Fatal(err)
	}
	publisher := &unavailablePublisher{address: address}
	if publisher.TryPublish(stored.Message, fp) {
		t.Fatal("Kafka publish unexpectedly succeeded")
	}
	sender := packetapp.NewSendService(packetapp.SendDependencies{SqlConn: conn, IDGenerator: ids, UserRPC: allowedGroup{}, RedPacketModel: packet.NewRedPacketModel(conn), ShareModel: share.NewRedPacketShareModel(conn), UserBalanceModel: balance.NewUserBalanceModel(conn), BalanceLogModel: balancelog.NewBalanceLogModel(conn), MessageWriter: pw, Publisher: publisher})
	red, err := sender.Send(ctx, packetapp.SendCommand{SenderID: userID, ClientMessageID: "failed-kafka-red", SessionID: sessionID, SessionType: packetapp.SessionTypeGroup, TotalAmount: 100, TotalCount: 1})
	if err != nil {
		t.Fatal(err)
	}
	if publisher.failures != 2 {
		t.Fatalf("expected 2 Kafka publish failures, got %d", publisher.failures)
	}
	reader := NewReader(fakeCache{err: errors.New("Redis unavailable")}, offlinemodel.NewMessageModel(conn), fakeSessionAccess{allowed: true})
	page, err := reader.Sync(ctx, userID, sessionID, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Messages) != 1 || page.Messages[0].MessageID != stored.Message.MessageID || page.NextSeq != 1 || !page.HasMore {
		t.Fatalf("normal sync: %+v", page)
	}
	page, err = reader.Sync(ctx, userID, sessionID, page.NextSeq, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Messages) != 1 || page.Messages[0].MessageID != red.MessageID || page.Messages[0].Body.RedPacketID == nil || *page.Messages[0].Body.RedPacketID != red.RedPacketID || page.NextSeq != 2 || page.HasMore {
		t.Fatalf("red packet sync: %+v", page)
	}
}
