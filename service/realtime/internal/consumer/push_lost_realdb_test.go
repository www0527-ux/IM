//go:build integration

package consumer

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"infinitechatgo/common/idgen"
	"infinitechatgo/common/kafkareader"
	"infinitechatgo/common/messageidentity"
	"infinitechatgo/common/messagepublish"
	event "infinitechatgo/common/model/event"
	messageapp "infinitechatgo/service/message/application"
	sequence "infinitechatgo/service/message/model/conversation_sequence"
	message "infinitechatgo/service/message/model/message"
	"infinitechatgo/service/message/persistence"
	realtimews "infinitechatgo/service/realtime/internal/websocket"
)

type commitObserver struct {
	*kafkareader.Reader
	committed chan kafka.Message
}

func (r *commitObserver) CommitMessages(ctx context.Context, messages ...kafka.Message) error {
	if err := r.Reader.CommitMessages(ctx, messages...); err != nil {
		return err
	}
	select {
	case r.committed <- messages[0]:
	default:
	}
	return nil
}

func TestRealKafkaPushWithoutConnectionCommitsOffset(t *testing.T) {
	dsn, addr, topic := os.Getenv("IM_ISOLATED_ATOMIC_TEST_DSN"), os.Getenv("IM_ISOLATED_KAFKA_ADDR"), os.Getenv("IM_ISOLATED_KAFKA_TOPIC")
	sessionID, err := strconv.ParseInt(os.Getenv("IM_PUSH_SESSION_ID"), 10, 64)
	if dsn == "" || addr == "" || topic == "" || err != nil || sessionID <= 0 {
		t.Skip("isolated database, Kafka, topic and session not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn := sqlx.NewMysql(dsn)
	ids := idgen.MustNew(idgen.Config{NodeID: 97})
	writer, err := persistence.NewWriter(persistence.Dependencies{SqlConn: conn, IDGenerator: ids, Messages: message.NewModel(conn), Sequences: sequence.NewModel(conn)})
	if err != nil {
		t.Fatal(err)
	}
	receiver := sessionID + 2
	msg := event.ChatMessage{SenderID: sessionID + 1, ClientMessageID: "kafka-no-connection", SessionID: sessionID, SessionType: event.SessionTypeSingle, ReceiverID: &receiver, MessageType: event.MessageTypeText, Body: event.MessageBody{Content: "no websocket, recover by seq"}}
	fp, err := messageidentity.Fingerprint(msg)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := messageapp.NewMessageWriteService(nil, writer).Send(ctx, messageapp.Command{Source: messageapp.SourceRealtime, Message: msg, Fingerprint: fp})
	if err != nil {
		t.Fatal(err)
	}
	manager := realtimews.NewManager()
	if manager.SendToUser(receiver, []byte("probe")) != 0 {
		t.Fatal("test recipient unexpectedly connected")
	}
	group := "no-client-test-" + strconv.FormatInt(sessionID, 10)
	reader := &commitObserver{Reader: kafkareader.New(kafka.ReaderConfig{Brokers: []string{addr}, Topic: topic, GroupID: group, StartOffset: kafka.FirstOffset, CommitInterval: 0}), committed: make(chan kafka.Message, 1)}
	defer reader.Close()
	handler := &MessagePushConsumer{manager: manager, groupMembers: &fakeGroupMemberProvider{}}
	done := make(chan error, 1)
	go func() { done <- RunKafkaConsumer(ctx, reader, handler, group, &scriptedDLQ{}) }()
	pub := messagepublish.New([]string{addr}, topic)
	if !pub.TryPublish(stored.Message, fp) {
		t.Fatal("publish queue full")
	}
	select {
	case record := <-reader.committed:
		if string(record.Key) != strconv.FormatInt(sessionID, 10) {
			t.Fatalf("unexpected record key: %s", record.Key)
		}
	case <-ctx.Done():
		t.Fatal("Kafka record not consumed and committed")
	}
	cancel()
	<-done
	if stored.Message.Seq != 1 {
		t.Fatalf("unexpected sequence %d", stored.Message.Seq)
	}
}
