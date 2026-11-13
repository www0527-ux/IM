//go:build integration

package syncer

import (
	"context"
	"errors"
	"os"
	"strconv"
	"testing"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"infinitechatgo/service/offline/model/message"
)

// No Kafka producer or cache is used; the database must be disposable.
func TestRealDatabaseSyncWithoutKafkaOrCache(t *testing.T) {
	dsn := os.Getenv("IM_ISOLATED_ATOMIC_TEST_DSN")
	if dsn == "" {
		t.Skip("isolated test database not configured")
	}
	conn := sqlx.NewMysql(dsn)
	_, err := conn.ExecCtx(context.Background(), `INSERT INTO message
		(message_id, client_message_id, sender_id, session_id, seq, session_type, message_type, red_packet_id, content, created_time)
		VALUES (918211, 'sync-without-kafka', 918212, 918213, 1, 1, 3, 918214, 'packet', NOW(3))`)
	if err != nil {
		t.Fatal(err)
	}
	reader := NewReader(fakeCache{err: errors.New("cache unavailable")}, message.NewMessageModel(conn), fakeSessionAccess{allowed: true})
	result, err := reader.Sync(context.Background(), 918212, 918213, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Messages) != 1 || result.Messages[0].Seq != 1 || result.Messages[0].Body.RedPacketID == nil || result.NextSeq != 1 {
		t.Fatalf("unexpected sync: %+v", result)
	}
}

// Run after TestRealKafkaPushWithoutConnectionCommitsOffset using the same
// disposable database and IM_PUSH_SESSION_ID.
func TestRecoverMessageAfterKafkaPushHadNoConnection(t *testing.T) {
	dsn := os.Getenv("IM_ISOLATED_ATOMIC_TEST_DSN")
	sessionID, err := strconv.ParseInt(os.Getenv("IM_PUSH_SESSION_ID"), 10, 64)
	if dsn == "" || err != nil || sessionID <= 0 {
		t.Skip("isolated fixture not configured")
	}
	conn := sqlx.NewMysql(dsn)
	reader := NewReader(fakeCache{err: errors.New("cache unavailable")}, message.NewMessageModel(conn), fakeSessionAccess{allowed: true})
	result, err := reader.Sync(context.Background(), sessionID+2, sessionID, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Messages) != 1 || result.Messages[0].Seq != 1 || result.Messages[0].ClientMessageID != "kafka-no-connection" || result.Messages[0].Body.Content != "no websocket, recover by seq" || result.NextSeq != 1 {
		t.Fatalf("unexpected recovered message: %+v", result)
	}
}
