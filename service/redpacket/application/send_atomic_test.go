package application

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"google.golang.org/grpc"
	"infinitechatgo/common/idgen"
	messageevent "infinitechatgo/common/model/event"
	sequence "infinitechatgo/service/message/model/conversation_sequence"
	message "infinitechatgo/service/message/model/message"
	"infinitechatgo/service/message/persistence"
	balancelog "infinitechatgo/service/redpacket/model/balance_log"
	packet "infinitechatgo/service/redpacket/model/red_packet"
	share "infinitechatgo/service/redpacket/model/red_packet_share"
	balance "infinitechatgo/service/redpacket/model/user_balance"
	"infinitechatgo/service/user/rpc/userrpc"
)

type allowGroupRPC struct{ userrpc.UserRpc }

func (allowGroupRPC) ValidateGroupMessage(context.Context, *userrpc.ValidateGroupMessageReq, ...grpc.CallOption) (*userrpc.MessageValidateResp, error) {
	return &userrpc.MessageValidateResp{Allowed: true}, nil
}

type recordPublisher struct {
	calls int
	msg   messageevent.ChatMessage
}

func (p *recordPublisher) TryPublish(m messageevent.ChatMessage, _ string) bool {
	p.calls++
	p.msg = m
	return true
}

func TestRedPacketMessageSharesFundsTransaction(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "message insert rolls back funds"}[fail], func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			conn := sqlx.NewSqlConnFromDB(db)
			ids := idgen.MustNew(idgen.Config{NodeID: 1})
			writer, err := persistence.NewWriter(persistence.Dependencies{SqlConn: conn, IDGenerator: ids, Messages: message.NewModel(conn), Sequences: sequence.NewModel(conn)})
			if err != nil {
				t.Fatal(err)
			}
			publisher := &recordPublisher{}
			svc := NewSendService(SendDependencies{SqlConn: conn, IDGenerator: ids, UserRPC: allowGroupRPC{}, RedPacketModel: packet.NewRedPacketModel(conn), ShareModel: share.NewRedPacketShareModel(conn), UserBalanceModel: balance.NewUserBalanceModel(conn), BalanceLogModel: balancelog.NewBalanceLogModel(conn), MessageWriter: writer, Publisher: publisher})
			mock.ExpectQuery("(?i)select .* from `red_packet`.*").WillReturnRows(sqlmock.NewRows([]string{"red_packet_id"}))
			mock.ExpectBegin()
			mock.ExpectExec("UPDATE user_balance").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec("INSERT INTO red_packet ").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec("INSERT INTO red_packet_share").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec("INSERT INTO balance_log").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery("SELECT .* FROM message WHERE sender_id").WillReturnRows(sqlmock.NewRows([]string{"message_id"}))
			mock.ExpectExec("INSERT INTO conversation_sequence").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery("SELECT last_seq FROM conversation_sequence").WillReturnRows(sqlmock.NewRows([]string{"last_seq"}).AddRow(4))
			mock.ExpectExec("UPDATE conversation_sequence").WillReturnResult(sqlmock.NewResult(0, 1))
			ins := mock.ExpectExec("INSERT INTO message")
			if fail {
				ins.WillReturnError(errors.New("insert failure"))
				mock.ExpectRollback()
				mock.ExpectQuery("(?i)select .* from `red_packet`.*").WillReturnRows(sqlmock.NewRows([]string{"red_packet_id"}))
			} else {
				ins.WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit()
			}
			result, err := svc.Send(context.Background(), SendCommand{SenderID: 1, ClientMessageID: "request-1", SessionID: 2, SessionType: SessionTypeGroup, TotalAmount: 100, TotalCount: 1})
			if fail {
				if err == nil || publisher.calls != 0 {
					t.Fatalf("err=%v publishes=%d", err, publisher.calls)
				}
			} else if err != nil || publisher.calls != 1 || publisher.msg.Seq != 5 || result.MessageID != publisher.msg.MessageID {
				t.Fatalf("result=%+v err=%v publisher=%+v", result, err, publisher)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
