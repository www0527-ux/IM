package application

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	redpacketmodel "infinitechatgo/service/redpacket/model/red_packet"
	"infinitechatgo/service/user/rpc/userrpc"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type detailPacketModel struct {
	redpacketmodel.RedPacketModel
	packet *redpacketmodel.RedPacket
}

func (m detailPacketModel) FindOne(context.Context, int64) (*redpacketmodel.RedPacket, error) {
	return m.packet, nil
}

type detailRowsConn struct {
	sqlx.SqlConn
	queries int
}

func (c *detailRowsConn) QueryRowsCtx(_ context.Context, dest any, query string, args ...any) error {
	c.queries++
	if !strings.Contains(query, "FROM red_packet_receive WHERE red_packet_id = ?") ||
		len(args) != 1 || args[0] != int64(7) {
		return fmt.Errorf("unexpected claim query: %q %v", query, args)
	}
	rows := reflect.ValueOf(dest).Elem()
	items := reflect.MakeSlice(rows.Type(), 1, 1)
	items.Index(0).FieldByName("ReceiverID").SetInt(2)
	items.Index(0).FieldByName("Amount").SetInt(100)
	items.Index(0).FieldByName("ReceivedAt").Set(reflect.ValueOf(time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)))
	rows.Set(items)
	return nil
}

type unusedDetailRPC struct{ userrpc.UserRpc }

func TestDetailChecksReaderBeforeListingClaims(t *testing.T) {
	conn := &detailRowsConn{}
	service := NewQueryService(conn, detailPacketModel{packet: &redpacketmodel.RedPacket{
		RedPacketId: 7, SenderId: 1, SessionType: SessionTypeSingle,
		ReceiverId: sql.NullInt64{Int64: 2, Valid: true},
	}}, unusedDetailRPC{})

	if _, err := service.Get(context.Background(), 7, 3); !errors.Is(err, ErrReceivePermission) {
		t.Fatalf("unauthorized detail error = %v", err)
	}
	if conn.queries != 0 {
		t.Fatalf("queried %d claim lists before permission check", conn.queries)
	}

	detail, err := service.Get(context.Background(), 7, 2)
	if err != nil {
		t.Fatal(err)
	}
	if conn.queries != 1 || len(detail.Receives) != 1 ||
		detail.Receives[0].ReceiverID != 2 || detail.Receives[0].Amount != 100 {
		t.Fatalf("authorized detail = %+v, claim queries = %d", detail, conn.queries)
	}
}
