package application

import (
	"context"
	"errors"
	"fmt"

	"infinitechatgo/common/txctx"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

// AuditPacket reads one consistent MySQL snapshot and checks the maintained
// totals against share ownership, receive records and balance logs.
type PacketAudit struct {
	RedPacketID         int64    `json:"redPacketId"`
	TotalAmount         int64    `json:"totalAmount"`
	ReceivedAmount      int64    `json:"receivedAmount"`
	ReceivedCount       int64    `json:"receivedCount"`
	ClaimedShareAmount  int64    `json:"claimedShareAmount"`
	ClaimedShareCount   int64    `json:"claimedShareCount"`
	ReceiveRecordAmount int64    `json:"receiveRecordAmount"`
	ReceiveRecordCount  int64    `json:"receiveRecordCount"`
	ReceiveLogAmount    int64    `json:"receiveLogAmount"`
	ReceiveLogCount     int64    `json:"receiveLogCount"`
	SendLogAmount       int64    `json:"sendLogAmount"`
	SendLogCount        int64    `json:"sendLogCount"`
	RefundAmount        int64    `json:"refundAmount"`
	RefundLogCount      int64    `json:"refundLogCount"`
	Problems            []string `json:"problems"`
}

func AuditPacket(ctx context.Context, db sqlx.SqlConn, redPacketID int64) (PacketAudit, error) {
	if db == nil || redPacketID <= 0 {
		return PacketAudit{}, fmt.Errorf("audit requires database and packet ID")
	}
	audit := PacketAudit{RedPacketID: redPacketID, Problems: []string{}}
	err := txctx.WithTransaction(ctx, db, func(txCtx context.Context) error {
		conn, err := txctx.SessionConn(txCtx)
		if err != nil {
			return err
		}
		var packet struct {
			SenderID       int64 `db:"sender_id"`
			TotalAmount    int64 `db:"total_amount"`
			ReceivedAmount int64 `db:"received_amount"`
			ReceivedCount  int64 `db:"received_count"`
			Status         int64 `db:"status"`
		}
		if err := conn.QueryRowCtx(txCtx, &packet, `SELECT sender_id, total_amount,
			received_amount, received_count, status FROM red_packet WHERE red_packet_id = ?`,
			redPacketID); err != nil {
			if errors.Is(err, sqlx.ErrNotFound) {
				return ErrReceiveNotFound
			}
			return err
		}
		audit.TotalAmount, audit.ReceivedAmount, audit.ReceivedCount =
			packet.TotalAmount, packet.ReceivedAmount, packet.ReceivedCount
		var shares struct {
			Count  int64 `db:"count"`
			Amount int64 `db:"amount"`
		}
		if err := conn.QueryRowCtx(txCtx, &shares, `SELECT COUNT(1) AS count,
			COALESCE(SUM(amount),0) AS amount FROM red_packet_share
			WHERE red_packet_id = ? AND status = 1`, redPacketID); err != nil {
			return err
		}
		audit.ClaimedShareCount, audit.ClaimedShareAmount = shares.Count, shares.Amount
		var receives struct {
			Count  int64 `db:"count"`
			Amount int64 `db:"amount"`
		}
		if err := conn.QueryRowCtx(txCtx, &receives, `SELECT COUNT(1) AS count,
			COALESCE(SUM(amount),0) AS amount FROM red_packet_receive
			WHERE red_packet_id = ?`, redPacketID); err != nil {
			return err
		}
		audit.ReceiveRecordCount, audit.ReceiveRecordAmount = receives.Count, receives.Amount
		var receiveLogs struct {
			Count  int64 `db:"count"`
			Amount int64 `db:"amount"`
		}
		if err := conn.QueryRowCtx(txCtx, &receiveLogs,
			`SELECT COUNT(1) AS count, COALESCE(SUM(amount),0) AS amount
			 FROM balance_log WHERE related_id = ? AND type = 1`, redPacketID); err != nil {
			return err
		}
		audit.ReceiveLogCount, audit.ReceiveLogAmount = receiveLogs.Count, receiveLogs.Amount
		var sendLogs struct {
			Count  int64 `db:"count"`
			Amount int64 `db:"amount"`
		}
		if err := conn.QueryRowCtx(txCtx, &sendLogs,
			`SELECT COUNT(1) AS count, COALESCE(SUM(amount),0) AS amount
			 FROM balance_log WHERE related_id = ? AND type = 0 AND user_id = ?`,
			redPacketID, packet.SenderID); err != nil {
			return err
		}
		audit.SendLogCount, audit.SendLogAmount = sendLogs.Count, sendLogs.Amount
		var refundLogs struct {
			Count  int64 `db:"count"`
			Amount int64 `db:"amount"`
		}
		if err := conn.QueryRowCtx(txCtx, &refundLogs,
			`SELECT COUNT(1) AS count, COALESCE(SUM(amount),0) AS amount FROM balance_log
			WHERE related_id = ? AND type = 2 AND user_id = ?`,
			redPacketID, packet.SenderID); err != nil {
			return err
		}
		audit.RefundLogCount, audit.RefundAmount = refundLogs.Count, refundLogs.Amount
		if packet.ReceivedAmount != shares.Amount || packet.ReceivedAmount != receives.Amount ||
			packet.ReceivedAmount != audit.ReceiveLogAmount {
			audit.Problems = append(audit.Problems, "received amount differs from share, receive or balance logs")
		}
		if packet.ReceivedCount != shares.Count || packet.ReceivedCount != receives.Count ||
			packet.ReceivedCount != audit.ReceiveLogCount {
			audit.Problems = append(audit.Problems, "received count differs from share or receive rows")
		}
		if audit.SendLogCount != 1 || audit.SendLogAmount != -packet.TotalAmount {
			audit.Problems = append(audit.Problems, "send debit log differs from packet total")
		}
		if packet.ReceivedAmount < 0 || packet.ReceivedAmount > packet.TotalAmount {
			audit.Problems = append(audit.Problems, "received amount is outside total amount")
		}
		if packet.Status == redPacketCompletedStatus && packet.ReceivedAmount != packet.TotalAmount {
			audit.Problems = append(audit.Problems, "completed packet is not fully claimed")
		}
		if packet.Status == redPacketExpiredStatus &&
			packet.ReceivedAmount+audit.RefundAmount != packet.TotalAmount {
			audit.Problems = append(audit.Problems, "claimed plus refunded amount differs from total")
		}
		if packet.Status == redPacketExpiredStatus && audit.RefundLogCount != 1 {
			audit.Problems = append(audit.Problems, "expired packet must have one refund log")
		}
		if packet.Status != redPacketExpiredStatus && audit.RefundAmount != 0 {
			audit.Problems = append(audit.Problems, "non-expired packet has a refund log")
		}
		return nil
	})
	return audit, err
}
