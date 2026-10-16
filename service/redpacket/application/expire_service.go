package application

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"infinitechatgo/common/idgen"
	"infinitechatgo/common/txctx"
	balancelogmodel "infinitechatgo/service/redpacket/model/balance_log"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var ErrExpireInvariant = errors.New("red packet refund invariant violation")

const redPacketRefundLogType int64 = 2

type ExpireDependencies struct {
	SqlConn         sqlx.SqlConn
	IDGenerator     *idgen.Generator
	CandidatePool   CandidatePool
	BalanceLogModel balancelogmodel.BalanceLogModel
}

type ExpireService struct {
	dependencies ExpireDependencies
	batchMu      sync.Mutex
	cursorTime   time.Time
	cursorID     int64
}

func NewExpireService(dependencies ExpireDependencies) *ExpireService {
	return &ExpireService{dependencies: dependencies}
}

// RunBatch scans MySQL as the durable expiry schedule. Concurrent schedulers
// may scan the same ID; the red_packet row lock and status recheck settle it.
func (s *ExpireService) RunBatch(ctx context.Context, limit int) (int, error) {
	if s == nil || s.dependencies.SqlConn == nil || s.dependencies.IDGenerator == nil ||
		s.dependencies.BalanceLogModel == nil || limit <= 0 || limit > 1000 {
		return 0, fmt.Errorf("red packet expiry dependencies or batch size are invalid")
	}
	s.batchMu.Lock()
	defer s.batchMu.Unlock()
	var rows []struct {
		RedPacketID int64     `db:"red_packet_id"`
		ExpireAt    time.Time `db:"expire_at"`
	}
	query := `SELECT red_packet_id, expire_at FROM red_packet
		WHERE status = 0 AND expire_at <= NOW(3)
		ORDER BY expire_at, red_packet_id LIMIT ?`
	args := []any{limit}
	if !s.cursorTime.IsZero() {
		query = `SELECT red_packet_id, expire_at FROM red_packet
			WHERE status = 0 AND expire_at <= NOW(3)
			AND (expire_at > ? OR (expire_at = ? AND red_packet_id > ?))
			ORDER BY expire_at, red_packet_id LIMIT ?`
		args = []any{s.cursorTime, s.cursorTime, s.cursorID, limit}
	}
	if err := s.dependencies.SqlConn.QueryRowsCtx(ctx, &rows, query, args...); err != nil {
		return 0, fmt.Errorf("scan expired red packets: %w", err)
	}
	if len(rows) == 0 {
		s.cursorTime = time.Time{}
		s.cursorID = 0
		return 0, nil
	}
	last := rows[len(rows)-1]
	s.cursorTime, s.cursorID = last.ExpireAt, last.RedPacketID
	processed := 0
	var batchErrors error
	for _, row := range rows {
		changed, err := s.ExpireOne(ctx, row.RedPacketID)
		if err != nil {
			batchErrors = errors.Join(batchErrors, err)
			continue
		}
		if changed {
			processed++
		}
	}
	return processed, batchErrors
}

func (s *ExpireService) ExpireOne(ctx context.Context, redPacketID int64) (bool, error) {
	if s == nil || s.dependencies.SqlConn == nil || s.dependencies.IDGenerator == nil ||
		s.dependencies.BalanceLogModel == nil || redPacketID <= 0 {
		return false, fmt.Errorf("red packet expiry dependencies or ID are invalid")
	}
	changed := false
	err := txctx.WithTransaction(ctx, s.dependencies.SqlConn, func(txCtx context.Context) error {
		conn, err := txctx.SessionConn(txCtx)
		if err != nil {
			return err
		}
		var packet struct {
			SenderID       int64     `db:"sender_id"`
			TotalAmount    int64     `db:"total_amount"`
			TotalCount     int64     `db:"total_count"`
			ReceivedAmount int64     `db:"received_amount"`
			ReceivedCount  int64     `db:"received_count"`
			Status         int64     `db:"status"`
			ExpireAt       time.Time `db:"expire_at"`
		}
		if err := conn.QueryRowCtx(txCtx, &packet, `SELECT sender_id,
			total_amount, total_count, received_amount, received_count, status, expire_at
			FROM red_packet WHERE red_packet_id = ? FOR UPDATE`, redPacketID); err != nil {
			return fmt.Errorf("lock red packet for refund: %w", err)
		}
		var clock struct {
			Now time.Time `db:"db_now"`
		}
		if err := conn.QueryRowCtx(txCtx, &clock, `SELECT NOW(3) AS db_now`); err != nil {
			return fmt.Errorf("read database time for refund: %w", err)
		}
		dbNow := clock.Now
		if packet.Status != redPacketActiveStatus || dbNow.Before(packet.ExpireAt) {
			return nil
		}
		remaining := packet.TotalAmount - packet.ReceivedAmount
		if packet.TotalAmount <= 0 || packet.TotalCount <= 0 || remaining <= 0 ||
			packet.ReceivedAmount < 0 || packet.ReceivedCount < 0 ||
			packet.ReceivedCount >= packet.TotalCount {
			return ErrExpireInvariant
		}
		credit, err := conn.ExecCtx(txCtx,
			`UPDATE user_balance SET balance = balance + ? WHERE user_id = ?`,
			remaining, packet.SenderID)
		if err != nil {
			return fmt.Errorf("refund red packet sender: %w", err)
		}
		if affected, err := credit.RowsAffected(); err != nil || affected != 1 {
			return fmt.Errorf("%w: refund balance affected %d rows: %v", ErrExpireInvariant, affected, err)
		}
		logID, err := s.dependencies.IDGenerator.NextID()
		if err != nil {
			return err
		}
		if _, err := s.dependencies.BalanceLogModel.InsertTx(txCtx, &balancelogmodel.BalanceLog{
			BalanceLogId: logID, UserId: packet.SenderID, Amount: remaining,
			Type: redPacketRefundLogType, RelatedId: redPacketID,
		}); err != nil {
			return fmt.Errorf("insert unique refund balance log: %w", err)
		}
		updated, err := conn.ExecCtx(txCtx, `UPDATE red_packet
			SET status = ?, version = version + 1 WHERE red_packet_id = ? AND status = 0`,
			redPacketExpiredStatus, redPacketID)
		if err != nil {
			return fmt.Errorf("mark red packet expired: %w", err)
		}
		if affected, err := updated.RowsAffected(); err != nil || affected != 1 {
			return fmt.Errorf("%w: expiry update affected %d rows: %v", ErrExpireInvariant, affected, err)
		}
		changed = true
		return nil
	})
	if err != nil {
		// COMMIT may have succeeded even when its response was lost. The packet
		// status and unique refund log together confirm the durable outcome.
		confirmed, confirmErr := s.refundCommitted(ctx, redPacketID)
		if confirmErr == nil && confirmed {
			changed = true
		} else if confirmErr != nil {
			return false, fmt.Errorf("expire red packet %d: %v; confirm refund: %w",
				redPacketID, err, confirmErr)
		} else if isDuplicateKey(err) {
			return false, fmt.Errorf("%w: refund unique key conflict: %v", ErrExpireInvariant, err)
		} else {
			return false, fmt.Errorf("expire red packet %d: %w", redPacketID, err)
		}
	}
	if changed && s.dependencies.CandidatePool != nil {
		if err := s.dependencies.CandidatePool.Remove(ctx, redPacketID); err != nil {
			log.Printf("remove expired red packet candidates packet=%d: %v", redPacketID, err)
		}
	}
	return changed, nil
}

func (s *ExpireService) refundCommitted(ctx context.Context, redPacketID int64) (bool, error) {
	var fact struct {
		Status         int64 `db:"status"`
		TotalAmount    int64 `db:"total_amount"`
		ReceivedAmount int64 `db:"received_amount"`
		RefundCount    int64 `db:"refund_count"`
		RefundAmount   int64 `db:"refund_amount"`
	}
	err := s.dependencies.SqlConn.QueryRowCtx(ctx, &fact, `SELECT p.status,
		p.total_amount, p.received_amount,
		(SELECT COUNT(1) FROM balance_log b WHERE b.related_id = p.red_packet_id
			AND b.user_id = p.sender_id AND b.type = ?) AS refund_count,
		(SELECT COALESCE(SUM(b.amount), 0) FROM balance_log b
			WHERE b.related_id = p.red_packet_id AND b.user_id = p.sender_id
			AND b.type = ?) AS refund_amount
		FROM red_packet p WHERE p.red_packet_id = ?`,
		redPacketRefundLogType, redPacketRefundLogType, redPacketID)
	if err != nil {
		return false, err
	}
	return fact.Status == redPacketExpiredStatus && fact.RefundCount == 1 &&
		fact.RefundAmount == fact.TotalAmount-fact.ReceivedAmount, nil
}
