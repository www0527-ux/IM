package application

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"infinitechatgo/common/idgen"
	"infinitechatgo/common/txctx"
	balancelogmodel "infinitechatgo/service/redpacket/model/balance_log"
	redpacketmodel "infinitechatgo/service/redpacket/model/red_packet"
	receivemodel "infinitechatgo/service/redpacket/model/red_packet_receive"
	"infinitechatgo/service/user/rpc/userrpc"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var (
	ErrReceiveNotFound       = errors.New("red packet not found")
	ErrReceivePermission     = errors.New("red packet claim permission denied")
	ErrReceiveExpired        = errors.New("red packet expired")
	ErrReceiveSoldOut        = errors.New("red packet has no available shares")
	ErrReceiveUnknownOutcome = errors.New("red packet claim outcome is unknown; retry the same packet and user")
	ErrReceiveInvariant      = errors.New("red packet claim invariant violation") //不变的约束
)

const (
	redPacketCompletedStatus int64 = 1
	redPacketExpiredStatus   int64 = 2
	redPacketShareClaimed    int64 = 1
	redPacketReceiveLogType  int64 = 1
)

type ReceiveDependencies struct {
	SqlConn         sqlx.SqlConn
	IDGenerator     *idgen.Generator
	UserRPC         userrpc.UserRpc
	CandidatePool   CandidatePool
	RedPacketModel  redpacketmodel.RedPacketModel
	ReceiveModel    receivemodel.RedPacketReceiveModel
	BalanceLogModel balancelogmodel.BalanceLogModel
}

type ReceiveService struct{ dependencies ReceiveDependencies }

type ReceiveResult struct {
	ReceiveID  int64
	ShareID    int64
	Amount     int64
	ReceivedAt time.Time
}

func NewReceiveService(dependencies ReceiveDependencies) *ReceiveService {
	return &ReceiveService{dependencies: dependencies}
}

func (s *ReceiveService) Receive(ctx context.Context, redPacketID, userID int64) (ReceiveResult, error) {
	if s == nil || s.dependencies.SqlConn == nil || s.dependencies.IDGenerator == nil ||
		s.dependencies.UserRPC == nil || s.dependencies.RedPacketModel == nil ||
		s.dependencies.ReceiveModel == nil || s.dependencies.BalanceLogModel == nil {
		return ReceiveResult{}, fmt.Errorf("red packet claim dependencies are missing")
	}
	if redPacketID <= 0 || userID <= 0 {
		return ReceiveResult{}, fmt.Errorf("red packet ID and user ID must be positive")
	}
	//红包记录快照，主要作用于rpc权限校验，快速错误返回
	packet, err := s.dependencies.RedPacketModel.FindOne(ctx, redPacketID)
	if errors.Is(err, redpacketmodel.ErrNotFound) {
		return ReceiveResult{}, ErrReceiveNotFound
	}
	if err != nil {
		return ReceiveResult{}, fmt.Errorf("load red packet before claim: %w", err)
	}
	//锁外查询，快速判断是否已经领取过了
	if existing, err := s.findReceive(ctx, redPacketID, userID); err == nil {
		return existing, nil
	} else if !errors.Is(err, receivemodel.ErrNotFound) {
		return ReceiveResult{}, fmt.Errorf("check previous claim: %w", err)
	}
	if err := s.validateClaimPermission(ctx, packet, userID); err != nil {
		return ReceiveResult{}, err
	}
	candidateID := int64(0)
	if s.dependencies.CandidatePool != nil {
		//从redis中获取候选ID
		candidateID, err = s.dependencies.CandidatePool.Pop(ctx, redPacketID)
		if err != nil {
			log.Printf("red packet candidate cache unavailable packet=%d: %v", redPacketID, err)
			candidateID = 0
		}
	}
	receiveID, err := s.dependencies.IDGenerator.NextID()
	if err != nil {
		return ReceiveResult{}, err
	}
	logID, err := s.dependencies.IDGenerator.NextID()
	if err != nil {
		return ReceiveResult{}, err
	}
	var result ReceiveResult
	//用于重构redis候选池的标记，true表示需要重构候选池，false表示不需要
	usedFallback := candidateID == 0
	//决定是否要启动删除候选池
	completed := false
	err = txctx.WithTransaction(ctx, s.dependencies.SqlConn, func(txCtx context.Context) error {
		conn, err := txctx.SessionConn(txCtx)
		if err != nil {
			return err
		}
		var locked struct {
			TotalAmount    int64     `db:"total_amount"`
			TotalCount     int64     `db:"total_count"`
			ReceivedAmount int64     `db:"received_amount"`
			ReceivedCount  int64     `db:"received_count"`
			Status         int64     `db:"status"`
			ExpireAt       time.Time `db:"expire_at"`
		}
		//获取红包详情，用于后续的更新与复查
		if err := conn.QueryRowCtx(txCtx, &locked, `SELECT total_amount, total_count,
			received_amount, received_count, status, expire_at
			FROM red_packet WHERE red_packet_id = ? FOR UPDATE`, redPacketID); err != nil {
			return fmt.Errorf("lock red packet: %w", err)
		}
		//锁内复查，确保没有重复领取
		var existing receivemodel.RedPacketReceive
		if err := conn.QueryRowCtx(txCtx, &existing, `SELECT red_packet_receive_id,
			share_id, amount, received_at FROM red_packet_receive
			WHERE red_packet_id = ? AND receiver_id = ? LIMIT 1`, redPacketID, userID); err == nil {
			result = resultFromReceive(&existing)
			return nil
		} else if !errors.Is(err, sqlx.ErrNotFound) {
			return fmt.Errorf("recheck claim after packet lock: %w", err)
		}
		//用于提前拒绝过期红包
		var clock struct {
			Now time.Time `db:"db_now"`
		}
		if err := conn.QueryRowCtx(txCtx, &clock, `SELECT NOW(3) AS db_now`); err != nil {
			return fmt.Errorf("read database time: %w", err)
		}
		dbNow := clock.Now
		//检查状态
		if locked.Status == redPacketCompletedStatus || locked.ReceivedCount >= locked.TotalCount {
			return ErrReceiveSoldOut
		}
		if locked.Status == redPacketExpiredStatus || !dbNow.Before(locked.ExpireAt) {
			return ErrReceiveExpired
		}
		if locked.Status != redPacketActiveStatus || locked.ReceivedAmount < 0 ||
			locked.ReceivedAmount >= locked.TotalAmount {
			return ErrReceiveInvariant
		}
		var share struct {
			ShareID int64 `db:"share_id"`
			Amount  int64 `db:"amount"`
		}
		//有候选ID时，尝试锁定候选红包份额
		if candidateID > 0 {
			err = conn.QueryRowCtx(txCtx, &share, `SELECT share_id, amount
				FROM red_packet_share WHERE red_packet_id = ? AND share_id = ? AND status = 0
				FOR UPDATE`, redPacketID, candidateID)
			if errors.Is(err, sqlx.ErrNotFound) {
				candidateID = 0
				usedFallback = true
			} else if err != nil {
				return fmt.Errorf("lock candidate share: %w", err)
			}
		}
		//无候选ID时，尝试锁定任意可用红包份额
		if candidateID == 0 {
			if err := conn.QueryRowCtx(txCtx, &share, `SELECT share_id, amount
				FROM red_packet_share WHERE red_packet_id = ? AND status = 0
				ORDER BY share_id LIMIT 1 FOR UPDATE SKIP LOCKED`, redPacketID); err != nil {
				if errors.Is(err, sqlx.ErrNotFound) {
					return ErrReceiveInvariant
				}
				return fmt.Errorf("find available share: %w", err)
			}
		}
		if share.Amount <= 0 || locked.ReceivedAmount+share.Amount > locked.TotalAmount {
			return ErrReceiveInvariant
		}
		update, err := conn.ExecCtx(txCtx, `UPDATE red_packet_share
			SET status = ?, receiver_id = ?, received_at = ?
			WHERE share_id = ? AND red_packet_id = ? AND status = 0`,
			redPacketShareClaimed, userID, dbNow, share.ShareID, redPacketID)
		if err != nil {
			return fmt.Errorf("claim share: %w", err)
		}
		if affected, err := update.RowsAffected(); err != nil || affected != 1 {
			return fmt.Errorf("%w: share update affected %d rows: %v", ErrReceiveInvariant, affected, err)
		}
		if _, err := conn.ExecCtx(txCtx, `INSERT INTO red_packet_receive
			(red_packet_receive_id, red_packet_id, share_id, receiver_id, amount, received_at)
			VALUES (?, ?, ?, ?, ?, ?)`, receiveID, redPacketID, share.ShareID,
			userID, share.Amount, dbNow); err != nil {
			return fmt.Errorf("insert claim record: %w", err)
		}
		credit, err := conn.ExecCtx(txCtx, `UPDATE user_balance SET balance = balance + ? WHERE user_id = ?`,
			share.Amount, userID)
		if err != nil {
			return fmt.Errorf("credit claimant: %w", err)
		}

		//统计前一条sql影响的行数
		if affected, err := credit.RowsAffected(); err != nil || affected != 1 {
			return fmt.Errorf("%w: claimant balance affected %d rows: %v", ErrReceiveInvariant, affected, err)
		}
		if _, err := s.dependencies.BalanceLogModel.InsertTx(txCtx, &balancelogmodel.BalanceLog{
			BalanceLogId: logID, UserId: userID, Amount: share.Amount,
			Type: redPacketReceiveLogType, RelatedId: redPacketID,
		}); err != nil {
			return fmt.Errorf("insert claim balance log: %w", err)
		}
		nextCount := locked.ReceivedCount + 1
		status := redPacketActiveStatus
		if nextCount == locked.TotalCount {
			status = redPacketCompletedStatus
			if locked.ReceivedAmount+share.Amount != locked.TotalAmount {
				return ErrReceiveInvariant
			}
		}
		//更新红包信息（附带日期检查）
		update, err = conn.ExecCtx(txCtx, `UPDATE red_packet
			SET received_amount = received_amount + ?, received_count = received_count + 1,
			status = ?, version = version + 1
			WHERE red_packet_id = ? AND status = 0 AND expire_at > NOW(3)`,
			share.Amount, status, redPacketID)
		if err != nil {
			return fmt.Errorf("update claim totals: %w", err)
		}
		if affected, err := update.RowsAffected(); err != nil || affected != 1 {
			return fmt.Errorf("%w: claim totals affected %d rows: %v", ErrReceiveExpired, affected, err)
		}
		completed = status == redPacketCompletedStatus
		result = ReceiveResult{ReceiveID: receiveID, ShareID: share.ShareID,
			Amount: share.Amount, ReceivedAt: dbNow}
		return nil
	})
	if err != nil {
		if existing, lookupErr := s.findReceive(ctx, redPacketID, userID); lookupErr == nil {
			return existing, nil
		} else if !errors.Is(lookupErr, receivemodel.ErrNotFound) {
			return ReceiveResult{}, fmt.Errorf("%w: transaction: %v; confirm: %v",
				ErrReceiveUnknownOutcome, err, lookupErr)
		}
		if errors.Is(err, ErrReceiveExpired) || errors.Is(err, ErrReceiveSoldOut) ||
			errors.Is(err, ErrReceiveInvariant) {
			return ReceiveResult{}, err
		}
		//插入时可能出现唯一键冲突
		if isDuplicateKey(err) {
			return ReceiveResult{}, fmt.Errorf("%w: duplicate key without matching user claim: %v",
				ErrReceiveInvariant, err)
		}
		return ReceiveResult{}, fmt.Errorf("%w: %v", ErrReceiveUnknownOutcome, err)
	}
	if s.dependencies.CandidatePool != nil {
		if completed {
			if err := s.dependencies.CandidatePool.Remove(ctx, redPacketID); err != nil {
				log.Printf("remove completed candidate pool packet=%d: %v", redPacketID, err)
			}
		} else if usedFallback {
			if err := s.dependencies.CandidatePool.Rebuild(ctx, redPacketID); err != nil {
				log.Printf("rebuild candidate pool after MySQL claim packet=%d: %v", redPacketID, err)
			}
		}
	}
	return result, nil
}

func (s *ReceiveService) findReceive(ctx context.Context, packetID, userID int64) (ReceiveResult, error) {
	record, err := s.dependencies.ReceiveModel.FindOneByRedPacketIdReceiverId(ctx, packetID, userID)
	if err != nil {
		return ReceiveResult{}, err
	}
	return resultFromReceive(record), nil
}

func resultFromReceive(record *receivemodel.RedPacketReceive) ReceiveResult {
	return ReceiveResult{ReceiveID: record.RedPacketReceiveId, ShareID: record.ShareId,
		Amount: record.Amount, ReceivedAt: record.ReceivedAt}
}

func (s *ReceiveService) validateClaimPermission(ctx context.Context,
	packet *redpacketmodel.RedPacket, userID int64) error {
	lookupCtx, cancel := context.WithTimeout(ctx, permissionTimeout)
	defer cancel()
	var (
		response *userrpc.MessageValidateResp
		err      error
	)
	if packet.SessionType == SessionTypeSingle {
		if !packet.ReceiverId.Valid || packet.ReceiverId.Int64 != userID || userID == packet.SenderId {
			return ErrReceivePermission
		}
		response, err = s.dependencies.UserRPC.ValidateSingleMessage(lookupCtx,
			&userrpc.ValidateSingleMessageReq{SenderId: userID,
				ReceiverId: packet.SenderId, SessionId: packet.SessionId})
	} else if packet.SessionType == SessionTypeGroup {
		response, err = s.dependencies.UserRPC.ValidateGroupMessage(lookupCtx,
			&userrpc.ValidateGroupMessageReq{SenderId: userID, SessionId: packet.SessionId})
	} else {
		return ErrReceiveInvariant
	}
	if err != nil {
		return fmt.Errorf("check claim membership: %w", err)
	}
	if response == nil {
		return fmt.Errorf("check claim membership: nil RPC response")
	}
	if !response.Allowed {
		return ErrReceivePermission
	}
	return nil
}
