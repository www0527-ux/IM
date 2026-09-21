package application

import (
	"context"
	"errors"

	"infinitechatgo/common/idgen"
	balancelogmodel "infinitechatgo/service/redpacket/model/balance_log"
	outboxmodel "infinitechatgo/service/redpacket/model/outbox_event"
	redpacketmodel "infinitechatgo/service/redpacket/model/red_packet"
	sharemodel "infinitechatgo/service/redpacket/model/red_packet_share"
	balancemodel "infinitechatgo/service/redpacket/model/user_balance"
	"infinitechatgo/service/user/rpc/userrpc"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var (
	ErrSendCoreNotImplemented = errors.New("send red packet core transaction is not implemented")
	ErrStageNotImplemented    = errors.New("red packet stage is not implemented")
)

type OutboxTopics struct {
	MessageCreateRequested string
	CacheBuild             string
}

type SendDependencies struct {
	SqlConn          sqlx.SqlConn
	Redis            *redis.Redis
	IDGenerator      *idgen.Generator
	UserRPC          userrpc.UserRpc
	RedPacketModel   redpacketmodel.RedPacketModel
	ShareModel       sharemodel.RedPacketShareModel
	UserBalanceModel balancemodel.UserBalanceModel
	BalanceLogModel  balancelogmodel.BalanceLogModel
	OutboxEventModel outboxmodel.OutboxEventModel
	Topics           OutboxTopics
}

type SendService struct {
	dependencies SendDependencies
}

type SendCommand struct {
	SenderID        int64
	ClientMessageID string
	SessionID       int64
	SessionType     int64
	ReceiverID      int64
	RedPacketType   int64
	WrapperText     string
	TotalAmount     int64
	TotalCount      int64
}

type SendResult struct {
	RedPacketID int64
	MessageID   int64
	Status      int64
}

func NewSendService(dependencies SendDependencies) *SendService {
	return &SendService{dependencies: dependencies}
}

// Send owns steps S2-S8 in redpacket-design.md.
//
// Preconditions:
//   - SenderID came from authenticated server context.
//   - Permission RPC finishes before any balance write.
//   - MySQL idempotency lookup decides whether creation may continue.
//
// Postconditions on success:
//   - debit, packet, every share, debit log and two independent Outbox rows
//     commit in one local transaction;
//   - this service does not insert a message row;
//   - a retry with the same sender/client-message key returns the original IDs.
//
// Failure semantics:
//   - any pre-commit failure leaves no new financial fact;
//   - an unknown commit result is reconciled by sender/request key;
//   - Redis cache writes happen only after commit and are best effort.
func (s *SendService) Send(
	context.Context,
	SendCommand,
) (SendResult, error) {
	// TODO(user): implement share allocation, permission error classification,
	// persistent idempotency and the S8 local transaction. Keep Kafka out of
	// this method; it only inserts MESSAGE_CREATE_REQUESTED and
	// REDPACKET_CACHE_BUILD Outbox rows.
	return SendResult{}, ErrSendCoreNotImplemented
}
