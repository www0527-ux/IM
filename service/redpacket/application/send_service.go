package application

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"infinitechatgo/common/idgen"
	"infinitechatgo/common/messageidentity"
	messageevent "infinitechatgo/common/model/event"
	"infinitechatgo/common/txctx"
	messageapplication "infinitechatgo/service/message/application"
	balancelogmodel "infinitechatgo/service/redpacket/model/balance_log"
	redpacketmodel "infinitechatgo/service/redpacket/model/red_packet"
	sharemodel "infinitechatgo/service/redpacket/model/red_packet_share"
	balancemodel "infinitechatgo/service/redpacket/model/user_balance"
	"infinitechatgo/service/user/rpc/userrpc"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

var (
	ErrSendDependenciesMissing = errors.New("red packet send dependencies are missing")
	ErrSendPermissionDenied    = errors.New("red packet send permission denied")
	ErrSendInsufficientBalance = errors.New("insufficient balance for red packet")
	ErrSendIdempotencyConflict = errors.New("red packet send idempotency conflict")
	ErrSendInvariantViolation  = errors.New("red packet send invariant violation")
	ErrSendUnknownOutcome      = errors.New("red packet send result is unknown; retry with the same client message ID")
)

const (
	redPacketActiveStatus   int64 = 0
	redPacketSendLogType    int64 = 0
	redPacketShareUnclaimed int64 = 0
	redPacketSendCacheTTL         = 24 * 60 * 60
	defaultWrapperText            = "恭喜发财，大吉大利"
	permissionTimeout             = 3 * time.Second
)

type SendDependencies struct {
	SqlConn          sqlx.SqlConn
	Redis            *redis.Redis
	IDGenerator      *idgen.Generator
	UserRPC          userrpc.UserRpc
	RedPacketModel   redpacketmodel.RedPacketModel
	ShareModel       sharemodel.RedPacketShareModel
	UserBalanceModel balancemodel.UserBalanceModel
	BalanceLogModel  balancelogmodel.BalanceLogModel
	MessageWriter    messageapplication.PersistentWriter
	Publisher        MessagePublisher
}

type MessagePublisher interface {
	TryPublish(messageevent.ChatMessage, string) bool
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

func (s *SendService) Send(ctx context.Context, command SendCommand) (SendResult, error) {
	if s == nil || !s.dependenciesReady() {
		return SendResult{}, ErrSendDependenciesMissing
	}
	if err := ValidateSendCommand(command); err != nil {
		return SendResult{}, err
	}
	//为红包补充文案
	command = normalizeSendCommand(command)
	//redis幂等检查
	if cached, ok := s.readSendCache(ctx, command); ok {
		return cached, nil
	}

	existing, err := s.findExisting(ctx, command)
	switch {
	case err == nil:
		return existing, nil
	case errors.Is(err, redpacketmodel.ErrNotFound):
		// The unique key is still authoritative if another request inserts now.
	case errors.Is(err, ErrSendIdempotencyConflict):
		return SendResult{}, err
	case errors.Is(err, ErrSendInvariantViolation):
		return SendResult{}, err
	default:
		return SendResult{}, fmt.Errorf("%w: check send idempotency: %v", ErrSendUnknownOutcome, err)
	}
	if err := s.validatePermission(ctx, command); err != nil {
		return SendResult{}, err
	}

	amounts, err := AllocateShareAmounts(command.TotalAmount, command.TotalCount, command.RedPacketType)
	if err != nil {
		return SendResult{}, err
	}
	redPacketID, err := s.nextID()
	if err != nil {
		return SendResult{}, err
	}
	messageID, err := s.nextID()
	if err != nil {
		return SendResult{}, err
	}
	logID, err := s.nextID()
	if err != nil {
		return SendResult{}, err
	}

	now := time.Now()
	//ReceivedAmount等默认为0
	packet := &redpacketmodel.RedPacket{
		RedPacketId:     redPacketID,
		MessageId:       messageID,
		SenderId:        command.SenderID,
		ClientMessageId: command.ClientMessageID,
		SessionId:       command.SessionID,
		SessionType:     command.SessionType,
		RedPacketType:   command.RedPacketType,
		WrapperText:     command.WrapperText,
		TotalAmount:     command.TotalAmount,
		TotalCount:      command.TotalCount,
		Status:          redPacketActiveStatus,
		ExpireAt:        now.Add(24 * time.Hour),
	}
	if command.SessionType == SessionTypeSingle {
		packet.ReceiverId = sql.NullInt64{Int64: command.ReceiverID, Valid: true}
	}
	shares := make([]*sharemodel.RedPacketShare, 0, len(amounts))
	//循环生成share份额
	for index, amount := range amounts {
		shareID, err := s.nextID()
		if err != nil {
			return SendResult{}, err
		}
		shares = append(shares, &sharemodel.RedPacketShare{
			ShareId:     shareID,
			RedPacketId: redPacketID,
			Ordinal:     int64(index + 1),
			Amount:      amount,
			Status:      redPacketShareUnclaimed,
		})
	}
	message := messageevent.ChatMessage{
		MessageID:       messageID,
		ClientMessageID: command.ClientMessageID,
		SessionID:       command.SessionID,
		SessionType:     messageevent.SessionType(command.SessionType),
		SenderID:        command.SenderID,
		MessageType:     messageevent.MessageTypeRedPacket,
		Body: messageevent.MessageBody{
			Content:              command.WrapperText,
			RedPacketID:          &redPacketID,
			RedPacketWrapperText: command.WrapperText,
		},
		CreatedAt: now.UnixMilli(),
	}
	if command.SessionType == SessionTypeSingle {
		message.ReceiverID = &command.ReceiverID
	}
	fingerprint, err := messageidentity.Fingerprint(message)
	if err != nil {
		return SendResult{}, fmt.Errorf("fingerprint red packet message: %w", err)
	}
	var persisted messageapplication.Result

	err = txctx.WithTransaction(ctx, s.dependencies.SqlConn, func(txCtx context.Context) error {
		//扣款
		debited, err := s.dependencies.UserBalanceModel.DebitIfEnoughTx(txCtx, command.SenderID, command.TotalAmount)
		if err != nil {
			return fmt.Errorf("debit red packet balance: %w", err)
		}
		if !debited {
			return ErrSendInsufficientBalance
		}
		if _, err := s.dependencies.RedPacketModel.InsertTx(txCtx, packet); err != nil {
			return fmt.Errorf("insert red packet: %w", err)
		}
		if err := s.dependencies.ShareModel.InsertBatchTx(txCtx, shares); err != nil {
			return fmt.Errorf("insert red packet shares: %w", err)
		}
		if _, err := s.dependencies.BalanceLogModel.InsertTx(txCtx, &balancelogmodel.BalanceLog{
			BalanceLogId: logID,
			UserId:       command.SenderID,
			Amount:       -command.TotalAmount,
			Type:         redPacketSendLogType,
			RelatedId:    redPacketID,
		}); err != nil {
			return fmt.Errorf("insert red packet debit log: %w", err)
		}
		persisted, err = s.dependencies.MessageWriter.Persist(txCtx, messageapplication.Command{
			Source: messageapplication.SourceRedPacket, Message: message, Fingerprint: fingerprint,
		})
		if err != nil {
			return fmt.Errorf("persist red packet message: %w", err)
		}
		if !persisted.Created {
			return fmt.Errorf("%w: message already exists", ErrSendInvariantViolation)
		}
		return nil
	})

	//并发插入出错，回查具体情况
	if err != nil {
		// The transaction may have committed despite a lost COMMIT response.
		confirmed, lookupErr := s.findExisting(ctx, command)
		switch {
		case lookupErr == nil:
			return confirmed, nil
		case errors.Is(lookupErr, ErrSendIdempotencyConflict):
			return SendResult{}, lookupErr
		case errors.Is(lookupErr, ErrSendInvariantViolation):
			return SendResult{}, lookupErr
		case lookupErr != nil && !errors.Is(lookupErr, redpacketmodel.ErrNotFound):
			return SendResult{}, fmt.Errorf("%w: transaction: %v; confirm: %v", ErrSendUnknownOutcome, err, lookupErr)
		case isDuplicateKey(err):
			return SendResult{}, fmt.Errorf("%w: duplicate key without matching sender/client key: %v", ErrSendInvariantViolation, err)
		case errors.Is(err, ErrSendInsufficientBalance):
			return SendResult{}, ErrSendInsufficientBalance
		default:
			return SendResult{}, fmt.Errorf("%w: transaction: %v", ErrSendUnknownOutcome, err)
		}
	}
	if s.dependencies.Publisher != nil && !s.dependencies.Publisher.TryPublish(persisted.Message, fingerprint) {
		log.Printf("best-effort red packet message publish queue full messageId=%d", persisted.Message.MessageID)
	}
	result := SendResult{RedPacketID: redPacketID, MessageID: messageID, Status: redPacketActiveStatus}
	return result, nil
}

func (s *SendService) dependenciesReady() bool {
	d := s.dependencies
	return d.SqlConn != nil && d.IDGenerator != nil && d.UserRPC != nil &&
		d.RedPacketModel != nil && d.ShareModel != nil && d.UserBalanceModel != nil &&
		d.BalanceLogModel != nil && d.MessageWriter != nil
}

func normalizeSendCommand(command SendCommand) SendCommand {
	if command.WrapperText == "" {
		command.WrapperText = defaultWrapperText
	}
	return command
}

// RPC权限校验
func (s *SendService) validatePermission(ctx context.Context, command SendCommand) error {
	lookupCtx, cancel := context.WithTimeout(ctx, permissionTimeout)
	defer cancel()
	var (
		result *userrpc.MessageValidateResp
		err    error
	)
	if command.SessionType == SessionTypeSingle {
		result, err = s.dependencies.UserRPC.ValidateSingleMessage(lookupCtx, &userrpc.ValidateSingleMessageReq{
			SenderId: command.SenderID, ReceiverId: command.ReceiverID, SessionId: command.SessionID,
		})
	} else {
		result, err = s.dependencies.UserRPC.ValidateGroupMessage(lookupCtx, &userrpc.ValidateGroupMessageReq{
			SenderId: command.SenderID, SessionId: command.SessionID,
		})
	}
	if err != nil {
		return fmt.Errorf("validate red packet send permission: %w", err)
	}
	if result == nil {
		return fmt.Errorf("validate red packet send permission: nil RPC response")
	}
	if !result.Allowed {
		return fmt.Errorf("%w: %s", ErrSendPermissionDenied, result.RejectReason)
	}
	return nil
}

func (s *SendService) findExisting(ctx context.Context, command SendCommand) (SendResult, error) {
	stored, err := s.dependencies.RedPacketModel.FindOneBySenderIdClientMessageId(
		ctx, command.SenderID, command.ClientMessageID)
	if err != nil {
		return SendResult{}, err
	}

	if stored == nil {
		return SendResult{}, fmt.Errorf("%w: lookup returned nil red packet without error for sender=%d clientMessageID=%q",
			ErrSendInvariantViolation, command.SenderID, command.ClientMessageID)
	}
	//判断数据库中落库消息与发送消息是否相等
	if !sameSendCommand(command, stored) {
		return SendResult{}, fmt.Errorf("%w: sender=%d clientMessageID=%q", ErrSendIdempotencyConflict,
			command.SenderID, command.ClientMessageID)
	}
	return SendResult{RedPacketID: stored.RedPacketId, MessageID: stored.MessageId, Status: redPacketActiveStatus}, nil
}

func sameSendCommand(command SendCommand, stored *redpacketmodel.RedPacket) bool {
	if stored.SenderId != command.SenderID || stored.ClientMessageId != command.ClientMessageID ||
		stored.SessionId != command.SessionID || stored.SessionType != command.SessionType ||
		stored.RedPacketType != command.RedPacketType || stored.TotalAmount != command.TotalAmount ||
		stored.TotalCount != command.TotalCount || stored.WrapperText != command.WrapperText {
		return false
	}
	if command.SessionType == SessionTypeSingle {
		return stored.ReceiverId.Valid && stored.ReceiverId.Int64 == command.ReceiverID
	}
	return !stored.ReceiverId.Valid
}

func (s *SendService) nextID() (int64, error) {
	id, err := s.dependencies.IDGenerator.NextID()
	if err != nil {
		return 0, fmt.Errorf("generate red packet ID: %w", err)
	}
	return id, nil
}

type sendCacheEntry struct {
	Command SendCommand `json:"command"`
	Result  SendResult  `json:"result"` //请求相同时直接返回结果
}

// senderID+clientMessageID->sendResult
func sendCacheKey(command SendCommand) string {
	return "im:redpacket:send:v1:" + strconv.FormatInt(command.SenderID, 10) + ":" + command.ClientMessageID
}

// 校验是否重复发送，发送内容是否相同
func (s *SendService) readSendCache(ctx context.Context, command SendCommand) (SendResult, bool) {
	if s.dependencies.Redis == nil {
		return SendResult{}, false
	}
	value, err := s.dependencies.Redis.GetCtx(ctx, sendCacheKey(command))
	if err != nil || value == "" {
		return SendResult{}, false
	}
	var entry sendCacheEntry
	if json.Unmarshal([]byte(value), &entry) != nil || entry.Command != command ||
		entry.Result.RedPacketID <= 0 || entry.Result.MessageID <= 0 {
		return SendResult{}, false
	}
	return entry.Result, true
}

func isDuplicateKey(err error) bool {
	var mysqlErr *mysqlDriver.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1062
}
