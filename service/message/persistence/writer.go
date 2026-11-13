package persistence

import (
	"context"
	"errors"
	"fmt"
	"time"

	"infinitechatgo/common/idgen"
	messageevent "infinitechatgo/common/model/event"
	"infinitechatgo/common/txctx"
	messageapplication "infinitechatgo/service/message/application"
	sequencemodel "infinitechatgo/service/message/model/conversation_sequence"
	messagemodel "infinitechatgo/service/message/model/message"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type Dependencies struct {
	SqlConn     sqlx.SqlConn
	IDGenerator *idgen.Generator
	Messages    messagemodel.Model
	Sequences   sequencemodel.Model
}

type Writer struct {
	dependencies Dependencies
}

func NewWriter(dependencies Dependencies) (*Writer, error) {
	if dependencies.SqlConn == nil || dependencies.IDGenerator == nil ||
		dependencies.Messages == nil || dependencies.Sequences == nil {
		return nil, fmt.Errorf("invalid message persistence dependencies")
	}
	return &Writer{dependencies: dependencies}, nil
}

// errDuplicateRow 只用来触发事务回滚，不会传到调用方。
var errDuplicateRow = errors.New("message unique key collision")

func (w *Writer) Persist(
	ctx context.Context,
	command messageapplication.Command,
) (messageapplication.Result, error) {
	var result messageapplication.Result
	err := txctx.WithTransaction(
		ctx,
		w.dependencies.SqlConn,
		func(txCtx context.Context) error {
			persisted, persistErr := w.persistInTransaction(txCtx, command)
			if persistErr != nil {
				return persistErr
			}
			result = persisted
			return nil
		},
	)
	switch {
	case err == nil:
		return result, nil
	case errors.Is(err, errDuplicateRow):
		if txctx.GetSession(ctx) != nil {
			return messageapplication.Result{}, err
		}
		return w.reloadPersisted(ctx, command)
	default:
		return messageapplication.Result{}, err
	}
}

func (w *Writer) persistInTransaction(
	ctx context.Context,
	command messageapplication.Command,
) (messageapplication.Result, error) {
	message := command.Message
	//普通查询
	existing, err := w.dependencies.Messages.FindByIdempotencyTx(
		ctx,
		message.SenderID,
		message.ClientMessageID,
	)
	switch {
	case err == nil:
		//幂等键行存在
		if !samePersistedMessage(command, existing) {
			return messageapplication.Result{}, conflictError(command)
		}
		// 幂等返回已有行
		return messageapplication.Result{Message: existing, Created: false}, nil
	case errors.Is(err, messagemodel.ErrNotFound):
		//不存在则可以进入后者开始创建
	default:
		return messageapplication.Result{}, fmt.Errorf(
			"check message idempotency: %w",
			err,
		)
	}
	//判断计数器行是否已经存在
	if err := w.dependencies.Sequences.EnsureTx(ctx, message.SessionID); err != nil {
		return messageapplication.Result{}, err
	}
	seq, err := w.dependencies.Sequences.LockAndIncrementTx(ctx, message.SessionID)
	if err != nil {
		return messageapplication.Result{}, err
	}
	//对于普通消息则创建MessageID
	if command.Source == messageapplication.SourceRealtime {
		generated, err := w.dependencies.IDGenerator.NextID()
		if err != nil {
			return messageapplication.Result{}, fmt.Errorf(
				"generate message id: %w",
				err,
			)
		}
		message.MessageID = generated
	}
	if message.MessageID <= 0 {
		return messageapplication.Result{}, fmt.Errorf(
			"%w: source %q did not carry a usable message id",
			messageapplication.ErrInvalidCommand,
			command.Source,
		)
	}
	if message.CreatedAt <= 0 {
		message.CreatedAt = time.Now().UnixMilli()
	}
	message.Seq = seq
	//唯一键冲突插入依旧会返回error，后续会进行复验转换
	if err := w.dependencies.Messages.InsertTx(ctx, message); err != nil {
		if messagemodel.IsDuplicateKeyError(err) {
			return messageapplication.Result{}, fmt.Errorf(
				"%w: %v",
				errDuplicateRow,
				err,
			)
		}
		return messageapplication.Result{}, err
	}
	return messageapplication.Result{Message: message, Created: true}, nil
}

func conflictError(command messageapplication.Command) error {
	return fmt.Errorf(
		"%w: sender_id=%d client_message_id=%q",
		messageapplication.ErrIdempotencyConflict,
		command.Message.SenderID,
		command.Message.ClientMessageID,
	)
}

func samePersistedMessage(
	command messageapplication.Command,
	stored messageevent.ChatMessage,
) bool {
	message := command.Message
	if command.Source == messageapplication.SourceRedPacket &&
		stored.MessageID != message.MessageID {
		return false
	}

	return stored.ClientMessageID == message.ClientMessageID &&
		stored.SenderID == message.SenderID &&
		stored.SessionID == message.SessionID &&
		stored.SessionType == message.SessionType &&
		stored.MessageType == message.MessageType &&
		stored.Body.Content == message.Body.Content &&
		sameOptionalID(stored.Body.RedPacketID, message.Body.RedPacketID) &&
		sameOptionalID(stored.Body.ReplyID, message.Body.ReplyID)
}

// 比较RedPacketID 和 ReplyID
func sameOptionalID(left, right *int64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func (w *Writer) reloadPersisted(
	ctx context.Context,
	command messageapplication.Command,
) (messageapplication.Result, error) {
	var result messageapplication.Result

	err := txctx.WithTransaction(
		ctx,
		w.dependencies.SqlConn,
		func(txCtx context.Context) error {
			existing, err := w.dependencies.Messages.FindByIdempotencyTx(
				txCtx,
				command.Message.SenderID,
				command.Message.ClientMessageID,
			)
			if errors.Is(err, messagemodel.ErrNotFound) {
				// 按幂等键查不到，说明这次撞的不是 uk_sender_client_message，
				// 那就不是重试，而是 uk_session_seq 或 uk_red_packet_id 出了问题。
				return fmt.Errorf("%w: sender_id=%d client_message_id=%q",
					messageapplication.ErrUniqueConstraintViolation,
					command.Message.SenderID, command.Message.ClientMessageID)
			}
			if err != nil {
				return fmt.Errorf("reload message after collision: %w", err)
			}
			if !samePersistedMessage(command, existing) {
				return conflictError(command)
			}
			result = messageapplication.Result{Message: existing, Created: false}
			return nil
		},
	)
	if err != nil {
		return messageapplication.Result{}, err
	}
	return result, nil
}
