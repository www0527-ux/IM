package persistence

import (
	"context"
	"fmt"
	"sort"
	"time"

	messageevent "infinitechatgo/common/model/event"
	"infinitechatgo/common/txctx"
	"infinitechatgo/service/message/application"
	messagemodel "infinitechatgo/service/message/model/message"
)

// PersistBatch owns one transaction for ordinary messages. Conversation rows
// are allocated in ascending ID order; external funds transactions use Persist.
func (w *Writer) PersistBatch(ctx context.Context, commands []application.Command) ([]application.Result, error) {
	if len(commands) == 0 || len(commands) > application.MaxBatchSize || txctx.GetSession(ctx) != nil {
		return nil, fmt.Errorf("%w: invalid batch size or external transaction", application.ErrInvalidCommand)
	}
	keys := make([]messagemodel.IdempotencyKey, 0, len(commands))
	first := make(map[messagemodel.IdempotencyKey]int, len(commands))
	owners := make([]int, len(commands))
	for i, command := range commands {
		if err := application.ValidateCommand(command); err != nil {
			return nil, err
		}
		if command.Source != application.SourceRealtime {
			return nil, fmt.Errorf("%w: batch must contain only ordinary realtime messages", application.ErrInvalidCommand)
		}
		key := messagemodel.IdempotencyKey{SenderID: command.Message.SenderID, ClientMessageID: command.Message.ClientMessageID}
		if index, found := first[key]; found {
			if command.Fingerprint != commands[index].Fingerprint || !samePersistedMessage(command, commands[index].Message) {
				return nil, fmt.Errorf("%w: %w", application.ErrBatchNeedsSplit, conflictError(command))
			}
			owners[i] = index
			continue
		}
		first[key] = i
		owners[i] = i
		keys = append(keys, key)
	}

	results := make([]application.Result, len(commands))
	err := txctx.WithTransaction(ctx, w.dependencies.SqlConn, func(txCtx context.Context) error {
		existing, err := w.dependencies.Messages.FindByIdempotencyBatchTx(txCtx, keys)
		if err != nil {
			return err
		}
		pending := make([]int, 0, len(keys))
		for i, command := range commands {
			if owners[i] != i {
				continue
			}
			key := messagemodel.IdempotencyKey{SenderID: command.Message.SenderID, ClientMessageID: command.Message.ClientMessageID}
			if stored, found := existing[key]; found {
				if !samePersistedMessage(command, stored) {
					return fmt.Errorf("%w: %w", application.ErrBatchNeedsSplit, conflictError(command))
				}
				results[i] = application.Result{Message: stored, Created: false}
			} else {
				pending = append(pending, i)
			}
		}
		if len(pending) > 0 {
			counts := make(map[int64]int64)
			for _, index := range pending {
				counts[commands[index].Message.SessionID]++
			}
			sessions := make([]int64, 0, len(counts))
			for id := range counts {
				sessions = append(sessions, id)
			}
			sort.Slice(sessions, func(i, j int) bool { return sessions[i] < sessions[j] })
			nextSeq := make(map[int64]int64, len(sessions))
			if len(sessions) > 1 {
				lastSeqs, err := w.dependencies.Sequences.LockAndAddManyTx(txCtx, counts)
				if err != nil {
					return err
				}
				for id, last := range lastSeqs {
					nextSeq[id] = last - counts[id]
				}
			} else {
				for _, id := range sessions {
					if err := w.dependencies.Sequences.EnsureTx(txCtx, id); err != nil {
						return err
					}
					lastSeq, err := w.dependencies.Sequences.LockAndAddTx(txCtx, id, counts[id])
					if err != nil {
						return err
					}
					nextSeq[id] = lastSeq - counts[id]
				}
			}
			messages := make([]messageevent.ChatMessage, 0, len(pending))
			now := time.Now().UnixMilli()
			for _, index := range pending {
				message := commands[index].Message
				message.MessageID, err = w.dependencies.IDGenerator.NextID()
				if err != nil {
					return fmt.Errorf("generate batch message id: %w", err)
				}
				nextSeq[message.SessionID]++
				message.Seq = nextSeq[message.SessionID]
				if message.CreatedAt <= 0 {
					message.CreatedAt = now
				}
				messages = append(messages, message)
				results[index] = application.Result{Message: message, Created: true}
			}
			if err := w.dependencies.Messages.InsertBatchTx(txCtx, messages); err != nil {
				if messagemodel.IsDuplicateKeyError(err) {
					return fmt.Errorf("%w: %w", application.ErrBatchNeedsSplit, err)
				}
				return err
			}
		}
		for i, owner := range owners {
			if owner != i {
				results[i] = application.Result{Message: results[owner].Message, Created: false}
			}
		}
		return nil
	})
	if err != nil {
		// This includes COMMIT errors: never expose results populated before the
		// commit was acknowledged. The caller must confirm with idempotency keys.
		return nil, err
	}
	return results, nil
}
