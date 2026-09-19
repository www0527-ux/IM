package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	messageevent "infinitechatgo/common/model/event"
	"infinitechatgo/service/offline/model/message"
)

var ErrSessionAccessDenied = errors.New("session access denied")

type Cache interface {
	MgetCtx(ctx context.Context, keys ...string) ([]string, error)
}

type MessageStore interface {
	MaxSeq(ctx context.Context, sessionID int64) (int64, error)
	FindBySessionSeqs(ctx context.Context, sessionID int64, seqs []int64) ([]*message.Message, error)
}

type SessionAccessStore interface {
	IsActiveMember(ctx context.Context, userID, sessionID int64) (bool, error)
}

type Reader struct {
	cache       Cache
	messages    MessageStore
	sessionAuth SessionAccessStore
}

type Result struct {
	SessionID    int64
	ServerMaxSeq int64
	NextSeq      int64
	HasMore      bool
	Messages     []messageevent.ChatMessage
}

func NewReader(
	cache Cache,
	messages MessageStore,
	sessionAuth SessionAccessStore,
) *Reader {
	return &Reader{
		cache:       cache,
		messages:    messages,
		sessionAuth: sessionAuth,
	}
}

func (r *Reader) Sync(
	ctx context.Context,
	userID int64,
	sessionID int64,
	afterSeq int64,
	limit int,
) (Result, error) {
	allowed, err := r.sessionAuth.IsActiveMember(ctx, userID, sessionID)
	if err != nil {
		return Result{}, fmt.Errorf("check session membership: %w", err)
	}
	if !allowed {
		return Result{}, ErrSessionAccessDenied
	}

	serverMaxSeq, err := r.messages.MaxSeq(ctx, sessionID)
	if err != nil {
		return Result{}, fmt.Errorf("get persisted max seq: %w", err)
	}

	result := Result{
		SessionID:    sessionID,
		ServerMaxSeq: serverMaxSeq,
		NextSeq:      afterSeq,
		Messages:     []messageevent.ChatMessage{},
	}
	if afterSeq >= serverMaxSeq {
		return result, nil
	}

	endSeq := afterSeq + int64(limit)
	if endSeq < afterSeq || endSeq > serverMaxSeq {
		endSeq = serverMaxSeq
	}
	result.NextSeq = endSeq
	result.HasMore = endSeq < serverMaxSeq

	bySeq := make(map[int64]messageevent.ChatMessage, limit)
	missingSeqs := make([]int64, 0, limit)
	keys := make([]string, 0, limit)
	for seq := afterSeq + 1; seq <= endSeq; seq++ {
		keys = append(keys, messageCacheKey(sessionID, seq))
	}

	values, cacheErr := r.cache.MgetCtx(ctx, keys...)
	if cacheErr == nil && len(values) == len(keys) {
		for index, value := range values {
			seq := afterSeq + 1 + int64(index)
			cached, ok := decodeCachedMessage(value, sessionID, seq)
			if ok {
				bySeq[seq] = cached
				continue
			}
			missingSeqs = append(missingSeqs, seq)
		}
	} else {
		for seq := afterSeq + 1; seq <= endSeq; seq++ {
			missingSeqs = append(missingSeqs, seq)
		}
	}

	rows, err := r.messages.FindBySessionSeqs(ctx, sessionID, missingSeqs)
	if err != nil {
		return Result{}, fmt.Errorf("load messages from database: %w", err)
	}
	for _, row := range rows {
		bySeq[row.Seq] = modelToChatMessage(row)
	}

	seqs := make([]int64, 0, len(bySeq))
	for seq := range bySeq {
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	for _, seq := range seqs {
		result.Messages = append(result.Messages, bySeq[seq])
	}
	return result, nil
}

func decodeCachedMessage(
	payload string,
	sessionID int64,
	seq int64,
) (messageevent.ChatMessage, bool) {
	if payload == "" {
		return messageevent.ChatMessage{}, false
	}
	var event messageevent.ChatMessageSequencedEvent
	if err := json.Unmarshal([]byte(payload), &event); err != nil {
		return messageevent.ChatMessage{}, false
	}
	if event.EventType != messageevent.ChatMessageSequencedEventType ||
		event.Version != messageevent.ChatMessageEventVersion ||
		event.Message.SessionID != sessionID ||
		event.Message.Seq != seq {
		return messageevent.ChatMessage{}, false
	}
	return event.Message, true
}

func modelToChatMessage(row *message.Message) messageevent.ChatMessage {
	chatMessage := messageevent.ChatMessage{
		MessageID:       row.MessageId,
		ClientMessageID: row.ClientMessageId,
		SessionID:       row.SessionId,
		SessionType:     messageevent.SessionType(row.SessionType),
		Seq:             row.Seq,
		SenderID:        row.SenderId,
		MessageType:     messageevent.MessageType(row.MessageType),
		Body: messageevent.MessageBody{
			Content: row.Content,
		},
		CreatedAt: row.CreatedTime.UnixMilli(),
	}
	if row.ReplyId.Valid {
		replyID := row.ReplyId.Int64
		chatMessage.Body.ReplyID = &replyID
	}
	return chatMessage
}

func messageCacheKey(sessionID, seq int64) string {
	return fmt.Sprintf("im:message-cache:{%d}:%d", sessionID, seq)
}
