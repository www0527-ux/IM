package application

import (
	"context"
	"encoding/json"
	"fmt"

	redpacketmodel "infinitechatgo/service/redpacket/model/red_packet"
)

type sendResultCacheWriter interface {
	SetexCtx(ctx context.Context, key, value string, seconds int) error
}

type redPacketLookup interface {
	FindOne(ctx context.Context, redPacketID int64) (*redpacketmodel.RedPacket, error)
}

// SendResultCacheProjector builds a disposable send-idempotency result from
// the red_packet row committed alongside the cache-build Outbox event.
type SendResultCacheProjector struct {
	cache   sendResultCacheWriter
	packets redPacketLookup
}

func NewSendResultCacheProjector(cache sendResultCacheWriter, packets redPacketLookup) *SendResultCacheProjector {
	return &SendResultCacheProjector{cache: cache, packets: packets}
}

func (p *SendResultCacheProjector) Build(ctx context.Context, redPacketID int64) error {
	if p == nil || p.cache == nil || p.packets == nil || redPacketID <= 0 {
		return fmt.Errorf("red packet send-result cache dependencies or ID are invalid")
	}
	packet, err := p.packets.FindOne(ctx, redPacketID)
	if err != nil {
		return fmt.Errorf("load committed red packet %d for send-result cache: %w", redPacketID, err)
	}
	if packet == nil || packet.RedPacketId != redPacketID || packet.MessageId <= 0 {
		return fmt.Errorf("red packet %d has invalid send-result facts", redPacketID)
	}
	command := SendCommand{
		SenderID: packet.SenderId, ClientMessageID: packet.ClientMessageId,
		SessionID: packet.SessionId, SessionType: packet.SessionType,
		RedPacketType: packet.RedPacketType, WrapperText: packet.WrapperText,
		TotalAmount: packet.TotalAmount, TotalCount: packet.TotalCount,
	}
	if packet.ReceiverId.Valid {
		command.ReceiverID = packet.ReceiverId.Int64
	}
	if err := ValidateSendCommand(command); err != nil {
		return fmt.Errorf("red packet %d has invalid send command: %w", redPacketID, err)
	}
	entry := sendCacheEntry{
		Command: command,
		Result: SendResult{
			RedPacketID: redPacketID, MessageID: packet.MessageId,
			Status: redPacketActiveStatus,
		},
	}
	value, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("encode red packet %d send-result cache: %w", redPacketID, err)
	}
	if err := p.cache.SetexCtx(ctx, sendCacheKey(command), string(value), redPacketSendCacheTTL); err != nil {
		return fmt.Errorf("write red packet %d send-result cache: %w", redPacketID, err)
	}
	return nil
}
