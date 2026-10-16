package application

import (
	"context"
	"encoding/json"
	"testing"

	redpacketmodel "infinitechatgo/service/redpacket/model/red_packet"
)

type fixedRedPacketLookup struct{ packet *redpacketmodel.RedPacket }

func (l fixedRedPacketLookup) FindOne(context.Context, int64) (*redpacketmodel.RedPacket, error) {
	return l.packet, nil
}

type recordingSendCache struct {
	key, value string
	ttl        int
}

func (c *recordingSendCache) SetexCtx(_ context.Context, key, value string, ttl int) error {
	c.key, c.value, c.ttl = key, value, ttl
	return nil
}

func TestSendResultCacheUsesCommittedPacketAndOriginalSendResult(t *testing.T) {
	packet := &redpacketmodel.RedPacket{
		RedPacketId: 12, MessageId: 34, SenderId: 56,
		ClientMessageId: "request-1", SessionId: 78,
		SessionType: SessionTypeGroup, RedPacketType: RedPacketTypeOrdinary,
		WrapperText: "红包", TotalAmount: 200, TotalCount: 2,
		Status: redPacketExpiredStatus,
	}
	cache := &recordingSendCache{}
	projector := NewSendResultCacheProjector(cache, fixedRedPacketLookup{packet: packet})
	if err := projector.Build(context.Background(), packet.RedPacketId); err != nil {
		t.Fatal(err)
	}
	if cache.key != "im:redpacket:send:v1:56:request-1" || cache.ttl != redPacketSendCacheTTL {
		t.Fatalf("cache destination = %q ttl=%d", cache.key, cache.ttl)
	}
	var entry sendCacheEntry
	if err := json.Unmarshal([]byte(cache.value), &entry); err != nil {
		t.Fatal(err)
	}
	if entry.Command.TotalAmount != 200 || entry.Command.SessionID != 78 ||
		entry.Result.RedPacketID != 12 || entry.Result.MessageID != 34 ||
		entry.Result.Status != redPacketActiveStatus {
		t.Fatalf("send-result cache = %+v", entry)
	}
}
