package logic

import (
	"infinitechatgo/service/redpacket/api/internal/types"
	"infinitechatgo/service/redpacket/application"
)

func detailResponse(d application.PacketDetail) types.RedPacketDetailResponse {
	result := types.RedPacketDetailResponse{
		RedPacketId: d.RedPacketID, MessageId: d.MessageID,
		SenderId: d.SenderID, SessionId: d.SessionID,
		SessionType: d.SessionType, RedPacketType: d.RedPacketType,
		WrapperText: d.WrapperText, TotalAmount: d.TotalAmount,
		TotalCount: d.TotalCount, ReceivedAmount: d.ReceivedAmount,
		ReceivedCount: d.ReceivedCount, Status: d.Status,
		ExpireAt: d.ExpireAt.UnixMilli(),
		Receives: make([]types.RedPacketReceiveItem, 0, len(d.Receives)),
	}
	for _, receive := range d.Receives {
		result.Receives = append(result.Receives, types.RedPacketReceiveItem{
			ReceiverId: receive.ReceiverID, Amount: receive.Amount,
			ReceivedAt: receive.ReceivedAt.UnixMilli(),
		})
	}
	return result
}

func sentPacketSummary(d application.PacketDetail) types.SentRedPacketSummary {
	return types.SentRedPacketSummary{
		RedPacketId: d.RedPacketID, MessageId: d.MessageID,
		SenderId: d.SenderID, SessionId: d.SessionID,
		SessionType: d.SessionType, RedPacketType: d.RedPacketType,
		WrapperText: d.WrapperText, TotalAmount: d.TotalAmount,
		TotalCount: d.TotalCount, ReceivedAmount: d.ReceivedAmount,
		ReceivedCount: d.ReceivedCount, Status: d.Status,
		ExpireAt: d.ExpireAt.UnixMilli(),
	}
}
