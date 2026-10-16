package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	redpacketmodel "infinitechatgo/service/redpacket/model/red_packet"
	"infinitechatgo/service/user/rpc/userrpc"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

type QueryService struct {
	db      sqlx.SqlConn
	packets redpacketmodel.RedPacketModel
	userRPC userrpc.UserRpc
}

type PacketDetail struct {
	RedPacketID    int64
	MessageID      int64
	SenderID       int64
	SessionID      int64
	SessionType    int64
	RedPacketType  int64
	WrapperText    string
	TotalAmount    int64
	TotalCount     int64
	ReceivedAmount int64
	ReceivedCount  int64
	Status         int64
	ExpireAt       time.Time
	Receives       []PacketReceive
}

type PacketReceive struct {
	ReceiverID int64
	Amount     int64
	ReceivedAt time.Time
}

func NewQueryService(db sqlx.SqlConn, packets redpacketmodel.RedPacketModel,
	userRPC userrpc.UserRpc) *QueryService {
	return &QueryService{db: db, packets: packets, userRPC: userRPC}
}

func (s *QueryService) Get(ctx context.Context, packetID, userID int64) (PacketDetail, error) {
	if s == nil || s.db == nil || s.packets == nil || s.userRPC == nil || packetID <= 0 || userID <= 0 {
		return PacketDetail{}, fmt.Errorf("invalid red packet query dependencies or IDs")
	}
	packet, err := s.packets.FindOne(ctx, packetID)
	if errors.Is(err, redpacketmodel.ErrNotFound) {
		return PacketDetail{}, ErrReceiveNotFound
	}
	if err != nil {
		return PacketDetail{}, err
	}
	if packet.SenderId != userID {
		if packet.SessionType == SessionTypeSingle {
			if !packet.ReceiverId.Valid || packet.ReceiverId.Int64 != userID {
				return PacketDetail{}, ErrReceivePermission
			}
		} else if packet.SessionType == SessionTypeGroup {
			lookupCtx, cancel := context.WithTimeout(ctx, permissionTimeout)
			defer cancel()
			response, err := s.userRPC.ValidateGroupMessage(lookupCtx,
				&userrpc.ValidateGroupMessageReq{SenderId: userID, SessionId: packet.SessionId})
			if err != nil {
				return PacketDetail{}, fmt.Errorf("check packet reader membership: %w", err)
			}
			if response == nil || !response.Allowed {
				return PacketDetail{}, ErrReceivePermission
			}
		} else {
			return PacketDetail{}, ErrReceiveInvariant
		}
	}
	var rows []struct {
		ReceiverID int64     `db:"receiver_id"`
		Amount     int64     `db:"amount"`
		ReceivedAt time.Time `db:"received_at"`
	}
	if err := s.db.QueryRowsCtx(ctx, &rows, `SELECT receiver_id, amount, received_at
		FROM red_packet_receive WHERE red_packet_id = ?
		ORDER BY received_at, red_packet_receive_id`, packetID); err != nil {
		return PacketDetail{}, fmt.Errorf("list red packet receives: %w", err)
	}
	detail := packetDetail(packet)
	detail.Receives = make([]PacketReceive, 0, len(rows))
	for _, row := range rows {
		detail.Receives = append(detail.Receives, PacketReceive{
			ReceiverID: row.ReceiverID, Amount: row.Amount, ReceivedAt: row.ReceivedAt,
		})
	}
	return detail, nil
}

func (s *QueryService) ListSent(ctx context.Context, senderID, pageNum, pageSize int64) ([]PacketDetail, int64, error) {
	if s == nil || s.db == nil || senderID <= 0 || pageNum < 1 || pageSize < 1 || pageSize > 100 ||
		pageNum > (1<<63-1)/pageSize {
		return nil, 0, fmt.Errorf("invalid sent packet query dependencies or pagination")
	}
	var total int64
	if err := s.db.QueryRowCtx(ctx, &total,
		`SELECT COUNT(1) FROM red_packet WHERE sender_id = ?`, senderID); err != nil {
		return nil, 0, err
	}
	var packets []*redpacketmodel.RedPacket
	if err := s.db.QueryRowsCtx(ctx, &packets, `SELECT * FROM red_packet
		WHERE sender_id = ? ORDER BY created_time DESC, red_packet_id DESC LIMIT ? OFFSET ?`,
		senderID, pageSize, (pageNum-1)*pageSize); err != nil {
		return nil, 0, err
	}
	details := make([]PacketDetail, 0, len(packets))
	for _, packet := range packets {
		details = append(details, packetDetail(packet))
	}
	return details, total, nil
}

func packetDetail(packet *redpacketmodel.RedPacket) PacketDetail {
	return PacketDetail{
		RedPacketID: packet.RedPacketId, MessageID: packet.MessageId,
		SenderID: packet.SenderId, SessionID: packet.SessionId,
		SessionType: packet.SessionType, RedPacketType: packet.RedPacketType,
		WrapperText: packet.WrapperText, TotalAmount: packet.TotalAmount,
		TotalCount: packet.TotalCount, ReceivedAmount: packet.ReceivedAmount,
		ReceivedCount: packet.ReceivedCount, Status: packet.Status,
		ExpireAt: packet.ExpireAt,
	}
}
