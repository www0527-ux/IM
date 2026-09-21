package messageidentity

import (
	"testing"

	messageevent "infinitechatgo/common/model/event"
)

func TestFingerprintIgnoresTransportIdentity(t *testing.T) {
	message := messageevent.ChatMessage{
		MessageID:       100,
		ClientMessageID: "client-a",
		SessionID:       200,
		SessionType:     messageevent.SessionTypeGroup,
		Seq:             1,
		SenderID:        300,
		MessageType:     messageevent.MessageTypeText,
		Body:            messageevent.MessageBody{Content: "hello"},
		CreatedAt:       400,
	}
	first, err := Fingerprint(message)
	if err != nil {
		t.Fatalf("fingerprint first message: %v", err)
	}

	message.MessageID = 101
	message.ClientMessageID = "client-b"
	message.Seq = 2
	message.CreatedAt = 401
	second, err := Fingerprint(message)
	if err != nil {
		t.Fatalf("fingerprint second message: %v", err)
	}
	if first != second {
		t.Fatalf("transport identity changed fingerprint: %q != %q", first, second)
	}
}

func TestFingerprintIncludesRedPacketIdentity(t *testing.T) {
	redPacketID := int64(88)
	message := messageevent.ChatMessage{
		SessionID:   200,
		SessionType: messageevent.SessionTypeGroup,
		SenderID:    300,
		MessageType: messageevent.MessageTypeRedPacket,
		Body: messageevent.MessageBody{
			Content:              "红包",
			RedPacketID:          &redPacketID,
			RedPacketWrapperText: "恭喜发财",
		},
	}
	first, err := Fingerprint(message)
	if err != nil {
		t.Fatalf("fingerprint first red packet: %v", err)
	}

	message.Body.RedPacketWrapperText = "生日快乐"
	second, err := Fingerprint(message)
	if err != nil {
		t.Fatalf("fingerprint second red packet: %v", err)
	}
	if first == second {
		t.Fatal("different red packet wrapper text returned the same fingerprint")
	}
}
