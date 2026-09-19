package protocol

import (
	"testing"

	messageevent "infinitechatgo/common/model/event"
)

func TestFingerprintSendMessageRequest(t *testing.T) {
	receiverID := int64(20)
	request := SendMessageRequest{
		ClientMessageID: "client-1",
		SessionID:       100,
		SessionType:     messageevent.SessionTypeSingle,
		ReceiverID:      &receiverID,
		MessageType:     messageevent.MessageTypeText,
		Body: messageevent.MessageBody{
			Content: "hello",
		},
	}

	first, err := FingerprintSendMessageRequest(request, 10)
	if err != nil {
		t.Fatalf("first fingerprint: %v", err)
	}
	second, err := FingerprintSendMessageRequest(request, 10)
	if err != nil {
		t.Fatalf("second fingerprint: %v", err)
	}
	if first != second {
		t.Fatalf("same request returned different fingerprints: %q != %q", first, second)
	}

	request.Body.Content = "changed"
	changed, err := FingerprintSendMessageRequest(request, 10)
	if err != nil {
		t.Fatalf("changed fingerprint: %v", err)
	}
	if first == changed {
		t.Fatal("different content returned the same fingerprint")
	}
}
