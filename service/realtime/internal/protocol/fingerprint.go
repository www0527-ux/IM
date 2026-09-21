package protocol

import (
	"infinitechatgo/common/messageidentity"
)

func FingerprintSendMessageRequest(
	request SendMessageRequest,
	senderID int64,
) (string, error) {
	message := BuildChatMessage(request, senderID, 0, 0)
	return messageidentity.Fingerprint(message)
}
