package event

// 设置message传递的dto
const (
	// ChatMessageCreateRequestedEventType asks the message domain to persist a
	// server-authenticated message. Red packet transactions publish this event
	// through Outbox; it is not proof that the message row already exists.
	ChatMessageCreateRequestedEventType = "chat.message.create_requested"
	// Deprecated: kept only while the legacy pipeline is available as a
	// migration fallback.
	ChatMessageAcceptedEventType = "chat.message.accepted"
	// Deprecated: target architecture assigns Seq in the message transaction.
	ChatMessageSequencedEventType = "chat.message.sequenced"
	// ChatMessagePersistedEventType is emitted only after message, Seq and its
	// Outbox row have committed in one MySQL transaction.
	ChatMessagePersistedEventType = "chat.message.persisted"

	ChatMessageEventVersion = 1
)

type SessionType int64

const (
	SessionTypeSingle SessionType = 0
	SessionTypeGroup  SessionType = 1
	SessionTypeRobot  SessionType = 2
)

type MessageType int64

const (
	MessageTypeText      MessageType = 0
	MessageTypeImage     MessageType = 1
	MessageTypeEmoji     MessageType = 2
	MessageTypeRedPacket MessageType = 3
)

// 消息内容本体
type MessageBody struct {
	Content string `json:"content"`

	ReplyID *int64 `json:"replyId,string,omitempty"`

	RedPacketID          *int64 `json:"redPacketId,string,omitempty"`
	RedPacketWrapperText string `json:"redPacketWrapperText,omitempty"`
	//也就是messageId
}

// 一条经过服务端认证和补全的标准聊天消息
type ChatMessage struct {
	MessageID int64 `json:"messageId,string"`

	ClientMessageID string `json:"clientMessageId"`

	SessionID   int64       `json:"sessionId,string"`
	SessionType SessionType `json:"sessionType"`
	Seq         int64       `json:"seq,string"`

	SenderID   int64  `json:"senderId,string"`
	ReceiverID *int64 `json:"receiverId,string,omitempty"`

	MessageType MessageType `json:"messageType"`
	Body        MessageBody `json:"body"`

	CreatedAt int64 `json:"createdAt"`
}

type ChatMessageAcceptedEvent struct {
	EventType string `json:"eventType"`
	Version   int    `json:"version"`

	Message ChatMessage `json:"message"`
}

type ChatMessageCreateRequestedEvent struct {
	EventID   int64  `json:"eventId,string"`
	EventType string `json:"eventType"`
	Version   int    `json:"version"`

	Message     ChatMessage `json:"message"`
	Fingerprint string      `json:"fingerprint"`
}

type ChatMessageSequencedEvent struct {
	EventType string `json:"eventType"`
	Version   int    `json:"version"`

	Message ChatMessage `json:"message"`
}

type ChatMessagePersistedEvent struct {
	EventID   int64  `json:"eventId,string,omitempty"`
	EventType string `json:"eventType"`
	Version   int    `json:"version"`

	Message     ChatMessage `json:"message"`
	Fingerprint string      `json:"fingerprint,omitempty"`

	PersistedAt int64 `json:"persistedAt"`
}

func NewChatMessageCreateRequestedEvent(
	eventID int64,
	message ChatMessage,
	fingerprint string,
) ChatMessageCreateRequestedEvent {
	return ChatMessageCreateRequestedEvent{
		EventID:     eventID,
		EventType:   ChatMessageCreateRequestedEventType,
		Version:     ChatMessageEventVersion,
		Message:     message,
		Fingerprint: fingerprint,
	}
}

func NewChatMessageAcceptedEvent(
	message ChatMessage,
) ChatMessageAcceptedEvent {
	return ChatMessageAcceptedEvent{
		EventType: ChatMessageAcceptedEventType,
		Version:   ChatMessageEventVersion,
		Message:   message,
	}
}

func NewChatMessageSequencedEvent(
	message ChatMessage,
) ChatMessageSequencedEvent {
	return ChatMessageSequencedEvent{
		EventType: ChatMessageSequencedEventType,
		Version:   ChatMessageEventVersion,
		Message:   message,
	}
}

func NewChatMessagePersistedEvent(
	message ChatMessage,
	persistedAt int64,
) ChatMessagePersistedEvent {
	return ChatMessagePersistedEvent{
		EventType:   ChatMessagePersistedEventType,
		Version:     ChatMessageEventVersion,
		Message:     message,
		PersistedAt: persistedAt,
	}
}

func NewChatMessagePersistedEventWithIdentity(
	eventID int64,
	message ChatMessage,
	fingerprint string,
	persistedAt int64,
) ChatMessagePersistedEvent {
	return ChatMessagePersistedEvent{
		EventID:     eventID,
		EventType:   ChatMessagePersistedEventType,
		Version:     ChatMessageEventVersion,
		Message:     message,
		Fingerprint: fingerprint,
		PersistedAt: persistedAt,
	}
}
