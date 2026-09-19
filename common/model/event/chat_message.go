package event

// 设置message传递的dto
const (
	//realtime 已经校验消息，并准备将它交给落库消费者。
	ChatMessageAcceptedEventType = "chat.message.accepted"
	// MessageTransfer 已经为消息分配了会话内 Seq，可以交给落库和推送消费者。
	ChatMessageSequencedEventType = "chat.message.sequenced"
	//落库消费者已经将消息写入 MySQL，可以进行在线推送。
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

type ChatMessageSequencedEvent struct {
	EventType string `json:"eventType"`
	Version   int    `json:"version"`

	Message ChatMessage `json:"message"`
}

type ChatMessagePersistedEvent struct {
	EventType string `json:"eventType"`
	Version   int    `json:"version"`

	Message ChatMessage `json:"message"`

	PersistedAt int64 `json:"persistedAt"`
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
