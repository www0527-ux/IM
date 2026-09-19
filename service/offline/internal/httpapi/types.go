package httpapi

import messageevent "infinitechatgo/common/model/event"

type ConversationCursor struct {
	SessionID int64 `json:"sessionId,string"`
	AfterSeq  int64 `json:"afterSeq,string"`
}

type SyncRequest struct {
	Conversations []ConversationCursor `json:"conversations"`
	Limit         int                  `json:"limit"`
}

type ConversationSyncResult struct {
	SessionID    int64                      `json:"sessionId,string"`
	ServerMaxSeq int64                      `json:"serverMaxSeq,string"`
	NextSeq      int64                      `json:"nextSeq,string"`
	HasMore      bool                       `json:"hasMore"`
	Messages     []messageevent.ChatMessage `json:"messages"`
}

type SyncResponse struct {
	Conversations []ConversationSyncResult `json:"conversations"`
}
