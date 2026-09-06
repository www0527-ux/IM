package constants

const (
	FriendStatusNormal int64 = iota
	FriendStatusBlocked
	FriendStatusDeleted
)

const (
	FriendApplicationStatusUnread int64 = iota
	FriendApplicationStatusAccepted
	FriendApplicationStatusRejected
	FriendApplicationStatusRead
	FriendApplicationStatusExpired
)
