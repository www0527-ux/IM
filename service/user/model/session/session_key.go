package session

import "fmt"

// BuildSingleKey 将两个用户ID规范化为与顺序无关的单聊唯一键。
// 调用者负责保证两个ID合法并且不相同。
func BuildSingleKey(userID, friendID int64) string {
	if userID > friendID {
		userID, friendID = friendID, userID
	}

	return fmt.Sprintf(
		"%d:%d",
		userID,
		friendID,
	)
}
