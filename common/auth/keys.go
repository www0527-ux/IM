package auth

import "strconv"

const (
	TokenTypeAccess   = "access"
	RefreshTokenBytes = 32
)

// login_session:<sid>  → 登录会话内容
// refresh:<hash>       → sid
// user_session:<uid>   → sid

const (
	loginSessionKeyPrefix = "infinitechat:auth:login_session:"
	refreshTokenKeyPrefix = "infinitechat:auth:refresh:"
	userSessionsKeyPrefix  = "infinitechat:auth:user_sessions:"
)

func LoginSessionKey(loginSessionID string) string {
	return loginSessionKeyPrefix + loginSessionID
}

func RefreshTokenKey(refreshTokenHash string) string {
	return refreshTokenKeyPrefix + refreshTokenHash
}

func UserSessionsKey(userID int64) string {
	return userSessionsKeyPrefix + strconv.FormatInt(userID, 10)
}
