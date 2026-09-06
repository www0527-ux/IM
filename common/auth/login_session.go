package auth

//保存到redis中
type LoginSession struct {
	UserID           int64  `json:"userId"`
	CurrentJTI       string `json:"currentJti"`
	RefreshTokenHash string `json:"refreshTokenHash"`
	CreatedAt        int64  `json:"createdAt"`
	ExpiresAt        int64  `json:"expiresAt"`
}
