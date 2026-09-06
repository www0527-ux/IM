package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

func GenerateRefreshToken() (string, error) {
	return generateRandomString(RefreshTokenBytes)
}

func HashRefreshToken(refreshToken string) (string, error) {
	if refreshToken == "" {
		return "", errors.New("refresh token is empty")
	}
	hash := sha256.Sum256([]byte(refreshToken))
	return hex.EncodeToString(hash[:]), nil
}
