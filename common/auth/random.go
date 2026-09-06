package auth

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
)

const identifierBytes = 16

func generateRandomString(byteLength int) (string, error) {
	if byteLength <= 0 {
		return "", errors.New("random byte length must be positive")
	}

	data := make([]byte, byteLength)

	if _, err := rand.Read(data); err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(data), nil
}

func GenerateLoginSessionID() (string, error) {
	return generateRandomString(identifierBytes)
}

func GenerateJTI() (string, error) {
	return generateRandomString(identifierBytes)
}

