package auth

import "github.com/golang-jwt/jwt/v4"

//payload
type AccessClaims struct {
	LoginSessionID string `json:"sid"`
	TokenType      string `json:"typ"`
	jwt.RegisteredClaims
}

// Issuer    → iss
// Subject   → sub，也就是userId
// ExpiresAt → exp
// IssuedAt  → iat
// ID        → jti
