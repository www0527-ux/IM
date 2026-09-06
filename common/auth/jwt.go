package auth

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

func GenerateAccessToken(
	config Config,
	userID int64,
	loginSessionID string,
	jti string,
) (string, error) {
	if config.AccessSecret == "" ||
		config.Issuer == "" ||
		config.AccessExpire <= 0 {
		return "", errors.New("invalid auth config")
	}

	if userID <= 0 ||
		loginSessionID == "" ||
		jti == "" {
		return "", errors.New("invalid access token params")
	}

	now := time.Now()

	claims := AccessClaims{
		LoginSessionID: loginSessionID,
		TokenType:      TokenTypeAccess,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:  config.Issuer,
			Subject: strconv.FormatInt(userID, 10),
			ExpiresAt: jwt.NewNumericDate(
				now.Add(time.Duration(config.AccessExpire) * time.Second),
			),
			IssuedAt: jwt.NewNumericDate(now),
			ID:       jti,
		},
	}
	token := jwt.NewWithClaims(
		jwt.SigningMethodHS256,
		claims,
	) //待签名对象
	return token.SignedString([]byte(config.AccessSecret))
}
func ParseAccessToken(
	config Config,
	tokenString string,
)(*AccessClaims,error){
	if config.AccessSecret == "" || config.Issuer == "" {
		return nil, errors.New("invalid auth config")
	}
	if tokenString == "" {
		return nil, errors.New("access token is empty")
	}
	claims := &AccessClaims{}
	//创建go中的token对象，反序列化转换为claim的字段
	token,err:=jwt.ParseWithClaims(
		tokenString,
		//接收payload
		claims,
		//获得验证token的密钥
		func(token *jwt.Token) (interface{}, error) {
			return []byte(config.AccessSecret), nil
		},
		//验证算法（alg）的种类，算法白名单
		jwt.WithValidMethods(
			[]string{jwt.SigningMethodHS256.Alg()},
		),
	)
	if err != nil {
		return nil, fmt.Errorf("parse access token: %w", err)
	}
	if !token.Valid {
		return nil, errors.New("invalid access token")
	}
	if claims.Issuer != config.Issuer {
		return nil, errors.New("invalid token issuer")
	}
	if claims.TokenType != TokenTypeAccess {
		return nil, errors.New("invalid token type")
	}
	if claims.Subject == "" ||
		claims.LoginSessionID == "" ||
		claims.ID == "" {
		return nil, errors.New("incomplete access token claims")
	}
	if _, err := strconv.ParseInt(claims.Subject, 10, 64); err != nil {
		return nil, errors.New("invalid token subject")
	}
	return claims, nil
}