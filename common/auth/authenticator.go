package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

var ErrInvalidAccessToken = errors.New("invalid access token")

// Authenticator validates an access token and its server-side login session.
type Authenticator struct {
	rds    *redis.Redis
	config Config
}

func NewAuthenticator(
	rds *redis.Redis,
	config Config,
) *Authenticator {
	return &Authenticator{
		rds:    rds,
		config: config,
	}
}

func (a *Authenticator) AuthenticateAccessToken(
	ctx context.Context,
	accessToken string,
) (Identity, error) {
	claims, err := ParseAccessToken(a.config, accessToken)
	if err != nil {
		return Identity{}, ErrInvalidAccessToken
	}

	userID, err := strconv.ParseInt(claims.Subject, 10, 64)
	if err != nil || userID <= 0 {
		return Identity{}, ErrInvalidAccessToken
	}

	sessionJSON, err := a.rds.GetCtx(
		ctx,
		LoginSessionKey(claims.LoginSessionID),
	)
	if err != nil {
		return Identity{}, fmt.Errorf(
			"get login session: %w",
			err,
		)
	}
	if sessionJSON == "" {
		return Identity{}, ErrInvalidAccessToken
	}

	var session LoginSession
	if err := json.Unmarshal(
		[]byte(sessionJSON),
		&session,
	); err != nil {
		return Identity{}, fmt.Errorf(
			"unmarshal login session: %w",
			err,
		)
	}

	if session.UserID != userID ||
		session.CurrentJTI != claims.ID ||
		session.ExpiresAt <= time.Now().Unix() {
		return Identity{}, ErrInvalidAccessToken
	}

	return Identity{
		UserID:         userID,
		LoginSessionID: claims.LoginSessionID,
	}, nil
}
