package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"infinitechatgo/common/auth"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

const issueRetryLimit = 3

type TokenService struct {
	rds           *redis.Redis
	config        auth.Config
	authenticator *auth.Authenticator
}

// 一次登录会话包含的所有信息
type tokenMaterial struct {
	loginSessionID string
	loginSession   auth.LoginSession
	tokenPair      auth.TokenPair
}

func NewTokenService(
	rds *redis.Redis,
	config auth.Config,
) *TokenService {
	return &TokenService{
		rds:           rds,
		config:        config,
		authenticator: auth.NewAuthenticator(rds, config),
	}
}

// 使用userID登录并且创建登录会话携带的信息
func (s *TokenService) buildTokenMaterial(
	userID int64,
) (*tokenMaterial, error) {
	if userID <= 0 {
		return nil, fmt.Errorf("invalid user id: %d", userID)
	}
	if s.config.RefreshExpire <= 0 {
		return nil, fmt.Errorf(
			"invalid refresh expire: %d",
			s.config.RefreshExpire,
		)
	}
	loginSessionID, err := auth.GenerateLoginSessionID()
	if err != nil {
		return nil, fmt.Errorf(
			"generate login session id: %w",
			err,
		)
	}
	jti, err := auth.GenerateJTI()
	if err != nil {
		return nil, fmt.Errorf(
			"generate jti: %w",
			err,
		)
	}
	refreshToken, err := auth.GenerateRefreshToken()
	if err != nil {
		return nil, fmt.Errorf(
			"generate refresh token: %w",
			err,
		)
	}
	refreshTokenHash, err := auth.HashRefreshToken(refreshToken)
	if err != nil {
		return nil, fmt.Errorf(
			"hash refresh token: %w",
			err,
		)
	}
	accessToken, err := auth.GenerateAccessToken(
		s.config,
		userID,
		loginSessionID,
		jti,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"generate access token: %w",
			err,
		)
	}
	now := time.Now()
	expiresAt := now.Add(
		time.Duration(s.config.RefreshExpire) * time.Second,
	)
	return &tokenMaterial{
		loginSessionID: loginSessionID,

		loginSession: auth.LoginSession{
			UserID:           userID,
			CurrentJTI:       jti,
			RefreshTokenHash: refreshTokenHash,
			CreatedAt:        now.Unix(),
			ExpiresAt:        expiresAt.Unix(),
		},

		tokenPair: auth.TokenPair{
			AccessToken:  accessToken,
			RefreshToken: refreshToken,
		},
	}, nil
}
func (s *TokenService) Issue(
	ctx context.Context,
	userID int64,
) (*auth.TokenPair, error) {
	for attempt := 0; attempt < issueRetryLimit; attempt++ {
		material, err := s.buildTokenMaterial(userID)
		if err != nil {
			return nil, err
		}
		//struct to json bytes using for redis to store
		sessionData, err := json.Marshal(
			material.loginSession,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"encode login session: %w",
				err,
			)
		}
		expireSeconds := int(s.config.RefreshExpire)
		//设置sid->会话信息,如果exist就返回false
		sessionCreated, err := s.rds.SetnxExCtx(
			ctx,
			auth.LoginSessionKey(
				material.loginSessionID,
			),
			string(sessionData),
			expireSeconds,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"save login session: %w",
				err,
			)
		}
		if !sessionCreated {
			continue
		}
		refreshCreated, err := s.rds.SetnxExCtx(
			ctx,
			auth.RefreshTokenKey(
				material.loginSession.RefreshTokenHash,
			),
			material.loginSessionID,
			expireSeconds,
		)
		//回滚
		if err != nil {
			s.rollbackNewSession(ctx, userID, material)
			continue
		}
		if !refreshCreated {
			s.rollbackNewSession(ctx, userID, material)
			continue
		}
		if _, err := s.rds.ZaddCtx(
			ctx,
			auth.UserSessionsKey(userID),
			material.loginSession.ExpiresAt, //按照时间排序
			material.loginSessionID,
		); err != nil {
			s.rollbackNewSession(ctx, userID, material)

			return nil, fmt.Errorf(
				"add user login session: %w",
				err,
			)
		}
		//为sessions集合整体设置过期时间
		if err := s.rds.ExpireCtx(
			ctx,
			auth.UserSessionsKey(userID),
			expireSeconds,
		); err != nil {
			s.rollbackNewSession(ctx, userID, material)

			return nil, fmt.Errorf(
				"expire user sessions index: %w",
				err,
			)
		}
		return &material.tokenPair, nil
	}
	return nil, fmt.Errorf(
		"failed to allocate unique token identifiers after %d attempts",
		issueRetryLimit,
	)
}

// 失败回滚--删除对应的redis数据
func (s *TokenService) rollbackNewSession(
	ctx context.Context,
	userID int64,
	material *tokenMaterial,
) {
	_, _ = s.rds.DelCtx(
		ctx,
		auth.LoginSessionKey(
			material.loginSessionID,
		),
		auth.RefreshTokenKey(
			material.loginSession.RefreshTokenHash,
		),
	)

	_, _ = s.rds.ZremCtx(
		ctx,
		auth.UserSessionsKey(userID),
		material.loginSessionID,
	)
}
func (s *TokenService) RevokeByRefreshToken(
	ctx context.Context,
	refreshToken string,
) error {
	refreshTokenHash, err := auth.HashRefreshToken(refreshToken)
	if err != nil {
		return fmt.Errorf("hash refresh token: %w", err)
	}

	refreshKey := auth.RefreshTokenKey(refreshTokenHash)

	// refresh:<hash> → sid
	sessionID, err := s.rds.GetCtx(ctx, refreshKey)
	if err != nil {
		return fmt.Errorf("get refresh token index: %w", err)
	}
	// 已退出或者Token已过期，仍然视为退出成功。
	//满足幂等
	if sessionID == "" {
		return nil
	}

	sessionKey := auth.LoginSessionKey(sessionID)

	sessionJSON, err := s.rds.GetCtx(ctx, sessionKey)
	if err != nil {
		return fmt.Errorf("get login session: %w", err)
	}
	if sessionJSON == "" {
		if _, err := s.rds.DelCtx(ctx, refreshKey); err != nil {
			return fmt.Errorf("delete stale refresh token index: %w", err)
		}
		return nil
	}
	var session auth.LoginSession
	if err := json.Unmarshal(
		[]byte(sessionJSON),
		&session,
	); err != nil {
		return fmt.Errorf("unmarshal login session: %w", err)
	}
	if _, err := s.rds.ZremCtx(
		ctx,
		auth.UserSessionsKey(session.UserID),
		sessionID,
	); err != nil {
		return fmt.Errorf("remove user login session: %w", err)
	}
	// 同时删除RefreshToken索引与登录session。
	if _, err := s.rds.DelCtx(
		ctx,
		refreshKey,
		sessionKey,
	); err != nil {
		return fmt.Errorf("delete login session: %w", err)
	}
	return nil
}

var ErrInvalidRefreshToken = errors.New("invalid refresh token")

var rotateTokenScript = redis.NewScript(`
	local currentSid = redis.call("GET", KEYS[1])

	if not currentSid or currentSid ~= ARGV[1] then
		return 0
	end

	if redis.call("EXISTS", KEYS[2]) == 0 then
		redis.call("DEL", KEYS[1])
		return -1
	end

	if redis.call("EXISTS", KEYS[3]) == 1 then
		return -2
	end

	redis.call("SET", KEYS[2], ARGV[2], "EX", ARGV[3])
	redis.call("SET", KEYS[3], ARGV[1], "EX", ARGV[3])
	redis.call("DEL", KEYS[1])

	return 1

`)

func (s *TokenService) Rotate(
	ctx context.Context,
	oldRefreshToken string,
) (*auth.TokenPair, error) {
	oldHash, err := auth.HashRefreshToken(oldRefreshToken)
	if err != nil {
		return nil, ErrInvalidRefreshToken
	}

	oldRefreshKey := auth.RefreshTokenKey(oldHash)

	sessionID, err := s.rds.GetCtx(ctx, oldRefreshKey)
	if err != nil {
		return nil, fmt.Errorf("get refresh token index: %w", err)
	}
	if sessionID == "" {
		return nil, ErrInvalidRefreshToken
	}
	sessionKey := auth.LoginSessionKey(sessionID)
	sessionJSON, err := s.rds.GetCtx(ctx, sessionKey)
	if err != nil {
		return nil, fmt.Errorf("get login session: %w", err)
	}
	if sessionJSON == "" {
		_, _ = s.rds.DelCtx(ctx, oldRefreshKey)
		return nil, ErrInvalidRefreshToken
	}
	var session auth.LoginSession
	if err := json.Unmarshal(
		[]byte(sessionJSON),
		&session,
	); err != nil {
		return nil, fmt.Errorf("unmarshal login session: %w", err)
	}
	//验证账号边界
	if session.RefreshTokenHash != oldHash ||
		session.UserID <= 0 {
		return nil, ErrInvalidRefreshToken
	}
	//防止自动过期失效
	// 本次轮换不延长登录会话的总有效期。
	expireSeconds := session.ExpiresAt - time.Now().Unix()
	if expireSeconds <= 0 {
		_, _ = s.rds.DelCtx(
			ctx,
			oldRefreshKey,
			sessionKey,
		)
		_, _ = s.rds.ZremCtx(
			ctx,
			auth.UserSessionsKey(session.UserID),
			sessionID,
		)
		return nil, ErrInvalidRefreshToken
	}
	newJTI, err := auth.GenerateJTI()
	if err != nil {
		return nil, fmt.Errorf("generate jti: %w", err)
	}
	newRefreshToken, err := auth.GenerateRefreshToken()
	if err != nil {
		return nil, fmt.Errorf("generate refresh token: %w", err)
	}
	newRefreshHash, err := auth.HashRefreshToken(
		newRefreshToken,
	)
	if err != nil {
		return nil, fmt.Errorf("hash new refresh token: %w", err)
	}
	newAccessToken, err := auth.GenerateAccessToken(
		s.config,
		session.UserID,
		sessionID,
		newJTI,
	)
	if err != nil {
		return nil, fmt.Errorf("generate access token: %w", err)
	}
	session.CurrentJTI = newJTI
	session.RefreshTokenHash = newRefreshHash
	newSessionJSON, err := json.Marshal(session)
	if err != nil {
		return nil, fmt.Errorf("marshal login session: %w", err)
	}
	result, err := s.rds.ScriptRunCtx(
		ctx,
		rotateTokenScript,
		[]string{
			oldRefreshKey,
			sessionKey,
			auth.RefreshTokenKey(newRefreshHash),
		},
		sessionID,
		string(newSessionJSON),
		expireSeconds,
	)
	if err != nil {
		return nil, fmt.Errorf("rotate refresh token: %w", err)
	}
	switch result {
	case int64(1):
		return &auth.TokenPair{
			AccessToken:  newAccessToken,
			RefreshToken: newRefreshToken,
		}, nil

	case int64(0), int64(-1):
		// 旧Token已被使用，或者session已经失效。
		return nil, ErrInvalidRefreshToken

	case int64(-2):
		// 极小概率的新RefreshToken哈希碰撞。
		return nil, errors.New("new refresh token key already exists")

	default:
		return nil, fmt.Errorf(
			"unexpected rotate result: %v",
			result,
		)
	}
}

var ErrInvalidAccessToken = auth.ErrInvalidAccessToken

func (s *TokenService) AuthenticateAccessToken(
	ctx context.Context,
	accessToken string,
) (auth.Identity, error) {
	return s.authenticator.AuthenticateAccessToken(
		ctx,
		accessToken,
	)
}
