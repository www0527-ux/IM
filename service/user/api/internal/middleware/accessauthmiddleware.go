// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package middleware

import (
	"errors"
	"net/http"
	"strings"

	"infinitechatgo/common/auth"
	"infinitechatgo/common/common"
	userService "infinitechatgo/service/user/api/internal/service"

	"github.com/zeromicro/go-zero/rest/httpx"
)

type AccessAuthMiddleware struct {
	tokenService *userService.TokenService
}

func NewAccessAuthMiddleware(tokenService *userService.TokenService) *AccessAuthMiddleware {
	return &AccessAuthMiddleware{
		tokenService: tokenService,
	}
}

func (m *AccessAuthMiddleware) Handle(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fields := strings.Fields(
			r.Header.Get("Authorization"),
		)
		if len(fields) != 2 ||
			!strings.EqualFold(fields[0], "Bearer") ||
			fields[1] == "" {
			httpx.ErrorCtx(
				r.Context(),
				w,
				common.NotLoginError,
			)
			return
		}
		identity, err := m.tokenService.AuthenticateAccessToken(
			r.Context(),
			fields[1],
		)
		if errors.Is(
			err,
			userService.ErrInvalidAccessToken,
		) {
			httpx.ErrorCtx(
				r.Context(),
				w,
				common.NotLoginError,
			)
			return
		}

		if err != nil {
			httpx.ErrorCtx(
				r.Context(),
				w,
				common.WrapError(
					common.SystemError,
					err,
				),
			)
			return
		}
		//生成带有identity信息的context
		ctx := auth.ContextWithIdentity(
			r.Context(),
			identity,
		)
		next(
			w,
			r.WithContext(ctx),
		)
	}
}
