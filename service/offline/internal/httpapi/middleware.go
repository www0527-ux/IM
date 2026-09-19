package httpapi

import (
	"errors"
	"net/http"
	"strings"

	"infinitechatgo/common/auth"
	"infinitechatgo/common/common"
	"infinitechatgo/common/utils"

	"github.com/zeromicro/go-zero/rest/httpx"
)

type AuthMiddleware struct {
	authenticator *auth.Authenticator
}

func NewAuthMiddleware(authenticator *auth.Authenticator) *AuthMiddleware {
	return &AuthMiddleware{authenticator: authenticator}
}

func (m *AuthMiddleware) Handle(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fields := strings.Fields(r.Header.Get("Authorization"))
		if len(fields) != 2 ||
			!strings.EqualFold(fields[0], "Bearer") ||
			fields[1] == "" {
			writeError(r, w, http.StatusUnauthorized, common.NotLoginError)
			return
		}

		identity, err := m.authenticator.AuthenticateAccessToken(
			r.Context(),
			fields[1],
		)
		if errors.Is(err, auth.ErrInvalidAccessToken) {
			writeError(r, w, http.StatusUnauthorized, common.NotLoginError)
			return
		}
		if err != nil {
			writeError(r, w, http.StatusInternalServerError, common.SystemError)
			return
		}

		ctx := auth.ContextWithIdentity(r.Context(), identity)
		next(w, r.WithContext(ctx))
	}
}

func writeError(
	r *http.Request,
	w http.ResponseWriter,
	status int,
	appError common.ErrorCode,
) {
	httpx.WriteJsonCtx(
		r.Context(),
		w,
		status,
		utils.Fail(appError),
	)
}
