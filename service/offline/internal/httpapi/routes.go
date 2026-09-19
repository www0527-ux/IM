package httpapi

import (
	"net/http"

	"infinitechatgo/service/offline/internal/svc"

	"github.com/zeromicro/go-zero/rest"
)

func RegisterRoutes(server *rest.Server, svcCtx *svc.ServiceContext) {
	authMiddleware := NewAuthMiddleware(svcCtx.Authenticator)
	server.AddRoutes(
		rest.WithMiddlewares(
			[]rest.Middleware{authMiddleware.Handle},
			[]rest.Route{
				{
					Method:  http.MethodPost,
					Path:    "/sync",
					Handler: SyncHandler(svcCtx),
				},
			}...,
		),
		rest.WithPrefix("/api/offline"),
	)
}
