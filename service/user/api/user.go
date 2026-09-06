// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1
// 作为user api服务的入口
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"

	"infinitechatgo/common/common"
	"infinitechatgo/common/middleware"
	"infinitechatgo/common/utils"
	"infinitechatgo/service/user/api/internal/config"
	"infinitechatgo/service/user/api/internal/handler"
	"infinitechatgo/service/user/api/internal/svc"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/rest/httpx"
)

var configFile = flag.String("f", "etc/user-api.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c, conf.UseEnv())

	httpx.SetOkHandler(func(_ context.Context, data any) any {
		return utils.Success(data)
	})

	httpx.SetErrorHandlerCtx(func(ctx context.Context, err error) (int, any) {
		var appErr common.ErrorCode
		if errors.As(err, &appErr) {
			if appErr.Code >= 50000 && appErr.Code < 60000 {
				if appErr.Cause != nil {
					logx.WithContext(ctx).Errorf(
						"system error: code=%d, message=%s, cause=%v",
						appErr.Code,
						appErr.Message,
						appErr.Cause,
					)
				} else {
					logx.WithContext(ctx).Errorf(
						"system error: code=%d, message=%s",
						appErr.Code,
						appErr.Message,
					)
				}

				return http.StatusOK, utils.FailWithCode(500, appErr.Message)
			}

			logx.WithContext(ctx).Infof(
				"business error: code=%d, message=%s",
				appErr.Code,
				appErr.Message,
			)
			return http.StatusOK, utils.Fail(appErr)
		}

		logx.WithContext(ctx).Errorf("unhandled error: %v", err)
		return http.StatusInternalServerError,
			utils.FailWithCode(500, common.SystemError.Message)
	})

	server := rest.MustNewServer(c.RestConf)
	server.Use(middleware.RecoverMiddleware)
	defer server.Stop()

	ctx := svc.NewServiceContext(c)
	handler.RegisterHandlers(server, ctx)

	fmt.Printf("Starting server at %s:%d...\n", c.Host, c.Port)
	server.Start()
}
