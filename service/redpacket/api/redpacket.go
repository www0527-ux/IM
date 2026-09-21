// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"

	"infinitechatgo/common/common"
	commonmiddleware "infinitechatgo/common/middleware"
	"infinitechatgo/common/utils"
	"infinitechatgo/service/redpacket/api/internal/config"
	"infinitechatgo/service/redpacket/api/internal/handler"
	"infinitechatgo/service/redpacket/api/internal/svc"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/rest/httpx"
)

var configFile = flag.String("f", "etc/redpacket-api.yaml", "the config file")

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
				logx.WithContext(ctx).Errorf(
					"redpacket system error: code=%d, message=%s, cause=%v",
					appErr.Code,
					appErr.Message,
					appErr.Cause,
				)
				return http.StatusOK, utils.FailWithCode(500, appErr.Message)
			}
			return http.StatusOK, utils.Fail(appErr)
		}

		logx.WithContext(ctx).Errorf("unhandled redpacket error: %v", err)
		return http.StatusInternalServerError,
			utils.FailWithCode(500, common.SystemError.Message)
	})

	server := rest.MustNewServer(c.RestConf)
	server.Use(commonmiddleware.RecoverMiddleware)
	defer server.Stop()

	ctx := svc.NewServiceContext(c)
	handler.RegisterHandlers(server, ctx)

	fmt.Printf("Starting server at %s:%d...\n", c.Host, c.Port)
	server.Start()
}
