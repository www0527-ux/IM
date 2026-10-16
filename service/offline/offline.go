package main

import (
	"flag"
	"log"

	"infinitechatgo/common/middleware"
	"infinitechatgo/service/offline/internal/config"
	"infinitechatgo/service/offline/internal/httpapi"
	"infinitechatgo/service/offline/internal/svc"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/rest"
)

var configFile = flag.String(
	"f",
	"etc/offline.yaml",
	"the config file",
)

func main() {
	flag.Parse()
	var c config.Config
	conf.MustLoad(
		*configFile,
		&c,
		conf.UseEnv(),
	)
	svcCtx := svc.NewServiceContext(c)

	server := rest.MustNewServer(c.RestConf)
	server.Use(middleware.RecoverMiddleware)
	httpapi.RegisterRoutes(server, svcCtx)
	log.Printf(
		"offline sync API started: http://%s:%d/api/offline/sync",
		c.Host,
		c.Port,
	)

	server.Start()
}
