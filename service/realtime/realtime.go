package main

import (
	"flag"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
	"infinitechatgo/common/auth"
	"infinitechatgo/service/realtime/internal/config"
	realtimews "infinitechatgo/service/realtime/internal/websocket"
	"infinitechatgo/service/user/rpc/userrpc"
	"log"
)

var configFile = flag.String(
	"f",
	"etc/realtime.yaml",
	"the config file",
)

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c, conf.UseEnv())
	//准备依赖
	rds := redis.MustNewRedis(c.Redis)
	authenticator := auth.NewAuthenticator(rds, c.Auth)
	userRPC := userrpc.NewUserRpc(
		zrpc.MustNewClient(c.UserRpc),//获得etcd的连接
	)

	
	if err := realtimews.StartServer(
		c.ListenOn,
		authenticator,
		userRPC,
	); err != nil {
		log.Fatal(err)
	}
}
