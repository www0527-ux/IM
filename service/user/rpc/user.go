package main

import (
	"flag"
	"fmt"

	"infinitechatgo/service/user/rpc/internal/config"
	"infinitechatgo/service/user/rpc/internal/server"
	"infinitechatgo/service/user/rpc/internal/svc"
	"infinitechatgo/service/user/rpc/user"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var configFile = flag.String("f", "etc/user.yaml", "the config file")

func main() {
	//更新configFile的指向字符串
	flag.Parse()

	var c config.Config

	conf.MustLoad(*configFile, &c)
	//
	ctx := svc.NewServiceContext(c)

	s := zrpc.MustNewServer(
		c.RpcServerConf, 
		func(grpcServer *grpc.Server) {
			//注册方法到RPC路由表中（第二个参数作为logic实现者满足grpc生成的接口）
			user.RegisterUserRpcServer(grpcServer, server.NewUserRpcServer(ctx))
			//开启reflection之后，调试工具可以直接询问服务器proto的内容
			if c.Mode == service.DevMode || c.Mode == service.TestMode {
				reflection.Register(grpcServer)
			}
		},
	)
	defer s.Stop()

	fmt.Printf("Starting rpc server at %s...\n", c.ListenOn)
	s.Start()
}
