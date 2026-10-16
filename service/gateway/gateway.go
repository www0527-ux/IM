package main

import (
	"errors"
	"flag"
	"log"
	"net/http"
	"time"

	"infinitechatgo/service/gateway/internal/config"
	"infinitechatgo/service/gateway/internal/proxy"

	"github.com/zeromicro/go-zero/core/conf"
)

func main() {
	configFile := flag.String("f", "service/gateway/etc/gateway.example.yaml", "gateway config file")
	staticDir := flag.String("static", "", "directory containing the built frontend (overrides StaticDir)")
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	if *staticDir != "" {
		c.StaticDir = *staticDir
	}
	handler, err := proxy.NewHandler(c)
	if err != nil {
		log.Fatal(err)
	}

	server := &http.Server{
		Addr:              c.ListenOn,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("gateway listening on %s", c.ListenOn)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
