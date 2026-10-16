package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"infinitechatgo/service/redpacket/application"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

func main() {
	packetID := flag.Int64("packet-id", 0, "red packet ID to audit")
	flag.Parse()
	dsn := os.Getenv("IM_MYSQL_DSN")
	if *packetID <= 0 || dsn == "" {
		log.Fatal("set IM_MYSQL_DSN and pass -packet-id")
	}
	audit, err := application.AuditPacket(context.Background(), sqlx.NewMysql(dsn), *packetID)
	if err != nil {
		log.Fatal(err)
	}
	result, err := json.MarshalIndent(audit, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(result))
	if len(audit.Problems) > 0 {
		os.Exit(1)
	}
}
