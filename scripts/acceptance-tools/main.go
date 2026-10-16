// Helpers for the isolated real-database acceptance run, never a production service.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"infinitechatgo/common/idgen"
	"infinitechatgo/common/utils"
	"infinitechatgo/service/redpacket/application"
	balancelog "infinitechatgo/service/redpacket/model/balance_log"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("expected hash, audit, or expire")
	}
	if os.Args[1] == "hash" {
		hash, err := utils.HashPassword("Test123!")
		if err != nil {
			return err
		}
		fmt.Println(hash)
		return nil
	}
	if len(os.Args) < 3 || os.Getenv("IM_MYSQL_DSN") == "" {
		return fmt.Errorf("packet ID and IM_MYSQL_DSN required")
	}
	config, err := mysql.ParseDSN(os.Getenv("IM_MYSQL_DSN"))
	if err != nil || !strings.HasPrefix(config.DBName, "im_acceptance_") {
		return fmt.Errorf("helper only supports isolated im_acceptance_ databases")
	}
	id, err := strconv.ParseInt(os.Args[2], 10, 64)
	if err != nil || id <= 0 {
		return fmt.Errorf("invalid packet ID")
	}
	db := sqlx.NewMysql(os.Getenv("IM_MYSQL_DSN"))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if os.Args[1] == "audit" {
		audit, err := application.AuditPacket(ctx, db, id)
		if err != nil {
			return err
		}
		if err := json.NewEncoder(os.Stdout).Encode(audit); err != nil {
			return err
		}
		if len(audit.Problems) != 0 {
			return fmt.Errorf("packet audit found inconsistencies")
		}
		return nil
	}
	if os.Args[1] != "expire" {
		return fmt.Errorf("unknown helper command")
	}
	// Two independent service instances compete, using distinct Snowflake nodes.
	services := make([]*application.ExpireService, 2)
	for i := range services {
		services[i] = application.NewExpireService(application.ExpireDependencies{
			SqlConn: db, IDGenerator: idgen.MustNew(idgen.Config{NodeID: int64(30 + i)}),
			BalanceLogModel: balancelog.NewBalanceLogModel(db),
		})
	}
	var wg sync.WaitGroup
	results := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); _, err := services[i%2].ExpireOne(ctx, id); results <- err }(i)
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			return err
		}
	}
	fmt.Println(`{"attempts":20,"result":"PASS"}`)
	return nil
}
