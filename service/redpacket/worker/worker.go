package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"infinitechatgo/common/idgen"
	"infinitechatgo/common/mysqlpool"
	"infinitechatgo/service/redpacket/application"
	balancelogmodel "infinitechatgo/service/redpacket/model/balance_log"

	"github.com/zeromicro/go-zero/core/conf"
	zeroredis "github.com/zeromicro/go-zero/core/stores/redis"
)

type workerConfig struct {
	DataSource string
	MySQLPool  mysqlpool.Config
	Redis      zeroredis.RedisConf
	Snowflake  idgen.Config
	Kafka      struct {
		Brokers         []string
		CacheBuildTopic string
		CacheBuildGroup string
		DLQTopic        string
	}
	ExpiryPollMillis   int
	RebuildPollSeconds int
	BatchSize          int
}

var workerConfigFile = flag.String("f", "etc/redpacket-worker.yaml", "worker config")

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	flag.Parse()
	var c workerConfig
	conf.MustLoad(*workerConfigFile, &c, conf.UseEnv())
	if c.DataSource == "" || c.ExpiryPollMillis <= 0 || c.RebuildPollSeconds <= 0 ||
		c.BatchSize <= 0 || c.BatchSize > 1000 {
		return fmt.Errorf("red packet worker configuration is incomplete")
	}
	db, err := mysqlpool.NewMysql(c.DataSource, c.MySQLPool)
	if err != nil {
		return err
	}
	rds := zeroredis.MustNewRedis(c.Redis)
	ids := idgen.MustNew(c.Snowflake)
	pool := application.NewRedisCandidatePool(rds, db)
	expiry := application.NewExpireService(application.ExpireDependencies{
		SqlConn: db, IDGenerator: ids, CandidatePool: pool,
		BalanceLogModel: balancelogmodel.NewBalanceLogModel(db),
	})
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var workers sync.WaitGroup
	errors := make(chan error, 3)
	start := func(fn func(context.Context) error) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if err := fn(ctx); err != nil {
				errors <- err
			}
		}()
	}
	start(func(ctx context.Context) error {
		ticker := time.NewTicker(time.Duration(c.ExpiryPollMillis) * time.Millisecond)
		defer ticker.Stop()
		for {
			for {
				count, err := expiry.RunBatch(ctx, c.BatchSize)
				if err != nil {
					log.Printf("refund expired packets: %v", err)
					break
				}
				if count < c.BatchSize {
					break
				}
			}
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
		}
	})
	start(func(ctx context.Context) error {
		ticker := time.NewTicker(time.Duration(c.RebuildPollSeconds) * time.Second)
		defer ticker.Stop()
		lastID := int64(0)
		for {
			var rows []struct {
				RedPacketID int64 `db:"red_packet_id"`
			}
			err := db.QueryRowsCtx(ctx, &rows, `SELECT red_packet_id FROM red_packet
				WHERE status = 0 AND red_packet_id > ? ORDER BY red_packet_id LIMIT ?`,
				lastID, c.BatchSize)
			if err != nil {
				log.Printf("scan candidate pools: %v", err)
			} else if len(rows) == 0 {
				lastID = 0
			} else {
				for _, row := range rows {
					lastID = row.RedPacketID
					if err := pool.RebuildIfMismatch(ctx, row.RedPacketID); err != nil {
						log.Printf("repair candidate pool packet=%d: %v", row.RedPacketID, err)
					}
				}
			}
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
		}
	})
	select {
	case err := <-errors:
		stop()
		workers.Wait()
		return err
	case <-ctx.Done():
		workers.Wait()
		return nil
	}
}
