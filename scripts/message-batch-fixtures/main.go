// Seeds only explicitly named disposable message-batch performance databases.
// Credentials and generated login tokens are kept in local temporary files.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"infinitechatgo/common/auth"
)

func main() {
	output := flag.String("output", "", "local token fixture file")
	flag.Parse()
	if err := seed(*output); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func seed(output string) error {
	dsn, secret := os.Getenv("IM_BATCH_PERF_DSN"), os.Getenv("IM_BATCH_PERF_SECRET")
	config, err := mysqlDriver.ParseDSN(dsn)
	if err != nil || !strings.HasPrefix(config.DBName, "im_batch_perf_") || secret == "" || output == "" {
		return fmt.Errorf("disposable im_batch_perf_ database, secret and output file are required")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	rds := redis.MustNewRedis(redis.RedisConf{Host: "127.0.0.1:6379", Type: "node"})
	authConfig := auth.Config{Issuer: "infinitechat-user-api", AccessSecret: secret, AccessExpire: 3600, RefreshExpire: 3600}
	type user struct {
		ID    string `json:"id"`
		Token string `json:"token"`
	}
	fixture := struct {
		Users  []user   `json:"users"`
		Groups []string `json:"groups"`
	}{}
	base := time.Now().UnixMilli() * 10000
	var ids []int64
	for i := 0; i < 101; i++ {
		id := base + int64(i)
		if _, err := db.ExecContext(ctx, "INSERT INTO user(user_id,password,nickname) VALUES (?,'unused-performance-fixture',?)", id, fmt.Sprintf("batch-perf-%d", i)); err != nil {
			return err
		}
		sid, err := auth.GenerateLoginSessionID()
		if err != nil {
			return err
		}
		jti, err := auth.GenerateJTI()
		if err != nil {
			return err
		}
		token, err := auth.GenerateAccessToken(authConfig, id, sid, jti)
		if err != nil {
			return err
		}
		encoded, err := json.Marshal(auth.LoginSession{UserID: id, CurrentJTI: jti, CreatedAt: time.Now().Unix(), ExpiresAt: time.Now().Add(time.Hour).Unix()})
		if err != nil {
			return err
		}
		if err := rds.SetexCtx(ctx, auth.LoginSessionKey(sid), string(encoded), 3600); err != nil {
			return err
		}
		ids = append(ids, id)
		fixture.Users = append(fixture.Users, user{strconv.FormatInt(id, 10), token})
	}
	for i := 0; i < 65; i++ {
		id := base + 1000 + int64(i)
		if _, err := db.ExecContext(ctx, "INSERT INTO session(session_id,name,type) VALUES (?,?,1)", id, fmt.Sprintf("batch-perf-group-%d", i)); err != nil {
			return err
		}
		parts := make([]string, len(ids))
		args := make([]any, 0, len(ids)*2)
		for j, userID := range ids {
			parts[j] = "(?,?,2)"
			args = append(args, userID, id)
		}
		if _, err := db.ExecContext(ctx, "INSERT INTO user_session(user_id,session_id,role) VALUES "+strings.Join(parts, ","), args...); err != nil {
			return err
		}
		fixture.Groups = append(fixture.Groups, strconv.FormatInt(id, 10))
	}
	encoded, err := json.MarshalIndent(fixture, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(output, encoded, 0600); err != nil {
		return err
	}
	fmt.Println("Seeded 101 users, 65 groups and temporary login sessions.")
	return nil
}
