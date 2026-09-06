// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.1

package svc

import (
	"context"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"github.com/zeromicro/go-zero/rest"
	"infinitechatgo/service/user/api/internal/config"
	userMiddleware "infinitechatgo/service/user/api/internal/middleware"
	userService "infinitechatgo/service/user/api/internal/service"
	apiutils "infinitechatgo/service/user/api/internal/utils"
	applyFriendModel "infinitechatgo/service/user/model/apply_friend"
	balanceLogModel "infinitechatgo/service/user/model/balance_log"
	friendModel "infinitechatgo/service/user/model/friend"
	sessionModel "infinitechatgo/service/user/model/session"
	"infinitechatgo/service/user/model/user"
	userBalanceModel "infinitechatgo/service/user/model/user_balance"
	userSessionModel "infinitechatgo/service/user/model/user_session"
	"time"
)

type ServiceContext struct {
	Config           config.Config
	SqlConn          sqlx.SqlConn
	Redis            *redis.Redis
	UserModel        user.UserModel
	UserBalanceModel userBalanceModel.UserBalanceModel
	TokenService     *userService.TokenService
	AccessAuth       rest.Middleware
	MinIOClient      *minio.Client
	BalanceLogModel  balanceLogModel.BalanceLogModel
	FriendModel      friendModel.FriendModel
	ApplyFriendModel applyFriendModel.ApplyFriendModel
	SessionModel     sessionModel.SessionModel
	UserSessionModel userSessionModel.UserSessionModel
}

func NewServiceContext(c config.Config) *ServiceContext {
	conn := sqlx.NewMysql(c.DataSource)
	rds := redis.MustNewRedis(c.Redis)
	userModel := user.NewUserModel(conn, c.Cache)
	userBalance := userBalanceModel.NewUserBalanceModel(conn, c.Cache)
	tokenService := userService.NewTokenService(
		rds,
		c.Auth,
	)
	accessAuth := userMiddleware.NewAccessAuthMiddleware(
		tokenService,
	)
	minioClient, err := apiutils.NewMinioClient(
		c.Minio.Endpoint,
		c.Minio.AccessKey,
		c.Minio.SecretKey,
		c.Minio.UseSSL,
	)
	if err != nil {
		panic(fmt.Errorf("创建MinIO客户端失败: %w", err))
	}
	checkCtx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()
	exists, err := minioClient.BucketExists(
		checkCtx,
		c.Minio.Bucket,
	)
	if err != nil {
		panic(fmt.Errorf("检查MinIO Bucket失败: %w", err))
	}
	if !exists {
		panic(fmt.Errorf(
			"MinIO Bucket不存在: %s",
			c.Minio.Bucket,
		))
	}
	balanceLog := balanceLogModel.NewBalanceLogModel(
		conn,
		c.Cache,
	)
	friend := friendModel.NewFriendModel(conn, c.Cache)
	applyFriend := applyFriendModel.NewApplyFriendModel(conn, c.Cache)
	chatSession := sessionModel.NewSessionModel(conn, c.Cache)
	userSession := userSessionModel.NewUserSessionModel(conn, c.Cache)
	return &ServiceContext{
		Config:           c,
		SqlConn:          conn,
		Redis:            rds,
		UserModel:        userModel,
		UserBalanceModel: userBalance,
		TokenService:     tokenService,
		AccessAuth:       accessAuth.Handle,
		MinIOClient:      minioClient,
		BalanceLogModel:  balanceLog,
		FriendModel:      friend,
		ApplyFriendModel: applyFriend,
		SessionModel:     chatSession,
		UserSessionModel: userSession,
	}
}
