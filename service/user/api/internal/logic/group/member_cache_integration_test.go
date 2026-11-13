//go:build integration

package group

import (
	"context"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/stores/cache"
	zeroredis "github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"infinitechatgo/common/auth"
	"infinitechatgo/common/groupmembercache"
	"infinitechatgo/common/idgen"
	"infinitechatgo/service/user/api/internal/svc"
	"infinitechatgo/service/user/api/internal/types"
	friendmodel "infinitechatgo/service/user/model/friend"
	sessionmodel "infinitechatgo/service/user/model/session"
	membermodel "infinitechatgo/service/user/model/user_session"
)

func TestRealMembershipTransactionsBroadcastToRecipientCaches(t *testing.T) {
	dsn, addr := os.Getenv("IM_GROUP_CACHE_TEST_DSN"), os.Getenv("IM_GROUP_CACHE_TEST_REDIS")
	parsed, err := mysqldriver.ParseDSN(dsn)
	if err != nil || addr == "" || !strings.HasPrefix(parsed.DBName, "im_group_cache_") {
		t.Skip("disposable im_group_cache_ database and Redis required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	conn := sqlx.NewMysql(dsn)
	client := redis.NewClient(&redis.Options{Addr: addr, Password: os.Getenv("IM_GROUP_CACHE_TEST_REDIS_PASSWORD")})
	defer client.Close()
	channel := "im-group-members-real-test:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	cc := cache.CacheConf{{RedisConf: zeroredis.RedisConf{Host: addr, Type: zeroredis.NodeType, Pass: os.Getenv("IM_GROUP_CACHE_TEST_REDIS_PASSWORD")}, Weight: 1}}
	services := &svc.ServiceContext{SqlConn: conn, IDGenerator: idgen.MustNew(idgen.Config{NodeID: 92}),
		SessionModel: sessionmodel.NewSessionModel(conn, cc), UserSessionModel: membermodel.NewUserSessionModel(conn, cc), FriendModel: friendmodel.NewFriendModel(conn, cc),
		GroupMemberInvalidator: groupmembercache.NewNotifier(client, groupmembercache.InvalidationConfig{Channel: channel})}
	base := time.Now().UnixNano()%1000000000000 + 1000
	owner, a, b := base, base+1, base+2
	for _, id := range []int64{owner, a, b} {
		if _, err := conn.ExecCtx(ctx, "INSERT INTO user(user_id,password,nickname) VALUES (?, 'fixture', 'cache-test')", id); err != nil {
			t.Fatal(err)
		}
	}
	for _, pair := range [][2]int64{{owner, a}, {a, owner}, {owner, b}, {b, owner}} {
		if _, err := conn.ExecCtx(ctx, "INSERT INTO friend(user_id,friend_id,status) VALUES (?,?,0)", pair[0], pair[1]); err != nil {
			t.Fatal(err)
		}
	}
	locals := make([]*groupmembercache.Local, 2)
	for i := range locals {
		locals[i], err = groupmembercache.NewLocal(groupmembercache.Config{TTLSeconds: 30})
		if err != nil {
			t.Fatal(err)
		}
		sub := groupmembercache.Subscribe(ctx, client, channel, locals[i])
		defer sub.Close()
		select {
		case <-sub.Ready():
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	ownerCtx := auth.ContextWithIdentity(ctx, auth.Identity{UserID: owner})
	created, err := NewCreateGroupLogic(ownerCtx, services).CreateGroup(&types.CreateGroupRequest{MemberIds: []int64{a}})
	if err != nil {
		t.Fatal(err)
	}
	check := func(notification uint64, want []int64) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if locals[0].Stats().Invalidations >= notification && locals[1].Stats().Invalidations >= notification {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
		for _, local := range locals {
			if local.Stats().Invalidations < notification {
				t.Fatalf("invalidation not received: %+v", local.Stats())
			}
			got, err := local.Get(ctx, created.SessionId, services.UserSessionModel.FindMemberIdsBySessionId)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("recipients=%v want=%v err=%v", got, want, err)
			}
		}
	}
	check(1, []int64{owner, a})
	if _, err := NewInviteGroupMembersLogic(ownerCtx, services).InviteGroupMembers(&types.InviteGroupMembersRequest{SessionId: created.SessionId, MemberIds: []int64{b}}); err != nil {
		t.Fatal(err)
	}
	check(2, []int64{owner, a, b})
	if _, err := NewKickGroupMemberLogic(ownerCtx, services).KickGroupMember(&types.KickGroupMemberRequest{SessionId: created.SessionId, MemberId: a}); err != nil {
		t.Fatal(err)
	}
	check(3, []int64{owner, b})
	bCtx := auth.ContextWithIdentity(ctx, auth.Identity{UserID: b})
	if _, err := NewLeaveGroupLogic(bCtx, services).LeaveGroup(&types.LeaveGroupRequest{SessionId: created.SessionId}); err != nil {
		t.Fatal(err)
	}
	check(4, []int64{owner})
	if _, err := NewLeaveGroupLogic(ownerCtx, services).LeaveGroup(&types.LeaveGroupRequest{SessionId: created.SessionId}); err == nil {
		t.Fatal("owner leave unexpectedly succeeded")
	}
	check(4, []int64{owner})
}
